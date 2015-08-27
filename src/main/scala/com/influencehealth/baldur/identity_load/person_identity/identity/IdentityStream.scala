package com.influencehealth.baldur.identity_load.person_identity.identity

import java.util.UUID

import com.influencehealth.baldur.identity_load.person_identity.change_capture.support._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.influencehealth.baldur.identity_load.person_identity.householding.support._
import com.influencehealth.baldur.identity_load.person_identity.identity.support.PersonMatchKey._
import com.influencehealth.baldur.identity_load.person_identity.identity.support._
import com.influencehealth.baldur.identity_load.person_identity.identity_table.support.IdentityTableCreatorConfig
import com.influencehealth.baldur.support.JsonSupport._
import com.influencehealth.baldur.identity_load.person_identity.support._
import com.influencehealth.baldur.support._
import org.apache.kafka.clients.producer._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import play.api.libs.json._

object IdentityStream {
  var support: Support = SupportImpl

  def processIdentity(rdd: RDD[JsObject], personIdentityConfig: PersonIdentityConfig, kafkaParams: Map[String, String], kafkaProducerConfig: Map[String, Object]): RDD[JsObject] = {
    val inputPartitions: Int = rdd.partitions.size

    val recordIdToRaw: RDD[(String, JsObject)] = rdd.map { jsObj =>
      ((jsObj \ support.ExternalRecordIdField).as[String], jsObj)
    }.persist(StorageLevel.MEMORY_AND_DISK_SER)

    // Pair RDD keyed by the person id from the input source with value of the raw JSON received
    val externalPersonIdToRaw: RDD[(String, JsObject)] = recordIdToRaw.map {
      case (_, jsObj) => ((jsObj \ support.ExternalPersonIdField).as[String], jsObj)
    }.persist(StorageLevel.MEMORY_AND_DISK_SER)

    // Pair RDD keyed by the record id from the input source with value of parsed record
    val recordIdToPersonIdentityColumns: RDD[(String, PersonIdentityColumns)] = recordIdToRaw.map {
      case (recordId, jsObj) => (recordId, jsObj.as[PersonIdentityColumns])
    }

    val sourceIdentityToRecord: RDD[(SourceIdentity, PersonIdentityColumns)] = recordIdToPersonIdentityColumns.map {
      case (_, record) =>
        (SourceIdentity(record.customerId, record.externalPersonId, record.source, record.sourceType), record)
    }.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val alreadyIdentified: RDD[(SourceIdentity, UUID)] = sourceIdentityToRecord.map {
      case (sourceIdentity, _) => sourceIdentity
    }.distinct().joinWithCassandraTable(personIdentityConfig.keyspace, personIdentityConfig.sourceIdentityTable).map {
      case (sourceIdentity, cassandraRow) => (sourceIdentity, cassandraRow.getUUID("person_id"))
    }.persist(StorageLevel.MEMORY_AND_DISK_SER)

    var sourceIdentityToPersonId: RDD[(SourceIdentity, (Option[UUID], PersonIdentityColumns))] =
      sourceIdentityToRecord.reduceByKey((a, b) => a).leftOuterJoin(alreadyIdentified).map {
        case (sourceIdentity, (personIdentityColumns, personId)) => (sourceIdentity, (personId, personIdentityColumns))
      }

    val unidentifiedKey1Candidates: RDD[PersonIdentityColumns] = sourceIdentityToPersonId.filter {
      case (sourceIdentity, (None, personIdentityColumns)) => personIdentityColumns.toPersonMatchKey1.isDefined
      case _ => false
    }.map {
      case (sourceIdentity, (None, personIdentityColumns)) => personIdentityColumns
    }

    val identifiedByKey1: RDD[(SourceIdentity, UUID)] = support.identifyByKey(unidentifiedKey1Candidates, (personIdentityConfig.keyspace, personIdentityConfig.identity1Table))

    sourceIdentityToPersonId = support.updateIdentifiedPersons(sourceIdentityToPersonId, identifiedByKey1)

    val unidentifiedKey2Candidates: RDD[PersonIdentityColumns] = sourceIdentityToPersonId.filter {
      case (sourceIdentity, (None, personIdentityColumns)) => personIdentityColumns.toPersonMatchKey2.isDefined
      case _ => false
    }.map {
      case (sourceIdentity, (None, personIdentityColumns)) => personIdentityColumns
    }

    val identifiedByKey2: RDD[(SourceIdentity, UUID)] = support.identifyByKey(unidentifiedKey2Candidates, (personIdentityConfig.keyspace, personIdentityConfig.identity2Table))

    sourceIdentityToPersonId = support.updateIdentifiedPersons(sourceIdentityToPersonId, identifiedByKey2)

    val unidentifiedKey3Candidates: RDD[PersonIdentityColumns] = sourceIdentityToPersonId.filter {
      case (sourceIdentity, (None, personIdentityColumns)) => personIdentityColumns.toPersonMatchKey3.isDefined
      case _ => false
    }.map {
      case (sourceIdentity, (None, personIdentityColumns)) => personIdentityColumns
    }

    val identifiedByKey3: RDD[(SourceIdentity, UUID)] = support.identifyByKey(unidentifiedKey3Candidates,
      (personIdentityConfig.keyspace, personIdentityConfig.identity3Table))

    sourceIdentityToPersonId = support.updateIdentifiedPersons(sourceIdentityToPersonId, identifiedByKey3)

    // Generate UUIDs for source identities that do not have
    // a person id.
    val newPersonsSourceIdentity = sourceIdentityToPersonId.filter {
      case (_, (None, _)) => true
      case _ => false
    }.map((_, UUID.randomUUID())).persist(StorageLevel.MEMORY_AND_DISK)

    // Save source identity to person id mapping for new person ids.
    val newPersonsSourceIdentityToPersonId = newPersonsSourceIdentity.map {
      case ((sourceIdentity, (None, _)), personId) => (sourceIdentity, personId)
    }

    newPersonsSourceIdentityToPersonId.map {
      case (sourceIdentity, personId) => (sourceIdentity.customerId, sourceIdentity.sourcePersonId, sourceIdentity.source, sourceIdentity.sourceType, personId)
    }.saveToCassandra(personIdentityConfig.keyspace, personIdentityConfig.sourceIdentityTable)

    val newPersonsByExternalPersonId: RDD[(String, UUID)] = newPersonsSourceIdentityToPersonId.map {
      case (sourceIdentity, personId) => (sourceIdentity.sourcePersonId, personId)
    }

    def addPersonId(x: (String, (JsObject, UUID))) = x match {
      case (_, (jsObj, personId)) =>
        jsObj ++ JsObject(Seq("personId" -> JsString(personId.toString)))
    }

    val newPersons: RDD[JsObject] = externalPersonIdToRaw.join(newPersonsByExternalPersonId).map(addPersonId)

    // Get the persons by external person id to join to the externalPersonIdToRaw rdd.
    val existingPersonsByExternalPersonId: RDD[(String, UUID)] = alreadyIdentified.union(identifiedByKey1).union(identifiedByKey2).map {
      case (sourceIdentity, personId) => (sourceIdentity.sourcePersonId, personId)
    }

    val existingPersons = externalPersonIdToRaw.join(existingPersonsByExternalPersonId).map(addPersonId)

    val results = newPersons.union(existingPersons).repartition(inputPartitions).persist(StorageLevel.MEMORY_AND_DISK_SER)

    val allCount = recordIdToRaw.count()
    val alreadyIdentifiedCount = alreadyIdentified.count()
    val identityKey1Matches = identifiedByKey1.count()
    val identityKey2Matches = identifiedByKey2.count()
    val matchesCount = identityKey1Matches + identityKey2Matches
    val newPersonsCount = newPersons.count()

    support.sendToTopic(ProducerObject.get(kafkaProducerConfig), new ProducerRecord[String, String](personIdentityConfig.identityStatsTopic,
      Json.stringify(JsObject(Seq(
        "newPersonsCount" -> JsNumber(newPersonsCount),
        "alreadyIdentifiedCount" -> JsNumber(alreadyIdentifiedCount),
        "identifiedCount" -> JsNumber(matchesCount),
        "identifiedByKey1Count" -> JsNumber(identityKey1Matches),
        "identifiedByKey2Count" -> JsNumber(identityKey2Matches),
        "totalCount" -> JsNumber(allCount))))))

    recordIdToRaw.unpersist(false)
    externalPersonIdToRaw.unpersist(false)
    sourceIdentityToRecord.unpersist(false)
    newPersonsSourceIdentityToPersonId.unpersist(false)

    results
  }

  implicit class IdentityStreamingContext(val streamingContext: StreamingContext) {
    def createIdentityStream(personIdentityConfig: PersonIdentityConfig,
                             householdConfig: HouseholdConfig,
                             changeCaptureConfig: ChangeCaptureConfig,
                             identityTableCreatorConfig: IdentityTableCreatorConfig,
                             cassandraConnector: CassandraConnector,
                             kafkaParams: Map[String, String],
                             kafkaProducerConfig: Map[String, Object],
                             emptyJsonRdd: RDD[JsObject],
                             emptyChangeRdd: RDD[ColumnChange]) = {
      support.createDirectStream(streamingContext,
        kafkaParams,
        personIdentityConfig.identityInputTopics.split(",").toSet,
        Map("maxRatePerPartition" -> 750.toString))
    }
  }
}
