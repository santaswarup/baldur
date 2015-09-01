package com.influencehealth.baldur.identity_load.person_identity.identity

import java.util.UUID

import com.influencehealth.baldur.identity_load.person_identity.change_capture.support._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.extensions._
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

    val sourceIdentityToRecord: RDD[(SourceIdentity, PersonIdentityColumns)] =
      rdd
      .map{case record =>  (SourceIdentity.fromJson(record), record.as[PersonIdentityColumns])}
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    // Pair RDD keyed by the person id from the input source with value of the raw JSON received
    val externalPersonIdToRaw: RDD[(String, JsObject)] =
      rdd
      .map { case jsObj =>
        val uniqueID: String = support.getUniquePersonIdFromJson(jsObj)
        (uniqueID, jsObj) }
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val joined: RDD[(SourceIdentity, Option[UUID])] =
      sourceIdentityToRecord
      .map { case (sourceIdentity, _) => sourceIdentity }
      .distinct()
      .leftOuterJoinWithCassandraTable[UUID](personIdentityConfig.keyspace, personIdentityConfig.sourceIdentityTable)
      .select("person_id")
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val alreadyIdentified =
      joined
      .filter{case (sourceIdentity, uuid) => uuid.isDefined}
      .map{case (sourceIdentity, uuid) => (sourceIdentity, uuid.get)}
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    var sourceIdentityToPersonId: RDD[(SourceIdentity, (Option[UUID], PersonIdentityColumns))] =
      sourceIdentityToRecord
      .join(joined)
      .map{ case (sourceIdentity, (personIdentityColumns, personId)) => (sourceIdentity, (personId, personIdentityColumns)) }

    val unidentifiedKey1Candidates: RDD[PersonIdentityColumns] =
    sourceIdentityToPersonId
    .filter {
      case (sourceIdentity, (None, personIdentityColumns)) => personIdentityColumns.toPersonMatchKey1.isDefined
      case _ => false
    }
    .map { case (sourceIdentity, (None, personIdentityColumns)) => personIdentityColumns  }

    val identifiedByKey1: RDD[(SourceIdentity, UUID)] = support.identifyByKey(unidentifiedKey1Candidates,
      (personIdentityConfig.keyspace, personIdentityConfig.identity1Table)).persist(StorageLevel.MEMORY_AND_DISK_SER)

    sourceIdentityToPersonId = support.updateIdentifiedPersons(sourceIdentityToPersonId, identifiedByKey1)

    val unidentifiedKey2Candidates: RDD[PersonIdentityColumns] = sourceIdentityToPersonId.filter {
      case (sourceIdentity, (None, personIdentityColumns)) => personIdentityColumns.toPersonMatchKey2.isDefined
      case _ => false
    }.map {
      case (sourceIdentity, (None, personIdentityColumns)) => personIdentityColumns
    }

    val identifiedByKey2: RDD[(SourceIdentity, UUID)] = support.identifyByKey(unidentifiedKey2Candidates,
      (personIdentityConfig.keyspace, personIdentityConfig.identity2Table)).persist(StorageLevel.MEMORY_AND_DISK_SER)

    sourceIdentityToPersonId = support.updateIdentifiedPersons(sourceIdentityToPersonId, identifiedByKey2)

    val unidentifiedKey3Candidates: RDD[PersonIdentityColumns] = sourceIdentityToPersonId.filter {
      case (sourceIdentity, (None, personIdentityColumns)) => personIdentityColumns.toPersonMatchKey3.isDefined
      case _ => false
    }.map {
      case (sourceIdentity, (None, personIdentityColumns)) => personIdentityColumns
    }

    val identifiedByKey3: RDD[(SourceIdentity, UUID)] = support.identifyByKey(unidentifiedKey3Candidates,
      (personIdentityConfig.keyspace, personIdentityConfig.identity3Table)).persist(StorageLevel.MEMORY_AND_DISK_SER)

    sourceIdentityToPersonId = support.updateIdentifiedPersons(sourceIdentityToPersonId, identifiedByKey3)

    // Generate UUIDs for source identities that do not have
    // a person id.
    val newPersonsSourceIdentity =
      sourceIdentityToPersonId
      .filter {
        case (_, (None, _)) => true
        case _ => false
      }.map((_, UUID.randomUUID()))

    // Save source identity to person id mapping for new person ids.
    val newPersonsSourceIdentityToPersonId = newPersonsSourceIdentity.map {
      case ((sourceIdentity, (None, _)), personId) => (sourceIdentity, personId)
    }.persist(StorageLevel.MEMORY_AND_DISK_SER)

    newPersonsSourceIdentityToPersonId.map {
      case (sourceIdentity, personId) => (sourceIdentity.customerId, sourceIdentity.sourcePersonId, sourceIdentity.source, sourceIdentity.sourceType, personId)
    }.saveToCassandra(personIdentityConfig.keyspace, personIdentityConfig.sourceIdentityTable)

    val newPersonsByExternalPersonId: RDD[(String, UUID)] =
      newPersonsSourceIdentityToPersonId
      .map { case (sourceIdentity, personId) =>
        val uniqueId: String = support.getUniquePersonIdFromSourceIdentity(sourceIdentity)
        (uniqueId, personId) }

    def addPersonId(x: (String, (JsObject, UUID))): JsObject = x match {
      case (_, (jsObj, personId)) =>
        jsObj + ("personId", JsString(personId.toString))
    }

    val sampleNewPersonJoinIds: Array[String] = newPersonsByExternalPersonId.take(5).map(_._1)
    val sampleRawJoinIds: Array[String] = externalPersonIdToRaw.take(5).map(_._1)

    val newPersons: RDD[JsObject] = externalPersonIdToRaw.join(newPersonsByExternalPersonId).map(addPersonId)

    // Get the persons by external person id to join to the externalPersonIdToRaw rdd.
    val existingPersonsByExternalPersonId: RDD[(String, UUID)] =
      alreadyIdentified
      .union(identifiedByKey1)
      .union(identifiedByKey2)
      .union(identifiedByKey3)
      .map { case (sourceIdentity, personId) =>
        val uniqueId: String = support.getUniquePersonIdFromSourceIdentity(sourceIdentity)
          (uniqueId, personId) }

    val existingPersons =
      externalPersonIdToRaw
      .join(existingPersonsByExternalPersonId)
      .map(addPersonId)

    val results = newPersons.union(existingPersons)

    println("raw person join id sample: " + sampleRawJoinIds.toString)
    println("new person join id sample: " + sampleNewPersonJoinIds.toString)

    val allCount = rdd.count()
    val allInboundPersons = sourceIdentityToRecord.map{case (sourceIdentity, record) => sourceIdentity}.distinct().count()
    val alreadyIdentifiedCount = alreadyIdentified.count()
    val identityKey1Matches = identifiedByKey1.count()
    val identityKey2Matches = identifiedByKey2.count()
    val identityKey3Matches = identifiedByKey3.count()
    val matchesCount = identityKey1Matches + identityKey2Matches + identityKey3Matches
    val newPersonsCount = newPersons.count()
    val resultCount = results.count()

    println("allInboundPersonCount: " + allInboundPersons.toString)
    println("newPersonsCount: " + newPersonsCount.toString)
    println("alreadyIdentifiedCount: " + alreadyIdentifiedCount.toString)
    println("identifiedCount: " + matchesCount.toString)
    println("identifiedByKey1Count: " + identityKey1Matches.toString)
    println("identifiedByKey2Count: " + identityKey2Matches.toString)
    println("identifiedByKey3Count: " + identityKey3Matches.toString)
    println("inboundRecordCount: " + allCount.toString)
    println("outboundRecordCount: " + resultCount.toString)

    support.sendToTopic(ProducerObject.get(kafkaProducerConfig), new ProducerRecord[String, String](personIdentityConfig.identityStatsTopic,
      Json.stringify(JsObject(Seq(
        "allInbounddPersonCount" -> JsNumber(allInboundPersons),
        "newPersonsCount" -> JsNumber(newPersonsCount),
        "alreadyIdentifiedCount" -> JsNumber(alreadyIdentifiedCount),
        "identifiedCount" -> JsNumber(matchesCount),
        "identifiedByKey1Count" -> JsNumber(identityKey1Matches),
        "identifiedByKey2Count" -> JsNumber(identityKey2Matches),
        "identifiedByKey3Count" -> JsNumber(identityKey3Matches),
        "totalRecordCount" -> JsNumber(allCount))))))

    joined.unpersist()
    identifiedByKey1.unpersist()
    identifiedByKey2.unpersist()
    identifiedByKey3.unpersist()
    alreadyIdentified.unpersist()
    externalPersonIdToRaw.unpersist()
    sourceIdentityToRecord.unpersist()
    newPersonsSourceIdentityToPersonId.unpersist()

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
