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

    val trustSourceId = personIdentityConfig.trustSourceId

    val results: RDD[JsObject] = trustSourceId match{
      /********************** TRUSTED SOURCE ID STREAM*************************************/
      // if we trust the source ID, we can use it by itself for grouping
      case true =>

        // key the input by SourceIdentity. Used in final results
        val inputKeyed: RDD[(SourceIdentity, JsObject)] =
          rdd
          .map{case record =>  (SourceIdentity.fromJson(record), record)}

        // table meant for processing through the person-keys
        val processingRdd: RDD[(SourceIdentity, PersonIdentityColumns)] =
          rdd
          .map{case record =>  (SourceIdentity.fromJson(record), record.as[PersonIdentityColumns])}
          .persist(StorageLevel.MEMORY_AND_DISK_SER)

        // determine if the data has already been processed
        val alreadyIdentified: RDD[(SourceIdentity, UUID)] =
          processingRdd
            .map{case (sourceIdentity, record) => sourceIdentity}
            .distinct()
            .joinWithCassandraTable[UUID](personIdentityConfig.keyspace, personIdentityConfig.sourceIdentityTable)
            .select("person_id")
            .persist(StorageLevel.MEMORY_AND_DISK_SER)

        // for non-processed records, set up the recordsForMatching RDD
        var recordsForMatching: RDD[(SourceIdentity, (Option[UUID], PersonIdentityColumns))] =
          processingRdd
            .leftOuterJoin(alreadyIdentified)
            .filter{ case (sourceIdentity, (record, personId)) => personId.isEmpty}
            .map{ case (sourceIdentity, (personIdentityColumns, personId)) => (sourceIdentity, (personId, personIdentityColumns)) }

        // grab key 1 candidates
        val unidentifiedKey1Candidates: RDD[PersonIdentityColumns] =
          recordsForMatching
            .filter {
            case (sourceIdentity, (None, personIdentityColumns)) => personIdentityColumns.toPersonMatchKey1.isDefined
            case _ => false
          }
            .map { case (sourceIdentity, (None, personIdentityColumns)) => personIdentityColumns  }

        // identify by key 1
        val identifiedByKey1: RDD[(SourceIdentity, UUID)] = support.identifyByKey(unidentifiedKey1Candidates,
          (personIdentityConfig.keyspace, personIdentityConfig.identity1Table)).persist(StorageLevel.MEMORY_AND_DISK_SER)

        // update recordsForMatching
        recordsForMatching = support.updateIdentifiedPersons(recordsForMatching, identifiedByKey1)

        // grab key 2 candidates
        val unidentifiedKey2Candidates: RDD[PersonIdentityColumns] = recordsForMatching.filter {
          case (sourceIdentity, (None, personIdentityColumns)) => personIdentityColumns.toPersonMatchKey2.isDefined
          case _ => false
        }.map {
          case (sourceIdentity, (None, personIdentityColumns)) => personIdentityColumns
        }

        // identify by key 2
        val identifiedByKey2: RDD[(SourceIdentity, UUID)] = support.identifyByKey(unidentifiedKey2Candidates,
          (personIdentityConfig.keyspace, personIdentityConfig.identity2Table)).persist(StorageLevel.MEMORY_AND_DISK_SER)

        // update records for matching
        recordsForMatching = support.updateIdentifiedPersons(recordsForMatching, identifiedByKey2)

        // grab key 3 candidates
        val unidentifiedKey3Candidates: RDD[PersonIdentityColumns] = recordsForMatching.filter {
          case (sourceIdentity, (None, personIdentityColumns)) => personIdentityColumns.toPersonMatchKey3.isDefined
          case _ => false
        }.map {
          case (sourceIdentity, (None, personIdentityColumns)) => personIdentityColumns
        }

        // identify by key 3
        val identifiedByKey3: RDD[(SourceIdentity, UUID)] = support.identifyByKey(unidentifiedKey3Candidates,
          (personIdentityConfig.keyspace, personIdentityConfig.identity3Table)).persist(StorageLevel.MEMORY_AND_DISK_SER)

        // update records for matching
        recordsForMatching = support.updateIdentifiedPersons(recordsForMatching, identifiedByKey3)

        // union everything that's already matched
        val matchedPersons: RDD[(SourceIdentity, UUID)] =
          alreadyIdentified
          .union(identifiedByKey1)
          .union(identifiedByKey2)
          .union(identifiedByKey3)

        // everything else, assign a random UUID based on the grouping key
        val newPersons: RDD[(SourceIdentity, UUID)] =
          recordsForMatching
          .filter{ case (sourceIdentity, (personId, record)) => personId.isEmpty}
          .map{ case (sourceIdentity, (personId, record)) => sourceIdentity}
          .distinct()
          .map((_, UUID.randomUUID()))
          .persist(StorageLevel.MEMORY_AND_DISK_SER)

        // Save new persons to the source_identity table
        newPersons
          .map{case (sourceIdentity, personId) => (sourceIdentity.customerId, sourceIdentity.sourcePersonId, sourceIdentity.source, sourceIdentity.sourceType, personId)}
          .saveToCassandra(personIdentityConfig.keyspace, personIdentityConfig.sourceIdentityTable)

        // final results are done by updating the jsObject with the newest personId value
        val results = matchedPersons
          .union(newPersons)
          .join(inputKeyed)
          .map{ case (sourceIdentity, (personId, jsObject)) => jsObject + ("personId", JsString(personId.toString))}

        // calculate statistics
        val allCount = rdd.count()
        val allInboundPersons = inputKeyed.map{case (sourceIdentity, record) => sourceIdentity}.distinct().count()
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

        processingRdd.unpersist()
        alreadyIdentified.unpersist()
        identifiedByKey1.unpersist()
        identifiedByKey2.unpersist()
        identifiedByKey3.unpersist()
        newPersons.unpersist()

        results
      /********************** UNTRUSTED SOURCE ID STREAM*************************************/
      case false =>
        // if we dont trust the source ID, we can use it with other data elements for grouping
        val inputKeyed: RDD[(SourceIdentityUntrusted, JsObject)] =
          rdd
            .map{case record =>  (SourceIdentityUntrusted.fromJson(record), record)}

        // key the input by SourceIdentity. Used in final results
        val processingRdd: RDD[(SourceIdentityUntrusted, PersonIdentityColumns)] =
          rdd
            .map{case record =>  (SourceIdentityUntrusted.fromJson(record), record.as[PersonIdentityColumns])}

        // table meant for processing through the person-keys
        var recordsForMatching: RDD[(SourceIdentityUntrusted, (Option[UUID], PersonIdentityColumns))] =
          processingRdd
            .map{ case (sourceIdentity, personIdentityColumns) => (sourceIdentity, (None, personIdentityColumns)) }

        // grab key 1 candidates
        val unidentifiedKey1Candidates: RDD[PersonIdentityColumns] =
          recordsForMatching
            .filter {
            case (sourceIdentity, (None, personIdentityColumns)) => personIdentityColumns.toPersonMatchKey1.isDefined
            case _ => false
          }
            .map { case (sourceIdentity, (None, personIdentityColumns)) => personIdentityColumns  }

        // identify by key 1
        val identifiedByKey1: RDD[(SourceIdentityUntrusted, UUID)] = support.identifyByKeyUntrusted(unidentifiedKey1Candidates,
          (personIdentityConfig.keyspace, personIdentityConfig.identity1Table)).persist(StorageLevel.MEMORY_AND_DISK_SER)

        // update recordsForMatching
        recordsForMatching = support.updateIdentifiedPersonsUntrusted(recordsForMatching, identifiedByKey1)

        // grab key 2 candidates
        val unidentifiedKey2Candidates: RDD[PersonIdentityColumns] = recordsForMatching.filter {
          case (sourceIdentity, (None, personIdentityColumns)) => personIdentityColumns.toPersonMatchKey2.isDefined
          case _ => false
        }.map {
          case (sourceIdentity, (None, personIdentityColumns)) => personIdentityColumns
        }

        // identify by key 2
        val identifiedByKey2: RDD[(SourceIdentityUntrusted, UUID)] = support.identifyByKeyUntrusted(unidentifiedKey2Candidates,
          (personIdentityConfig.keyspace, personIdentityConfig.identity2Table)).persist(StorageLevel.MEMORY_AND_DISK_SER)

        // update records for matching
        recordsForMatching = support.updateIdentifiedPersonsUntrusted(recordsForMatching, identifiedByKey2)

        // grab key 3 candidates
        val unidentifiedKey3Candidates: RDD[PersonIdentityColumns] = recordsForMatching.filter {
          case (sourceIdentity, (None, personIdentityColumns)) => personIdentityColumns.toPersonMatchKey3.isDefined
          case _ => false
        }.map {
          case (sourceIdentity, (None, personIdentityColumns)) => personIdentityColumns
        }

        // identify by key 3
        val identifiedByKey3: RDD[(SourceIdentityUntrusted, UUID)] = support.identifyByKeyUntrusted(unidentifiedKey3Candidates,
          (personIdentityConfig.keyspace, personIdentityConfig.identity3Table)).persist(StorageLevel.MEMORY_AND_DISK_SER)

        // update records for matching
        recordsForMatching = support.updateIdentifiedPersonsUntrusted(recordsForMatching, identifiedByKey3)

        // union everything that's already matched
        val matchedPersons: RDD[(SourceIdentityUntrusted, UUID)] =
          identifiedByKey1
            .union(identifiedByKey2)
            .union(identifiedByKey3)

        // everything else, assign a random UUID based on the grouping key
        val newPersons: RDD[(SourceIdentityUntrusted, UUID)] =
          recordsForMatching
            .filter{ case (sourceIdentity, (personId, record)) => personId.isEmpty}
            .map{ case (sourceIdentity, (personId, record)) => sourceIdentity}
            .distinct()
            .map((_, UUID.randomUUID()))

        // final results are done by updating the jsObject with the newest personId value
        val results = matchedPersons
          .union(newPersons)
          .join(inputKeyed)
          .map{ case (sourceIdentity, (personId, jsObject)) => jsObject + ("personId", JsString(personId.toString))}

        // calculate statistics
        val allCount = rdd.count()
        val allInboundPersons = inputKeyed.map{case (sourceIdentity, record) => sourceIdentity}.distinct().count()
        val alreadyIdentifiedCount = 0
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

        processingRdd.unpersist()
        identifiedByKey1.unpersist()
        identifiedByKey2.unpersist()
        identifiedByKey3.unpersist()

        results
    }
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
