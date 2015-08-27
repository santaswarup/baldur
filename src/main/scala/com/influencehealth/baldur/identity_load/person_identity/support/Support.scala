package com.influencehealth.baldur.identity_load.person_identity.support

import java.util.UUID

import com.datastax.spark.connector._
import com.influencehealth.baldur.identity_load.person_identity.identity.support._
import com.influencehealth.baldur.identity_load.person_identity.identity.support.SourceIdentity._
import com.influencehealth.baldur.support._
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.producer._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka._
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import play.api.libs.json.JsObject

trait Support {
  val ExternalPersonIdField = "sourcePersonId"
  val ExternalRecordIdField = "sourceRecordId"

  def createDirectStream(ssc: StreamingContext,
    kafkaParams: Map[String, String],
    topics: Set[String],
    config: Map[String, String]=Map("maxRatePerPartition" -> 0.toString)): InputDStream[(String, String)] =
    MoreKafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, config, topics)

  def soundex(w: String) = {
    w.toLowerCase
  }

  def soundex(w: Option[String]): Option[String] = w match {
    case Some(word) => Some(word.toLowerCase)
    case _ => None
  }

  def rootFirstName(firstName: Option[String], sex: Option[String]): Option[String] = {
    FirstNameRoot.getRootName(firstName, sex)
  }

  def points[T](unidentified: Option[T], candidate: Option[T], magnitude: Int): Int = {
    candidate match {
      case Some(candidate) =>
        if (unidentified.nonEmpty && unidentified.get == candidate)
          magnitude
        else
          -magnitude
      case None =>
        0
    }
  }

  def points[T](unidentified: Option[T], candidate: Set[T], magnitude: Int): Int = {
    if (candidate.isEmpty) {
      0
    } else {
      if (unidentified.isDefined && candidate.contains(unidentified.get))
        magnitude
      else
        -magnitude
    }
  }

  def identityScore(x: (PersonIdentityColumns, CassandraRow)): ((PersonIdentityColumns, CassandraRow), Int) = {

    var score = 0
    val unidentified = x._1
    val candidateIdentity = x._2

    score += points(unidentified.mrn, candidateIdentity.getSet[String]("mrids"), 1000000)

    score += points(unidentified.rootFirstName, candidateIdentity.get[Option[String]]("root_first_name"), 1000000)

    score += points(unidentified.dob, candidateIdentity.get[Option[DateTime]]("dob"), 100000)

    score += points(unidentified.sex, candidateIdentity.get[Option[String]]("sex"), 10000)

    score += points(unidentified.address2, candidateIdentity.get[Option[String]]("address2"), 1000)

    score += points(unidentified.streetSecondNumber, candidateIdentity.get[Option[String]]("street_second_number"), 1000)

    score += points(unidentified.middleName, candidateIdentity.get[Option[String]]("middle_name"), 100)

    score += points(unidentified.suffix, candidateIdentity.get[Option[String]]("suffix"), 10)

    (x, score)
  }

  def toDateTime(value: String): DateTime = {
    DateTime.parse(value, ISODateTimeFormat.basicDate())
  }

  def getUniquePersonIdFromJson(jsObj: JsObject): String = {
    (jsObj \ support.ExternalPersonIdField).as[String] + "." + (jsObj \ "source").as[String] + "." + (jsObj \ "sourceType").as[String] + "." + (jsObj \ "customerId").as[String]
  }

  def getUniquePersonIdFromSourceIdentity(sourceIdentity: SourceIdentity): String = {
    sourceIdentity.sourcePersonId + "." + sourceIdentity.source + "." + sourceIdentity.sourceType + "." + sourceIdentity.customerId
  }

  def toDateTimeOpt(value: Option[String]): Option[DateTime]= {
    value match{
      case None => None
      case Some("") => None
      case Some(x) => Some(DateTime.parse(x, ISODateTimeFormat.basicDate()))
    }
  }

  def valueIsNone(x: (Any, Option[UUID])) = x match {
    case (_, None) => true
    case _ => false
  }

  def takeSecondValue[A, B, C](x: (A, (B, C))): C = x match {
    case (key, (_, secondValue)) => secondValue
  }

  def filterPositiveValue[K](x: (K, Int)) = x._2 > 0

  def mapSourceIdentityToPersonId[V](x: ((PersonIdentityColumns, CassandraRow), V)): (SourceIdentity, UUID) = x match {
    case ((record, cassandraRow), _) => (record.sourceIdentity, cassandraRow.getUUID("person_id"))
  }

  def identifyByKey(unidentifiedRecords: RDD[PersonIdentityColumns],
    cassandraTable: (String, String)): RDD[(SourceIdentity, UUID)] =
  {
    val joinedWithTable: RDD[(PersonIdentityColumns, CassandraRow)] = unidentifiedRecords
      .joinWithCassandraTable(cassandraTable._1, cassandraTable._2)

    joinedWithTable
      .map(identityScore)
      .filter(filterPositiveValue)
      .map(mapSourceIdentityToPersonId)
  }

  def updateIdentifiedPersons(sourceIdentityToPersonId: RDD[(SourceIdentity, (Option[UUID], PersonIdentityColumns))],
    identified: RDD[(SourceIdentity, UUID)]): RDD[(SourceIdentity, (Option[UUID], PersonIdentityColumns))] =
  {
    sourceIdentityToPersonId.leftOuterJoin(identified).map {
      case (sourceIdentity, ((Some(personId), personIdentityColumns ), _)) => (sourceIdentity, (Some(personId), personIdentityColumns))
      case (sourceIdentity, ((None, personIdentityColumns), Some(personId))) => (sourceIdentity, (Some(personId), personIdentityColumns))
      case (sourceIdentity, ((None, personIdentityColumns), None)) => (sourceIdentity, (None, personIdentityColumns))
    }
  }

  def sendToTopic(kafkaProducer: KafkaProducer[String, String], producerRecord: ProducerRecord[String, String], attemptNumber: Int = 0): RecordMetadata = {
    val retries = 10

    val result = kafkaProducer.send(producerRecord)
    try {
      result.get()
    } catch {
      case e: Exception =>
        if (attemptNumber >= retries) {
          throw new Error("Failed "+retries+" times, giving up")
        } else {
          sendToTopic(kafkaProducer, producerRecord, attemptNumber + 1)
        }
    }
  }

  def snakify(name: String) = "[A-Z]".r.replaceAllIn(name, {m => "_" + m.group(0).toLowerCase })


}

object SupportImpl extends Support with Serializable {
}

