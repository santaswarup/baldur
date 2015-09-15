package com.influencehealth.baldur.identity_load.person_identity.support

import java.util.UUID

import com.datastax.spark.connector._
import com.influencehealth.baldur.identity_load.person_identity.identity.support._
import com.influencehealth.baldur.identity_load.person_identity.identity.support.SourceIdentity._
import com.influencehealth.baldur.identity_load.person_identity.identity.support.SourceIdentityUntrusted._
import com.influencehealth.baldur.support._
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.producer._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka._
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

trait Support {
  val ExternalPersonIdField = "sourcePersonId"
  val ExternalRecordIdField = "sourceRecordId"

  def createDirectStream(ssc: StreamingContext,
    kafkaParams: Map[String, String],
    topics: Set[String],
    config: Map[String, String]=Map("maxRatePerPartition" -> 0.toString)): InputDStream[(String, String)] =
    MoreKafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, config, topics)

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

  def toUpperCaseOption(str: Option[String]): Option[String] = {
    str.isDefined match {
      case false => None
      case true => Some(str.get.toUpperCase)
    }
  }

  def toLowerCaseOption(str: Option[String]): Option[String] = {
    str.isDefined match {
      case false => None
      case true => Some(str.get.toLowerCase)
    }
  }

  def identityScore(x: (PersonIdentityColumns, CassandraRow)): ((PersonIdentityColumns, CassandraRow), Int) = {

    var score = 0
    val unidentified = x._1
    val candidateIdentity = x._2

    // adding comparison vals. ensuring that string comparisons are not case sensitive by using toUpperCase

    val unidentifiedMrid: Option[String] = Some(f"${unidentified.source}.${unidentified.sourceType}.${unidentified.sourcePersonId}".toLowerCase)
    val identifiedMrid: Set[String] = candidateIdentity.getSet[String]("mrids").isEmpty match{
      case true => Set()
      case false => candidateIdentity.getSet[String]("mrids").map(_.toLowerCase)
    }

    val unidentifiedRootFirstName: Option[String] = toLowerCaseOption(unidentified.rootFirstName)
    val identifiedRootFirstName: Option[String] = toLowerCaseOption(candidateIdentity.get[Option[String]]("root_first_name"))

    val unidentifiedSex: Option[String] = toLowerCaseOption(unidentified.sex)
    val identifiedSex: Option[String] = toLowerCaseOption(candidateIdentity.get[Option[String]]("sex"))

    val unidentifiedAddress2: Option[String] = toLowerCaseOption(unidentified.address2)
    val identifiedAddress2: Option[String] = toLowerCaseOption(candidateIdentity.get[Option[String]]("address2"))

    val unidentifiedStreetSecondNumber: Option[String] = toLowerCaseOption(unidentified.streetSecondNumber)
    val identifiedStreetSecondNumber: Option[String] = toLowerCaseOption(candidateIdentity.get[Option[String]]("street_second_number"))

    val unidentifiedMiddleName: Option[String] = toLowerCaseOption(unidentified.middleName)
    val identifiedMiddleName: Option[String] = toLowerCaseOption(candidateIdentity.get[Option[String]]("middle_name"))

    val unidentifiedSuffix: Option[String] = toLowerCaseOption(unidentified.suffix)
    val identifiedSuffix: Option[String] = toLowerCaseOption(candidateIdentity.get[Option[String]]("suffix"))



    score += points(unidentifiedMrid, identifiedMrid, 1000000)

    score += points(unidentifiedRootFirstName, identifiedRootFirstName, 1000000)

    score += points(unidentified.dob, candidateIdentity.get[Option[DateTime]]("dob"), 100000)

    score += points(unidentifiedSex, identifiedSex, 10000)

    score += points(unidentifiedAddress2, identifiedAddress2, 1000)

    score += points(unidentifiedStreetSecondNumber, identifiedStreetSecondNumber, 1000)

    score += points(unidentifiedMiddleName, identifiedMiddleName, 100)

    score += points(unidentifiedSuffix, identifiedSuffix, 10)

    (x, score)
  }

  def toDateTime(value: String): DateTime = {
    DateTime.parse(value, ISODateTimeFormat.basicDate())
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

  def mapSourceIdentityToPersonId(x: (PersonIdentityColumns, Seq[(CassandraRow, Int)])): (SourceIdentity, UUID) = {
    val matches = x._2
    val personIdentity = x._1

    val maxScore: Int = matches.map{case (row, score) => score}.max
    val matchedId: UUID =
      matches
      .filter{ case (row, score) => score.equals(maxScore)}
      .map{ case (row, score) => row.getUUID("person_id")}
      .head

    (personIdentity.sourceIdentity, matchedId)
  }

  def mapSourceIdentityUntrustedToPersonId(x: (PersonIdentityColumns, Seq[(CassandraRow, Int)])): (SourceIdentityUntrusted, UUID) = {
    val matches = x._2
    val personIdentity = x._1

    val maxScore: Int = matches.map{case (row, score) => score}.max
    val matchedId: UUID =
      matches
        .filter{ case (row, score) => score.equals(maxScore)}
        .map{ case (row, score) => row.getUUID("person_id")}
        .head

    (personIdentity.sourceIdentityUntrusted, matchedId)
  }

  def identifyByKey(unidentifiedRecords: RDD[PersonIdentityColumns],
                    cassandraTable: (String, String)): RDD[(SourceIdentity, UUID)] =
  {
    val joinedWithTable: RDD[(PersonIdentityColumns, CassandraRow)] = unidentifiedRecords
      .joinWithCassandraTable(cassandraTable._1, cassandraTable._2)

    val grouped: RDD[(PersonIdentityColumns, Seq[(CassandraRow, Int)])] = joinedWithTable
      .map(identityScore)
      .filter(filterPositiveValue)
      .map{ case ((personIdentity, cassRow), score) => (personIdentity, (cassRow, score))}
      .spanByKey

      grouped
        .map(mapSourceIdentityToPersonId)
  }

  def identifyByKeyUntrusted(unidentifiedRecords: RDD[PersonIdentityColumns],
                    cassandraTable: (String, String)): RDD[(SourceIdentityUntrusted, UUID)] =
  {
    val joinedWithTable: RDD[(PersonIdentityColumns, CassandraRow)] = unidentifiedRecords
      .joinWithCassandraTable(cassandraTable._1, cassandraTable._2)

    val grouped: RDD[(PersonIdentityColumns, Seq[(CassandraRow, Int)])] = joinedWithTable
      .map(identityScore)
      .filter(filterPositiveValue)
      .map{ case ((personIdentity, cassRow), score) => (personIdentity, (cassRow, score))}
      .spanByKey

    grouped
      .map(mapSourceIdentityUntrustedToPersonId)
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

  def updateIdentifiedPersonsUntrusted(sourceIdentityToPersonId: RDD[(SourceIdentityUntrusted, (Option[UUID], PersonIdentityColumns))],
                              identified: RDD[(SourceIdentityUntrusted, UUID)]): RDD[(SourceIdentityUntrusted, (Option[UUID], PersonIdentityColumns))] =
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

