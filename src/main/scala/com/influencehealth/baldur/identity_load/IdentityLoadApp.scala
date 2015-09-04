package com.influencehealth.baldur.identity_load

import java.util.UUID

import com.datastax.spark.connector.cql.CassandraConnector
import com.influencehealth.baldur.identity_load.meta._
import com.influencehealth.baldur.identity_load.meta.experian.ExperianSupport
import com.influencehealth.baldur.support._
import com.influencehealth.baldur.identity_load.config._
import com.influencehealth.baldur.identity_load.person_identity.change_capture.ChangeCaptureStream
import com.influencehealth.baldur.identity_load.person_identity.change_capture.support.{ChangeCaptureMessage, ChangeCaptureConfig, ColumnChange}
import com.influencehealth.baldur.identity_load.person_identity.householding.HouseholdStream
import com.influencehealth.baldur.identity_load.person_identity.householding.support.HouseholdConfig
import com.influencehealth.baldur.identity_load.person_identity.identity.IdentityStream
import com.influencehealth.baldur.identity_load.person_identity.identity.support.PersonIdentityConfig
import com.influencehealth.baldur.identity_load.person_identity.identity_table.IdentityTableCreator
import com.influencehealth.baldur.identity_load.person_identity.identity_table.support.IdentityTableCreatorConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.spark._
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import play.api.libs.json._

object IdentityLoadApp {
  val OutputTopicKey = "spark.app.topic.anchor"
  val StringSerializer = "org.apache.kafka.common.serialization.StringSerializer"

  def main (args: Array[String]): Unit = {
    val config = IdentityLoadConfig.getConfig(args)
    val sparkConf = createSparkConf()
    val sc = new SparkContext(sparkConf)

    val personIdentityConfig = PersonIdentityConfig.read(sparkConf)
    val householdConfig = HouseholdConfig.read(sparkConf)
    val changeCaptureConfig = ChangeCaptureConfig.read(sparkConf)
    val identityTableCreatorConfig = IdentityTableCreatorConfig.read(sparkConf)

    val cassandraConnector = CassandraConnector(sparkConf)

    val kafkaParams: Map[String, String] = Map("metadata.broker.list" -> personIdentityConfig.brokerList,
      "auto.offset.reset" -> personIdentityConfig.kafkaReset, "client.id" -> "identity_load")

    val kafkaProducerConfig = Map[String, Object](
      "bootstrap.servers" -> personIdentityConfig.brokerList,
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> StringSerializer,
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> StringSerializer)

    val fileInputMeta = getFileInputMeta(config.inputSource)
    val fieldsMapping = sc.broadcast(fileInputMeta.originalFields())
    val delimiter = fileInputMeta.delimiter

    val fieldNames: Seq[String] = fieldsMapping.value.map {
      case (fieldName: String, fieldType) => fieldName
      case (fieldName: String, fieldType, format) => fieldName
    }

    // Leveraging the baldur "Clean" class to parse the file into standard data types
    // TODO - put the Clean class in its own repo, import it to whatever projects need it
    val input: RDD[Seq[(String, Any)]] = sc.textFile(config.in.getPath,personIdentityConfig.filePartitions)
      .map(line => line.split(delimiter))
      .map(fields => {
        try {
          val cleansedFields = fields.zip(fieldsMapping.value).map(Clean.byType)
          cleansedFields
        } catch {
          case err: Throwable =>
            val fieldsStr = fields.mkString(",")
            throw new Error(
              f"Cleansing row failed:\n$fieldsStr\n",
              err)
        }
      })
      .map(fieldNames.zip(_))
      .distinct()

    // Experian files need to append a customer id and limit the data to defined zips for said clients. an added flatMap
    // handles this
    val inputMapped = config.inputSource match{
      case "baldur" =>
        input.map{ line => Json.toJson(ActivityOutput.mapJsonFields(fileInputMeta.mapping(line.toMap[String, Any]))).as[JsObject]}
        .persist(StorageLevel.MEMORY_AND_DISK_SER)

      case "experian" =>
        input
          .flatMap{ line => ExperianSupport.appendClientIds(line.toMap[String, Any])}
          .map{ line => Json.toJson(ActivityOutput.mapJsonFields(fileInputMeta.mapping(line))).as[JsObject]}
          .persist(StorageLevel.MEMORY_AND_DISK_SER)
    }


    val output =
      inputMapped
      .addIdentity(personIdentityConfig, kafkaParams, kafkaProducerConfig)
      .addHouseholds(householdConfig, kafkaProducerConfig)
      .toChangeCaptureMessages
      .captureChanges(changeCaptureConfig, cassandraConnector, kafkaProducerConfig)
      .toCustomerIdPersonIdTuples
      .createIdentityTables(identityTableCreatorConfig)

    println("Identity Stream Outputs: " + output.count())

  }

  implicit class KafkaStreamExtensions(val rdd: RDD[(String, String)]) {
    def getBodyAsJsObj = {
      rdd.map {
        case (_, value) => Json.parse(value).as[JsObject]
      }
    }
  }

  implicit class JsObjectExtensions(val rdd: RDD[JsObject]) {
    def addIdentity(personIdentityConfig: PersonIdentityConfig,
                    kafkaParams: Map[String, String],
                    kafkaProducerConfig: Map[String, Object]) = {

        IdentityStream.processIdentity(rdd, personIdentityConfig, kafkaParams, kafkaProducerConfig)
    }


    def addHouseholds(householdConfig: HouseholdConfig, kafkaProducerConfig: Map[String, Object]) = {
      HouseholdStream.processHouseholds(householdConfig, rdd, kafkaProducerConfig)
    }

    def toChangeCaptureMessages = {
      rdd.map(ChangeCaptureMessage.create)
    }
  }

  implicit class ChangeCaptureMessageExtensions(val rdd: RDD[ChangeCaptureMessage]) {
    def captureChanges(changeCaptureConfig: ChangeCaptureConfig, cassandraConnector: CassandraConnector,
                       kafkaProducerConfig: Map[String, Object]) = {
      ChangeCaptureStream.processChanges(changeCaptureConfig, cassandraConnector, kafkaProducerConfig, rdd)
    }
  }

  implicit class ColumnChangeExtension(val rdd: RDD[ColumnChange]) {
    def toCustomerIdPersonIdTuples = {
      rdd.map(columnChange => (columnChange.customerId, columnChange.personId))
    }
  }

  implicit class IdentityTableCreatorExtensions(val rdd: RDD[(Int, UUID)]) {
    def createIdentityTables(identityTableCreatorConfig: IdentityTableCreatorConfig) = {
          IdentityTableCreator.createIdentityTables(rdd.distinct(), identityTableCreatorConfig)
    }
  }

  // Determining the mappings based on command line arguments
  def getFileInputMeta(inputSource: String, overrideDelimiter: Option[String] = None): FileInputMeta = {
    val inputMeta = inputSource match {
      case "baldur" =>
        com.influencehealth.baldur.identity_load.meta.baldur.BaldurSchema
      case "prospects" =>
        com.influencehealth.baldur.identity_load.meta.experian.ExperianSchema
      case _ =>
        throw new IllegalArgumentException(f"Metadata for parsing files of type $inputSource not found")
    }

    if (overrideDelimiter.isDefined)
      inputMeta.delimiter = overrideDelimiter.get

    inputMeta
  }

  def createSparkConf(): SparkConf = {
    new SparkConf()
      .setAppName("Identity Load")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  }

}
