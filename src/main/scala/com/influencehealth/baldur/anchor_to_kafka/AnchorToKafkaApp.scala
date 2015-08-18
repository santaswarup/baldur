package com.influencehealth.baldur.anchor_to_kafka

import com.influencehealth.baldur.anchor_to_kafka.meta._
import com.influencehealth.baldur.support._
import com.influencehealth.baldur.anchor_to_kafka.config._
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import play.api.libs.json._

object AnchorToKafkaApp {
  val OutputTopicKey = "spark.app.topic.anchor"
  val StringSerializer = "org.apache.kafka.common.serialization.StringSerializer"

  def main (args: Array[String]): Unit = {
    val config = AnchorToKafkaConfig.getConfig(args)
    val sparkConf = createSparkConf()
    val sc = new SparkContext(sparkConf)

    // Set up Kafka producer
    val kafkaProducerConfig = Map("client.id" -> "Anchor_to_Kafka",
      "bootstrap.servers" -> config.brokerList,
      "key.serializer" -> StringSerializer,
      "value.serializer" -> StringSerializer)

    val fileInputMeta = getFileInputMeta(config.inputSource)
    val fieldsMapping = sc.broadcast(fileInputMeta.originalFields())
    val delimiter = fileInputMeta.delimiter

    val fieldNames: Seq[String] = fieldsMapping.value.map {
      case (fieldName: String, fieldType) => fieldName
      case (fieldName: String, fieldType, format) => fieldName
    }

    // Leveraging the baldur "Clean" class to parse the file into standard data types
    // TODO - put the Clean class in its own repo, import it to whatever projects need it
    val input = sc.textFile(config.in.getPath)
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
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    // Send to Kafka
    input
      .map(fieldNames.zip(_))
      .foreachPartition{ partition =>
        partition
          .map{case line =>
            // String JSON message representation
            // fileInputMeta.mapping() handles the map to the ActivityOutput class
            // ActivityOutput.mapJsonFields converts data into JsObjects
            // Stringify creates the json string to send to Kafka
            Json.stringify(Json.toJson(ActivityOutput.mapJsonFields(fileInputMeta.mapping(line.toMap[String, Any]))))
          }
          .foreach { line =>
            val producer = ProducerObject.get(kafkaProducerConfig)
            producer.send(new ProducerRecord[String, String](config.outputTopic, line))
          }

      }
  }

  // Determining the mappings based on command line arguments
  def getFileInputMeta(inputSource: String, overrideDelimiter: Option[String] = None): FileInputMeta = {
    val inputMeta = inputSource match {
      case "baldur" =>
        com.influencehealth.baldur.anchor_to_kafka.meta.baldur.BaldurSchema
      case "prospects" =>
        com.influencehealth.baldur.anchor_to_kafka.meta.experian.ExperianSchema
      case _ =>
        throw new IllegalArgumentException(f"Metadata for parsing files of type $inputSource not found")
    }

    if (overrideDelimiter.isDefined)
      inputMeta.delimiter = overrideDelimiter.get

    inputMeta
  }

  def createSparkConf(): SparkConf = {
    new SparkConf().setAppName("Anchor to Kafka")
  }

}
