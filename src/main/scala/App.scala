import java.net.URI

import meta.{ActivityOutput, ClientInputMeta}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import play.api.libs.json._

object App {
  val OutputTopicKey = "spark.app.topic.activity"
  val StringSerializer = "org.apache.kafka.common.serialization.StringSerializer"

  def main (args: Array[String]): Unit = {
    val config = BaldurConfig.getConfig(args)
    val sparkConf = createSparkConf()
    val sc = new SparkContext(sparkConf)

    val outputTopic = sparkConf.get(OutputTopicKey, "unidentified_encounters")

    // Set up Kafka producer
    val kafkaProducerConfig = Map("client.id" -> "Baldur",
      "bootstrap.servers" -> config.brokerList,
      "key.serializer" -> StringSerializer,
      "value.serializer" -> StringSerializer)

    // Client document structure
    val clientInputMeta = getClientInputMeta(config.client, config.source, config.sourceType, config.delimiter)
    val fieldsMapping = sc.broadcast(clientInputMeta.originalFields())
    val customerId = sc.broadcast(clientInputMeta.CustomerId)
    val delimiter = clientInputMeta.delimiter.replace("|", "\\|")

    val fieldNames = fieldsMapping.value.map {
      case (fieldName: String, fieldType) => fieldName
      case (fieldName: String, fieldType, format) => fieldName
    }

    // First cleanse the original input values
    val cleansedLines: RDD[Array[Any]] = sc
      .textFile(config.in.getPath)
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


    // Next map them to the field names
    cleansedLines
      .map(fieldNames.zip(_))
      // Now loop through the mapped lines, map to the ActivityOutput class, and send the messages
      .foreachPartition { partition =>
      partition.foreach { row =>
        val mappedRow = row.map {
          case (key, value: Any) => (key, value)
        }.toMap[String, Any]

        // Standard lines
        val standardLines: ActivityOutput = clientInputMeta.mapping(mappedRow)

        //Create Json for sending
         val jsonRowString = Json.stringify(Json.toJson(ActivityOutput.mapJsonFields(standardLines)))
         val producer = ProducerObject.get(kafkaProducerConfig)
         producer.send(new ProducerRecord[String, String](outputTopic, jsonRowString))
      }
    }

    StatsReporter.processRDD(cleansedLines, fieldsMapping.value, kafkaProducerConfig)
  }

  def getClientInputMeta(client: String, source: String, sourceType: String, overrideDelimiter: Option[String]): ClientInputMeta = {
    val inputMeta = (client, source, sourceType) match {
      case ("piedmont", "epic", "hospital") =>
        meta.piedmont.Hospital
      case ("piedmont", "epic", "physician office") =>
        meta.piedmont.PhysicianOffice
      case _ =>
        throw new IllegalArgumentException(f"Metadata for parsing files of type $source for client $client not found")
    }

    if (overrideDelimiter.isDefined)
      inputMeta.delimiter = overrideDelimiter.get

    inputMeta
  }

  def createSparkConf(): SparkConf = {
    new SparkConf().setAppName("Utilization Cleansing")
  }

  def createInputStreamingContext(sparkConf: SparkConf, uri: URI, duration: Duration): StreamingContext = {
    new StreamingContext(sparkConf, duration)
  }
}
