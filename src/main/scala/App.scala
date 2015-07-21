import java.net.URI

import meta.ClientInputMeta
import org.apache.spark._
import org.apache.spark.streaming._

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
    val fieldsMapping = sc.broadcast(clientInputMeta.mapping)
    val clientKey = sc.broadcast(clientInputMeta.ClientKey)
    val delimiter = clientInputMeta.delimiter.replace("|", "\\|")
    val customerId = 1 // TODO: Make this be based off of the client key

    // Begin streaming
    val cleansedLines = sc
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
              f"Cleansing row failed:\n${fieldsStr}\n",
              err)
        }
      })
      .cache()

    StatsReporter.processRDD(cleansedLines, fieldsMapping.value, kafkaProducerConfig)
    CleansedDataFormatter.processRDD(cleansedLines, fieldsMapping.value, kafkaProducerConfig, customerId, outputTopic, config.source, config.sourceType, config.in.getPath)
  }

  def getClientInputMeta(client: String, source: String, sourceType: String, overrideDelimiter: Option[String]): ClientInputMeta = {
    val inputMeta = (client, source, sourceType) match {
      case ("piedmont", "epic", "hospital") =>
        meta.piedmont.Hospital
      case ("piedmont", "epic", "physician office") =>
        meta.piedmont.PhysicianOffice
      case _ =>
        throw new IllegalArgumentException(f"Metadata for parsing files of type ${source} for client ${client} not found")
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
