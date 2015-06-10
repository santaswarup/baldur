import java.net.URI
import java.util.Properties

import meta.ClientInputMeta
import org.apache.spark._
import org.apache.spark.streaming._

object App {

  def main (args: Array[String]): Unit = {
    val config = BaldurConfig.getConfig(args)
    val sparkConf = createSparkConf()
    val streamingContext = createInputStreamingContext(sparkConf, config.in, Seconds(config.interval));

    // Set up Kafka producer
    val producerProperties = new Properties()
    producerProperties.setProperty("client.id", "Baldur")
    producerProperties.setProperty("metadata.broker.list", config.brokerList)
    producerProperties.setProperty("serializer.class", "kafka.serializer.StringEncoder")

    val producerConfig = streamingContext.sparkContext.broadcast(producerProperties)

    // Client document structure
    val clientInputMeta = getClientInputMeta(config.client, config.inputType)
    val separator = streamingContext.sparkContext.broadcast(clientInputMeta.delimiter)
    val fieldsMapping = streamingContext.sparkContext.broadcast(clientInputMeta.mapping)
    val clientKey = streamingContext.sparkContext.broadcast(clientInputMeta.ClientKey)

    // Begin streaming
    val cleansedLines = streamingContext
      .textFileStream(config.in.getPath)
      .map(line => line.split(separator.value))
      .map(fields => {
        val cleansedFields = fields.zip(fieldsMapping.value).map(Clean.byType)
        cleansedFields
      })
      .cache()

    //cleansedLines.map(rdd => rdd.mkString("\t")).saveAsTextFiles(config.out.getPath + "/" + config.client, "txt")
    cleansedLines.foreachRDD(rdd => StatsReporter.processRDD(rdd, fieldsMapping.value, producerConfig))
    cleansedLines.foreachRDD(rdd => CleansedDataFormatter.processRDD(rdd, fieldsMapping.value, producerConfig, clientKey.value))

    streamingContext.start()
    streamingContext.awaitTermination()
  }

  def getClientInputMeta(client: String, inputType: String): ClientInputMeta = {
    (client, inputType) match {
      case ("piedmont", "utilization") =>
        meta.piedmont.Utilization
      case ("piedmont", "physician") =>
        meta.piedmont.PhysicianOffice
      case _ =>
        throw new IllegalArgumentException(f"Metadata for parsing files of type ${inputType} for client ${client} not found")
    }
  }

  def createSparkConf(): SparkConf = {
    new SparkConf().setAppName("Utilization Cleansing")
  }

  def createInputStreamingContext(sparkConf: SparkConf, uri: URI, duration: Duration): StreamingContext = {
    new StreamingContext(sparkConf, duration)
  }

}
