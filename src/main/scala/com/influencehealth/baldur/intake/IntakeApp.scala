package com.influencehealth.baldur.intake

import java.io.{FileOutputStream, PrintWriter}
import java.net.URI

import com.influencehealth.baldur.intake.meta._
import com.influencehealth.baldur.support._
import com.influencehealth.baldur.intake.config._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

object IntakeApp {
  val OutputTopicKey = "spark.app.topic.activity"
  val StringSerializer = "org.apache.kafka.common.serialization.StringSerializer"

  def main (args: Array[String]): Unit = {
    val config = IntakeConfig.getConfig(args)
    val sparkConf = createSparkConf()
    val sc = new SparkContext(sparkConf)

    // Set up Kafka producer
    val kafkaProducerConfig = Map("client.id" -> "Baldur",
      "bootstrap.servers" -> config.brokerList,
      "key.serializer" -> StringSerializer,
      "value.serializer" -> StringSerializer)

    // Client document structure
    val clientInputMeta = getClientInputMeta(config.client, config.source, config.sourceType, config.delimiter)
    val fieldsMapping = sc.broadcast(clientInputMeta.originalFields())
    val delimiter = clientInputMeta.delimiter.replace("|", "\\|")

    val fieldNames: Seq[String] = fieldsMapping.value.map {
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

    val today: String = ISODateTimeFormat.basicDateTime().print(DateTime.now())
    val outputPath = config.out.getPath.last match {
      case '/' => config.out.getPath + "baldur_output_" + today + ".txt"
      case _ => config.out.getPath + "/baldur_output_" + today + ".txt"
    }

    // Next map them to the field names
    cleansedLines
      .map(fieldNames.zip(_))
      // Now loop through the mapped lines, map to the ActivityOutput class, and send the messages
      .foreachPartition { partition =>

      val mappedLines: String = partition
      .map{case line =>
       clientInputMeta.mapping(line.toMap[String, Any]).productIterator.map(ActivityOutput.toStringFromActivity)
        .mkString("|")
      }.mkString("\n")

      val outputFile = new FileOutputStream(outputPath, true)
      val writer = new PrintWriter(outputFile)

      // Create file for anchor
      writer.append(mappedLines)
      writer.close()
      outputFile.close()
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

  def extractFieldNames[T<:Product:Manifest] = {
    implicitly[Manifest[T]].runtimeClass.getDeclaredFields.map(_.getName)
  }

}
