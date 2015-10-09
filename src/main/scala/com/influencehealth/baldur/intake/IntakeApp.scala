package com.influencehealth.baldur.intake

import java.io.File
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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._


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

    val today: String = ISODateTimeFormat.basicDateTime().print(DateTime.now())

    val outputPath = config.out.getPath.last match {
      case '/' => config.out.getPath + "input/baldurToAnchor_" + today + ".txt"
      case _ => config.out.getPath + "/input/baldurToAnchor_" + today + ".txt"
    }

    val outputCRCPath = config.out.getPath.last match {
      case '/' => config.out.getPath + "input/.baldurToAnchor_" + today + ".txt.crc"
      case _ => config.out.getPath + "/input/.baldurToAnchor_" + today + ".txt.crc"
    }

    val tempPath = config.out.getPath.last match {
      case '/' => config.out.getPath + "temp/baldurToAnchor_" + today + ".txt"
      case _ => config.out.getPath + "/temp/baldurToAnchor_" + today + ".txt"
    }

    val drgInputPath = config.out.getPath.last match {
      case '/' => config.out.getPath + "msDrg/input/baldur_drg_input_" + today + ".txt"
      case _ => config.out.getPath + "/msDrg/input/baldur_drg_input_" + today + ".txt"
    }

    val drgInputCrc = config.out.getPath.last match {
      case '/' => config.out.getPath + "msDrg/input/.baldur_drg_input_" + today + ".txt.crc"
      case _ => config.out.getPath + "/msDrg/input/.baldur_drg_input_" + today + ".txt.crc"
    }

    val drgTempPath = config.out.getPath.last match {
      case '/' => config.out.getPath + "msDrg/temp/baldur_drg_input_" + today + ".txt"
      case _ => config.out.getPath + "/msDrg/temp/baldur_drg_input_" + today + ".txt"
    }

    val drgOutputPath = config.out.getPath.last match {
      case '/' => config.out.getPath + "msDrg/output/baldur_drg_output_" + today + ".txt"
      case _ => config.out.getPath + "/msDrg/output/baldur_drg_output_" + today + ".txt"
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
      }).persist(StorageLevel.MEMORY_AND_DISK_SER)

    // Next map them to the field names
    val activityOutput: RDD[ActivityOutput] = cleansedLines
      .map(fieldNames.zip(_)) // Now loop through the mapped lines, map to the ActivityOutput class, make an RDD of strings
      .map {case line =>
              val mappedLine = line.map(x => (x._1, x._2)).toMap[String, Any]
              clientInputMeta.mapping(mappedLine) // creates an ActivityOutput object
       }

    //Creates the Input for the Drg Software
    val drgInput: RDD[String] = activityOutput.map(Drg.createDrgInput)

    drgInput.saveAsTextFile(drgTempPath)

    merge(drgTempPath, outputPath)
    FileUtil.fullyDelete(new File(drgTempPath))
    new File(drgInputCrc).delete()

    // Saves to multiple files in a directory. We use the tempPath as a storage area for this
   activityOutput
     .map { case activity =>
     activity.productIterator // maps all values from the ActivityOutput object to an iterator ordered by the class's definition
       .map(ActivityOutput.toStringFromActivity) // takes all values, makes them a string
       .mkString("|") + "\r" // coalesce all values in iterator, separated by pipes. added return carriage for Anchr}
   }.saveAsTextFile(tempPath)

    // Merge the results into the desired output path
    merge(tempPath, outputPath)

    // Delete the temp results
    FileUtil.fullyDelete(new File(tempPath))

    // Clean up CRC file
    new File(outputCRCPath).delete()

    // Process the statistics of the RDD
    StatsReporter.processRDD(cleansedLines, fieldsMapping.value, kafkaProducerConfig)

    cleansedLines.unpersist()

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

  def merge(srcPath: String, dstPath: String): Unit =  {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, hadoopConfig, null)

  }


}
