import java.net.URI

import org.apache.spark._
import org.apache.spark.streaming._
import scopt.OptionParser

object App {
  case class Config(in: java.net.URI=new java.net.URI("in"), out: java.net.URI=new java.net.URI("out"),
                     separator: String="|", client: String="", inputType: String="")

  def main (args: Array[String]): Unit = {
    val optionParser = new OptionParser[Config]("Baldur") {
      opt[java.net.URI]('i', "in") required() valueName "<input_directory>" action { (x, c) =>
        c.copy(in = x)
      }

      opt[java.net.URI]('o', "out") required() valueName "<output_directory>" action { (x, c) =>
        c.copy(out = x)
      }

      opt[String]('s', "separator") valueName "<separator>" action { (x, c) =>
        c.copy(separator = x)
      }

      opt[String]('c', "client") required() valueName "<client_key>" action { (x, c) =>
        c.copy(client = x)
      }

      opt[String]("type") required() valueName "<input_type>" action { (x, c) =>
        c.copy(inputType = x)
      }
    }

    optionParser.parse(args, Config()) match {
      case Some(config) =>
        val sparkConf = createSparkConf()

        val streamingContext = createInputStreamingContext(sparkConf, config.in, Minutes(1));

        val separator = streamingContext.sparkContext.broadcast(config.separator)

        val clientInputMeta = getClientInputMeta(config.client, config.inputType)
        if (clientInputMeta.isEmpty) {
          val client = config.client
          val inputType = config.inputType
          throw new IllegalArgumentException(f"Metadata for parsing files of type $inputType%s for client $client%s not found")
        }

        val fieldsMeta = streamingContext.sparkContext.broadcast(clientInputMeta.get)

        val lines = streamingContext.textFileStream(config.in.getPath)

        val cleansedLines = lines
          .map(line => line.split(separator.value))
          .map(fields => {
            println("fields")
            fields.zip(fieldsMeta.value).foreach(field => println(field._1 + " " + field._2))
            val cleansedFields = fields.zip(fieldsMeta.value).map({
              case (fieldValue, (_, "string")) =>
                Clean.string(fieldValue)
              case (fieldValue, (_, "int")) =>
                Clean.int(fieldValue)
              case (fieldValue, (_, "date")) =>
                Clean.date(fieldValue)
              case (fieldValue, (_, "date", format: String)) =>
                Clean.date(fieldValue, format)
              case (fieldValue, (_, "float")) =>
                Clean.float(fieldValue)
              case (fieldValue, (_, "skip")) =>
                fieldValue
              case _ =>
                throw new Error("Metadata not understood")
            })

            cleansedFields foreach (x => println(x))

            cleansedFields
          })
          .cache()

        cleansedLines.saveAsTextFiles(config.out.getPath + "/" + config.client, "txt")

        fieldsMeta.value.zipWithIndex foreach (fieldMeta => {
          cleansedLines.foreachRDD(rdd => {
            val stats = rdd.groupBy(x => {
              if (x.length > fieldMeta._2)
                x(fieldMeta._2)
              else
                None
            }).filter(x => x._1 != None).countByKey()
            stats.foreach(stat => println(stat._1 + ": " + stat._2))
          })
        })

        streamingContext.start()
        streamingContext.awaitTermination()
      case None =>
        sys.exit(1)
    }
  }

  def getClientInputMeta(client: String, inputType: String): Option[Seq[Product]] = {
    (client, inputType) match {
      case ("piedmont", "utilization") =>
        Some(meta.piedmont.Utilization.mapping())
      case ("piedmont", "physician") =>
        Some(meta.piedmont.PhysicianOffice.mapping())
      case _ =>
        None
    }
  }

  def createSparkConf(): SparkConf = {
    new SparkConf().setAppName("Utilization Cleansing")
  }

  def createInputStreamingContext(sparkConf: SparkConf, uri: URI, duration: Duration): StreamingContext = {
    new StreamingContext(sparkConf, duration)
  }
}
