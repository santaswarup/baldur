import java.net.URI

import scopt.OptionParser
import org.apache.spark._
import org.apache.spark.streaming._

/**
 * Created by nathan on 5/23/15.
 */
object App {
  case class Config(in: java.net.URI=new java.net.URI("in"), out: java.net.URI=new java.net.URI("out"),
                     separator: String="|", client: String="")

  def main (args: Array[String]): Unit = {
    val optionParser = new OptionParser[Config]("Baldur") {
      opt[java.net.URI]('i', "in") required() valueName("<input_directory>") action { (x, c) =>
        c.copy(in = x)
      }

      opt[java.net.URI]('o', "out") required() valueName("<output_directory>") action { (x, c) =>
        c.copy(out = x)
      }

      opt[String]('s', "separator") valueName("<separator>") action { (x, c) =>
        c.copy(separator = x)
      }

      opt[String]('c', "client") required() valueName("<client_key>") action { (x, c) =>
        c.copy(client = x)
      }
    }

    optionParser.parse(args, Config()) match {
      case Some(config) =>
        val sparkConf = createSparkConf()

        val streamingContext = createInputStreamingContext(sparkConf, config.in, Minutes(1));

        val separator = streamingContext.sparkContext.broadcast(config.separator)
        val fieldsMeta = streamingContext.sparkContext.broadcast(Array(
          ("hospital_account_id", "string"),
          ("patient_id", "string"),
          ("facility_id", "string"),
          ("facility_name", "string"),
          ("facility_address", "string"),
          ("facility_city", "string"),
          ("facility_state", "string"),
          ("facility_zip", "string"),
          ("last_name", "string"),
          ("first_name", "string"),
          ("patient_address", "string"),
          ("patient_city", "string"),
          ("patient_state", "string"),
          ("patient_zip", "string"),
          ("sex", "string"),
          ("sex_description", "string"),
          ("age", "int"),
          ("dob", "date", "dd/MM/yyyy"),
          ("home_phone", "string"),
          ("birth_year", "int"),
          ("birth_day", "int"),
          ("birth_month", "string"),
          ("discharge_date", "date", "dd/MM/yyyy"),
          ("payor_id", "string"),
          ("payor_name", "string"),
          ("patient_type", "string"),
          ("patient_type_short", "string"),
          ("visit_type", "string"),
          ("department_id", "string"),
          ("department_name", "string"),
          ("patient_email", "string"),
          ("patient_id_extra", "string"),
          ("primary_dx_id", "string"),
          ("primary_dx_description", "string"),
          ("secondary_dx_ids", "string"),
          ("primary_procedure_id", "string"),
          ("secondary_procedures", "string"),
          ("final_drg_cd", "string")))

        val lines = streamingContext.textFileStream(config.in.getPath())

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
              case _ =>
                throw new Error("Metadata not understood")
            })

            cleansedFields foreach (x => println(x))

            cleansedFields
          })
          .cache()

        cleansedLines.saveAsTextFiles(config.out.getPath() + "/" + config.client, "txt")

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

  def createSparkConf(): SparkConf = {
    return new SparkConf().setAppName("Utilization Cleansing")
  }

  def createInputStreamingContext(sparkConf: SparkConf, uri: URI, duration: Duration): StreamingContext = {
    return new StreamingContext(sparkConf, duration)
  }
}
