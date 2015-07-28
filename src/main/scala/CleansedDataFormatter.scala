import org.apache.kafka.clients.producer._
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import org.joda.time.format._
import play.api.libs.json._

object CleansedDataFormatter {
  def processRDD(rdd: RDD[Array[Any]], mapping: Seq[Product], producerConfig: Map[String, String],
    customerId: Int, outputTopic: String, source: String, sourceType: String, sourceDescription: String): Unit = {

    val fieldNames = mapping.map {
      case (fieldName: String, fieldType) => fieldName
      case (fieldName: String, fieldType, format) => fieldName
    }

    val zippedKeyValuePairs = rdd
      .map(fieldNames.zip(_))
      .foreachPartition { partition =>
        partition.foreach { row =>
          var jsonRow = row.map {
            case (key, value: String) => (key, JsString(value))
            case (key, value: Int) => (key, JsNumber(value))
            case (key, value: Float) => (key, JsNumber(BigDecimal.valueOf(value.toDouble)))
            case (key, value: DateTime) => (key, JsString(ISODateTimeFormat.basicDate().print(value)))
            case (key, None) => (key, JsNull)
          }.toMap[String, JsValue] ++ Map("customerId" -> JsNumber(customerId), "source" -> JsString(source), "sourceType" -> JsString(sourceType), "sourceDescription" -> JsString(sourceDescription), "personType" -> JsString("c"))

          if (jsonRow("zip5").as[String].contains("-")) {
            val parts = jsonRow("zip5").as[String].split("-")
            jsonRow = jsonRow ++ Map("zip5" -> JsString(parts(0)), "zip4" -> JsString(parts(1)))
          }

          if (jsonRow("source").as[String] == "physician office"){
            val secondaryDxIds = concat(jsonRow("dxTwoId").as[String],jsonRow("dxThreeId").as[String],jsonRow("dxFourId").as[String],jsonRow("dxFiveId").as[String],jsonRow("dxSixId").as[String])
            jsonRow = jsonRow ++ Map("secondaryDxIds" -> JsString(secondaryDxIds))
          }

          val jsonRowString = Json.stringify(Json.toJson(jsonRow))
          val producer = ProducerObject.get(producerConfig)
          producer.send(new ProducerRecord[String, String](outputTopic, jsonRowString))
        }
     }
  }

  def concat(ss: String*) = ss filter (_.nonEmpty) mkString ", "
}
