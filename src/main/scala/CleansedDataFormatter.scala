import kafka.producer.{KeyedMessage, Producer}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import play.api.libs.json._
import java.util.{Properties, Date}
import org.joda.time.DateTime
import org.joda.time.format._

object CleansedDataFormatter {
  def processRDD(rdd: RDD[Array[Any]], mapping: Seq[Product], producerConfig: Broadcast[Properties], clientKey: String, outputTopic: String): Unit = {

    val fieldNames = mapping.map {
      case (fieldName: String, fieldType) => fieldName
      case (fieldName: String, fieldType, format) => fieldName
    }

    val zippedKeyValuePairs = rdd
      .map(fieldNames.zip(_))
      .foreach(row => {
        var jsonRow = row.map {
          case (key, value: String) => (key, JsString(value))
          case (key, value: Int) => (key, JsNumber(value))
          case (key, value: Float) => (key, JsNumber(BigDecimal.valueOf(value)))
          case (key, value: DateTime) => (key, JsString(ISODateTimeFormat.basicDate().print(value)))
        }.toMap[String, JsValue]

        if (jsonRow("zip5").as[String].contains("-")) {
          val parts = jsonRow("zip5").as[String].split("-")
          jsonRow = jsonRow ++ Map("zip5" -> JsString(parts(0)), "zip4" -> JsString(parts(1)))
        }

        val jsonRowString = Json.stringify(Json.toJson(jsonRow))
        val producer = ProducerObject.get(producerConfig)
        //println("KEY: " + clientKey + partitionKey + " VALUE:" + jsonRowString)
        producer.send(new KeyedMessage(outputTopic, jsonRowString))
      })
  }
}
