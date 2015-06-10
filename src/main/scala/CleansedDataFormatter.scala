import kafka.producer.{KeyedMessage, Producer}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import play.api.libs.json._
import java.util.{Properties, Date}

object CleansedDataFormatter {
  def processRDD(rdd: RDD[Array[Any]], mapping: Seq[Product], producerConfig: Broadcast[Properties], clientKey: String): Unit = {

    val fieldNames = mapping.map {
      case (fieldName: String, fieldType) => fieldName
      case (fieldName: String, fieldType, format) => fieldName
    }

    val zipIndex = fieldNames
    .indexWhere(x => x match {
      case ("patient_zip") => true
      case _ => false
    })

    val zippedKeyValuePairs = rdd
      .map(fieldNames.zip(_))
      .foreach(row => {
        val jsonRow = row.map {
          case (key, value: String) => (key, JsString(value))
          case (key, value: Int) => (key, JsNumber(value))
          case (key, value: Float) => (key, JsNumber(BigDecimal.valueOf(value)))
          case (key, value: Date) => (key, JsString(value.toString))
        }

        val jsonRowString = Json.stringify(Json.toJson(jsonRow.toMap[String, JsValue]))
        val partitionKey = row(zipIndex)._2
        val producer = ProducerObject.get(producerConfig)
        //println("KEY: " + clientKey + partitionKey + " VALUE:" + jsonRowString)
        producer.send(new KeyedMessage("baldur.activity", clientKey + partitionKey, jsonRowString))
      })
  }
}