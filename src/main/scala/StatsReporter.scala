import org.apache.kafka.clients.producer._
import org.apache.spark.rdd.RDD

object StatsReporter {
  def processRDD(rdd: RDD[Array[Any]], mapping: Seq[Product], producerConfig: Map[String, String]): Unit = {

    val indexOfAge = mapping.indexWhere(x => x match {
      case ("age", _) =>
        true
      case _ =>
        false
    })

    val averageAge = rdd.map(x => {
      val y = x.toString

      if (x.length < indexOfAge)
        throw new Error("row too skinny:\n"+x.length+" "+x(0))
      y match {
        case "" => -1
        case _ => x.asInstanceOf[Int]
      }
    }).filter(x => x >= 0)
      .fold(0)((a, b) => a + b)

    val producer = ProducerObject.get(producerConfig)

    val jsonStr = "{ \"averageAge\": " + averageAge.toString + ", \"records\": " + rdd.count().toString + "}"
    println("SENDING JSON: "+jsonStr)
    producer.send(new ProducerRecord[String, String]("baldur.stats", jsonStr))
  }
}
