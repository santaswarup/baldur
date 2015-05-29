import kafka.producer.KeyedMessage
import kafka.producer.Producer
import org.apache.spark.rdd.RDD

object StatsReporter {
  def processRDD(rdd: RDD[Array[Any]], fieldsMeta: Seq[Product], producer: Producer[String, String]): Unit = {

    val indexOfAge = fieldsMeta.indexWhere(x => x match {
      case ("age", _) =>
        true
      case _ =>
        false
    })
    println("INDEXOFAGE" + indexOfAge)
    println("FIELDSMETA" + fieldsMeta(indexOfAge))

    val averageAge = rdd.map(x => {
      println("LENGTH OF X: " + x.length)
      println("X IS" + x.mkString(" ") + x.getClass)
      x(indexOfAge).asInstanceOf[Int]
    })
      .fold(0)((a, b) => a + b)

    producer.send(new KeyedMessage("baldur.stats", "{ \"averageAge\": " + averageAge.toString + ", \"records\": " + rdd.count().toString + "}"))
  }
}