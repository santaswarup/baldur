import java.util.Properties

import kafka.producer.KeyedMessage
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object StatsReporter {
  def processRDD(rdd: RDD[Array[Any]], mapping: Seq[Product], producerConfig: Broadcast[Properties]): Unit = {

    val indexOfAge = mapping.indexWhere(x => x match {
      case ("age", _) =>
        true
      case _ =>
        false
    })

    val averageAge = rdd.map(x => {
      x(indexOfAge).asInstanceOf[Int]
    })
      .fold(0)((a, b) => a + b)

    val producer = ProducerObject.get(producerConfig)

    producer.send(new KeyedMessage("baldur.stats", "{ \"averageAge\": " + averageAge.toString + ", \"records\": " + rdd.count().toString + "}"))
  }
}