import kafka.producer.{ProducerConfig, Producer}
import java.util.Properties

import org.apache.spark.broadcast.Broadcast

object ProducerObject {
  private var producerOpt: Option[Producer[String, String]] = None

  def get(properties: Broadcast[Properties]): Producer[String, String] = producerOpt match {
    case Some(producerOpt) => producerOpt
    case None =>
      producerOpt = Some(new Producer[String, String](new ProducerConfig(properties.value)))
      producerOpt.get
  }
}