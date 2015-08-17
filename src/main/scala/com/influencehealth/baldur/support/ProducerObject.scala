package com.influencehealth.baldur.support

import org.apache.kafka.clients.producer._

import scala.collection.JavaConversions._

object ProducerObject {
  private var producer: Option[KafkaProducer[String, String]] = None

  def get(properties: Map[String, Object]): KafkaProducer[String, String] = producer match {
    case Some(producer) => producer
    case None =>
      producer = Some(new KafkaProducer[String, String](properties))
      producer.get
  }
}
