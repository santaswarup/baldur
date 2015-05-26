
object ProducerObject {
  private var producerOpt: Option[Any] = None

  def getCachedProducer: Any = {
    producerOpt.get
  }

  def cacheProducer(producer: Any): Unit = {
    producerOpt = Some(producer)
  }

  def isCached: Boolean = {
    producerOpt.isDefined
  }
}
