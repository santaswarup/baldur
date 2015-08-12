package meta

import org.joda.time.DateTime

/**
 * Defines the contract for input metadata defining implementors.
 */
trait ClientInputMeta extends ClientSpec {
  def originalFields(): Seq[scala.Product]
  def mapping(map: Map[String, Any]): ActivityOutput
  var delimiter: String = "\t"

  def getStringValue(map: Map[String, Any], columnName: String): String = {
    map
      .filter { case (key, value) => key.equals(columnName) }
      .map { case (key, value) => value }.head.toString
  }

  def getDateValue(map: Map[String, Any], columnName: String): DateTime = {
    DateTime.parse(map
      .filter { case (key, value) => key.equals(columnName) }
      .map { case (key, value) => value }.head.toString)
  }

  def getFloatValue(map: Map[String, Any], columnName: String): Float = {
    map
      .filter { case (key, value) => key.equals(columnName) }
      .map { case (key, value) => value }.head.toString.toFloat
  }

  def getDoubleValue(map: Map[String, Any], columnName: String): Double = {
    map
      .filter { case (key, value) => key.equals(columnName) }
      .map { case (key, value) => value }.head.toString.toDouble
  }

  def getLongValue(map: Map[String, Any], columnName: String): Long = {
    map
      .filter { case (key, value) => key.equals(columnName) }
      .map { case (key, value) => value }.head.toString.toLong
  }

  def getIntValue(map: Map[String, Any], columnName: String): Int = {
    map
      .filter { case (key, value) => key.equals(columnName) }
      .map { case (key, value) => value }.head.toString.toInt
  }

  def getStringOptValue(map: Map[String, Any], columnName: String): Option[String] = {
    Some(map
      .filter { case (key, value) => key.equals(columnName) }
      .map { case (key, value) => value }.head.toString)
  }

  def getDateOptValue(map: Map[String, Any], columnName: String): Option[DateTime] = {
    Some(DateTime.parse(map
      .filter { case (key, value) => key.equals(columnName) }
      .map { case (key, value) => value }.head.toString))
  }

  def getFloatOptValue(map: Map[String, Any], columnName: String): Option[Float] = {
    Some(map
      .filter { case (key, value) => key.equals(columnName) }
      .map { case (key, value) => value }.head.toString.toFloat)
  }

  def getDoubleOptValue(map: Map[String, Any], columnName: String): Option[Double] = {
    Some(map
      .filter { case (key, value) => key.equals(columnName) }
      .map { case (key, value) => value }.head.toString.toDouble)
  }

  def getLongOptValue(map: Map[String, Any], columnName: String): Option[Long] = {
    Some(map
      .filter { case (key, value) => key.equals(columnName) }
      .map { case (key, value) => value }.head.toString.toLong)
  }

  def getIntOptValue(map: Map[String, Any], columnName: String): Option[Int] = {
    Some(map
      .filter { case (key, value) => key.equals(columnName) }
      .map { case (key, value) => value }.head.toString.toInt)
  }
}

abstract class ClientSpec {
  def CustomerId: Int
}
