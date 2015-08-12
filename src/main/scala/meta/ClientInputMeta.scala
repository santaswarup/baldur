package meta

import org.joda.time.DateTime

/**
 * Defines the contract for input metadata defining implementors.
 */
trait ClientInputMeta extends ClientSpec {
  def originalFields(): Seq[scala.Product]
  def mapping(map: Map[String, Any]): ActivityOutput

  var delimiter: String = "\t"
  var codeTypes: Seq[(Int, String)] = Seq(
    (11, "cpt"),
    (31, "icd9_diag"),
    (41, "icd9_proc"),
    (51, "ms_drg")
    )

  def getCodeType(codeType: String): String = {
    codeTypes
      .filter{case (id, desc) => desc.equals(codeType)}
      .map{case(id, desc) => id}
      .head
      .toString
  }

  // Standard helper functions, used to assign cleansed versions of an input field to the
  // ActivityOutput class
  def getStringValue(map: Map[String, Any], columnName: String): String = {
    map
      .filter { case (key, value) => key.equals(columnName) }
      .map { case (key, value) => value match{
      case value: String => value
    }}.head
  }

  def getDateValue(map: Map[String, Any], columnName: String): DateTime = {
    map
      .filter { case (key, value) => key.equals(columnName) }
      .map { case (key, value) => value match{
      case value: DateTime => value
    }}.head
  }

  def getFloatValue(map: Map[String, Any], columnName: String): Float = {
    map
      .filter { case (key, value) => key.equals(columnName) }
      .map { case (key, value) => value match{
      case value: Float => value
    }}.head
  }

  def getDoubleValue(map: Map[String, Any], columnName: String): Double = {
    map
      .filter { case (key, value) => key.equals(columnName) }
      .map { case (key, value) => value match{
      case value: Double => value
    }}.head
  }

  def getLongValue(map: Map[String, Any], columnName: String): Long = {
    map
      .filter { case (key, value) => key.equals(columnName) }
      .map { case (key, value) => value match{
      case value: Long => value
    }}.head
  }

  def getIntValue(map: Map[String, Any], columnName: String): Int = {
    map
      .filter { case (key, value) => key.equals(columnName) }
      .map { case (key, value) => value match{
      case value: Int => value
    }}.head
  }

  def getSetValue(map: Map[String, Any], columnName: String, delimiter: String = ","): Set[String] = {
    map
      .filter { case (key, value) => key.equals(columnName) }
      .map { case (key, value) => value match{
      case value: String => value.split(delimiter).toSet
    }}.head
  }

  def getListValue(map: Map[String, Any], columnName: String, delimiter: String = ","): List[String] = {
    map
      .filter { case (key, value) => key.equals(columnName) }
      .map { case (key, value) => value match{
        case value: String => value.split(delimiter).toList
    }}.head
  }

  def getStringOptValue(map: Map[String, Any], columnName: String): Option[String] = {
    map
      .filter { case (key, value) => key.equals(columnName) }
      .map { case (key, value) => value match{
      case value: String => Some(value)
      case None => None
    }}.head
  }

  def getDateOptValue(map: Map[String, Any], columnName: String): Option[DateTime] = {
    map
      .filter { case (key, value) => key.equals(columnName) }
      .map { case (key, value) => value match{
      case value: DateTime => Some(value)
      case None => None
    }}.head
  }

  def getFloatOptValue(map: Map[String, Any], columnName: String): Option[Float] = {
    map
      .filter { case (key, value) => key.equals(columnName) }
      .map { case (key, value) => value match{
      case value: Float => Some(value)
      case None => None
    }}.head
  }

  def getDoubleOptValue(map: Map[String, Any], columnName: String): Option[Double] = {
    map
      .filter { case (key, value) => key.equals(columnName) }
      .map { case (key, value) => value match{
      case value: Double => Some(value)
      case None => None
    }}.head
  }

  def getLongOptValue(map: Map[String, Any], columnName: String): Option[Long] = {
    map
      .filter { case (key, value) => key.equals(columnName) }
      .map { case (key, value) => value match{
      case value: Long => Some(value)
      case None => None
    }}.head
  }

  def getIntOptValue(map: Map[String, Any], columnName: String): Option[Int] = {
    map
      .filter { case (key, value) => key.equals(columnName) }
      .map { case (key, value) => value match{
      case value: Int => Some(value)
      case None => None
    }}.head
  }

  def getSetOptValue(map: Map[String, Any], columnName: String, delimiter: String = ","): Option[Set[String]] = {
    map
      .filter { case (key, value) => key.equals(columnName) }
      .map { case (key, value) => value match{
        case value: String => Some(value.split(delimiter).toSet)
        case None => None
    }}.head
  }

  def getListOptValue(map: Map[String, Any], columnName: String, delimiter: String = ","): Option[List[String]] = {
    map
      .filter { case (key, value) => key.equals(columnName) }
      .map { case (key, value) => value match{
      case value: String => Some(value.split(delimiter).toList)
      case None => None
    }}.head
  }

  def getMedicalCodeString(map: Map[String, Any], columnName: String, codeType: String, delimiter: String = ","): Option[String] = {
    map
      .filter { case (key, value) => key.equals(columnName) }
      .map{case x => mapMedicalCode(x, codeType, delimiter)}
      .head
  }

  def mapMedicalCode(value: Any, codeType: String, delimiter: String): Option[String] = {
    value match {
      case None => None
      case value: String => Some(value.replace(delimiter, ";" + getCodeType(codeType) + ",") + ";" + getCodeType(codeType))
    }
  }

}

abstract class ClientSpec {
  def CustomerId: Int
}
