package com.influencehealth.baldur.support

import java.util.UUID
import org.joda.time.{PeriodType, Period, DateTime}

/**
 * Defines the contract for input metadata defining implementors.
 */
object FileInputSupport {

  def getAgeDob(map: Map[String, Any], ageColumn: String, dobColumn: String): (Option[DateTime], Option[Int], Option[String]) = {
    val dobMap =
      map
        .filter { case (key, value) => key.equals(dobColumn) }
        .map { case (key, value) => value match{
        case value: DateTime => Some(value)
        case _ => None
      }}

    val ageMap =
      map
        .filter { case (key, value) => key.equals(ageColumn) }
        .map { case (key, value) => value match{
        case value: Int => Some(value)
        case _ => None
      }}

    val ageRaw: Option[Int] = ageMap.nonEmpty match {
      case false => None
      case true => ageMap.head
    }

    val dob: Option[DateTime] = dobMap.nonEmpty match {
      case false => None
      case true => dobMap.head
    }

    val ageCalculated: Option[Int] = dob.nonEmpty match {
      case false => ageRaw
      case true => Some(new Period((dob, DateTime.now())).getYears())
    }

    val ageGroup = ageCalculated.nonEmpty match {
      case false => None
      case true => ageCalculated.get match{
        case x if x <= 4 => Some("A")
        case x if x <= 10 => Some("B")
        case x if x <= 14 => Some("C")
        case x if x <= 17 => Some("D")
        case x if x <= 20 => Some("E")
        case x if x <= 24 => Some("F")
        case x if x <= 29 => Some("G")
        case x if x <= 34 => Some("H")
        case x if x <= 39 => Some("I")
        case x if x <= 44 => Some("J")
        case x if x <= 49 => Some("K")
        case x if x <= 54 => Some("L")
        case x if x <= 59 => Some("M")
        case x if x <= 61 => Some("N")
        case x if x <= 64 => Some("O")
        case x if x <= 66 => Some("P")
        case x if x <= 69 => Some("Q")
        case x if x <= 74 => Some("R")
        case x if x <= 79 => Some("S")
        case x if x <= 84 => Some("T")
        case x if x <= 99 => Some("U")
        case x if x >= 100 => Some("V")
        case _ => Some("U")
      }
    }

    (dob, ageCalculated, ageGroup)
  }

  def getAddressStringValue(map: Map[String, Any], columnName: String, validAddressFlag: Option[Boolean]): Option[String] = {
    validAddressFlag.isDefined match{
      case false => None
      case true => validAddressFlag.get match{
        case true => getStringOptValue(map, columnName)
        case _ => None
      }
    }
  }

  def getAddressFloatValue(map: Map[String, Any], columnName: String, validAddressFlag: Option[Boolean]): Option[Float] = {
    validAddressFlag.isDefined match{
      case false => None
      case true => validAddressFlag.get match{
        case true => getFloatOptValue(map, columnName)
        case _ => None
      }
    }
  }

  def getValidAddressFlag(ncoaActionCode: Option[String]): Option[Boolean] = {
    ncoaActionCode.isDefined match{
      case true => ncoaActionCode.get match{
        case "B" => Some(false)
        case "C" => Some(true)
        case "P" => Some(false)
        case "Y" => Some(false)
        case "F" => Some(false)
        case "Z" => Some(false)
        case "G" => Some(false)
        case "I" => Some(true)
        case "M" => Some(true)
        case "O" => Some(true)
        case _ => Some(false)
      }
      case false => None
    }
  }

  def containsHyphen(str: Option[String]): Boolean = {
    str match {
      case None => false
      case Some(x) => Some(x).get.contains("-")
    }
  }

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

  def getMedicalCodeString(map: Map[String, Any], columnName: String, codeType: String, delimiter: String = ","): Option[String] = {
    val newMap =
      map
        .filter { case (key, value) => key.equals(columnName) }
        .map{case (key, value) => mapMedicalCode(value, codeType, delimiter)}

    newMap.nonEmpty match {
      case false => None
      case true => newMap.head
    }
  }

  def mapMedicalCode(value: Any, codeType: String, delimiter: String): Option[String] = {
    value match {
      case None => None
      case "" => None
      case _ => Some(value.toString.replace(delimiter, ";" + getCodeType(codeType) + ",") + ";" + getCodeType(codeType))
    }
  }

  def getStringValue(map: Map[String, Any], columnName: String): String = {
    map
      .filter { case (key, value) => key.equals(columnName) }
      .map { case (key, value) => value match{
      case value: String => value
      case _ => value.toString
    }}.head
  }

  def getUUIDValue(map: Map[String, Any], columnName: String): UUID = {
    map
      .filter { case (key, value) => key.equals(columnName) }
      .map { case (key, value) => value match{
      case value: String => UUID.fromString(value)
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

  def getBoolValue(map: Map[String, Any], columnName: String): Boolean = {
      map
        .filter { case (key, value) => key.equals(columnName) }
        .map { case (key, value) => value match{
        case value: Boolean => value
      }}.head
  }

  def getStringOptValue(map: Map[String, Any], columnName: String): Option[String] = {
    val newMap =
      map
        .filter { case (key, value) => key.equals(columnName) }
        .map { case (key, value) => value match{
        case "" => None
        case None => None
        case value: String => Some(value)
        case _ => Some(value.toString)
      }}

    newMap.nonEmpty match {
      case false => None
      case true => newMap.head
    }
  }

  def getUUIDOptValue(map: Map[String, Any], columnName: String): Option[UUID] = {
    val newMap =
      map
        .filter { case (key, value) => key.equals(columnName) }
        .map { case (key, value) => value match{
        case "" => None
        case value: String => Some(UUID.fromString(value))
        case _ => None
      }}

    newMap.nonEmpty match {
      case false => None
      case true => newMap.head
    }
  }

  def getDateOptValue(map: Map[String, Any], columnName: String): Option[DateTime] = {
    val newMap =
      map
        .filter { case (key, value) => key.equals(columnName) }
        .map { case (key, value) => value match{
        case value: DateTime => Some(value)
        case _ => None
      }}

    newMap.nonEmpty match {
      case false => None
      case true => newMap.head
    }
  }

  def getFloatOptValue(map: Map[String, Any], columnName: String): Option[Float] = {
    val newMap =
      map
        .filter { case (key, value) => key.equals(columnName) }
        .map { case (key, value) => value match{
        case value: Float => Some(value)
        case _ => None
      }}

    newMap.nonEmpty match {
      case false => None
      case true => newMap.head
    }
  }

  def getBoolOptValue(map: Map[String, Any], columnName: String): Option[Boolean] = {
    val newMap =
      map
        .filter { case (key, value) => key.equals(columnName) }
        .map { case (key, value) => value match{
        case value: Boolean => Some(value)
        case _ => None
      }}

    newMap.nonEmpty match {
      case false => None
      case true => newMap.head
    }
  }

  def getDoubleOptValue(map: Map[String, Any], columnName: String): Option[Double] = {
    val newMap =
      map
        .filter { case (key, value) => key.equals(columnName) }
        .map { case (key, value) => value match{
        case value: Double => Some(value)
        case _ => None
      }}

    newMap.nonEmpty match {
      case false => None
      case true => newMap.head
    }
  }

  def getLongOptValue(map: Map[String, Any], columnName: String): Option[Long] = {
    val newMap =
      map
        .filter { case (key, value) => key.equals(columnName) }
        .map { case (key, value) => value match{
        case value: Long => Some(value)
        case _ => None
      }}

    newMap.nonEmpty match {
      case false => None
      case true => newMap.head
    }
  }

  def getIntOptValue(map: Map[String, Any], columnName: String): Option[Int] = {
    val newMap =
      map
        .filter { case (key, value) => key.equals(columnName) }
        .map { case (key, value) => value match{
        case value: Int => Some(value)
        case _ => None
      }}

    newMap.nonEmpty match {
      case false => None
      case true => newMap.head
    }
  }

  def getSetOptValue(map: Map[String, Any], columnName: String, delimiter: String = ","): Option[Set[String]] = {
    val newMap =
      map
        .filter { case (key, value) => key.equals(columnName) }
        .map { case (key, value) => value match{
        case "" => None
        case value: String => Some(value.split(delimiter).toSet)
        case _ => None
      }}

    newMap.nonEmpty match {
      case false => None
      case true => newMap.head
    }
  }

  def getIntSetOptValue(map: Map[String, Any], columnName: String, delimiter: String = ","): Option[Set[Int]] = {
    val newMap =
      map
        .filter { case (key, value) => key.equals(columnName) }
        .map { case (key, value) => value match{
        case "" => None
        case value: String => Some(value.split(delimiter).map(value => value.toInt).toSet)
        case _ => None
      }}

    newMap.nonEmpty match {
      case false => None
      case true => newMap.head
    }
  }

  def getListOptValue(map: Map[String, Any], columnName: String, delimiter: String = ","): Option[List[String]] = {
    val newMap =
      map
        .filter { case (key, value) => key.equals(columnName) }
        .map { case (key, value) => value match{
        case "" => None
        case value: String => Some(value.split(delimiter).toList)
        case _ => None
      }}

    newMap.nonEmpty match {
      case false => None
      case true => newMap.head
    }
  }
}


