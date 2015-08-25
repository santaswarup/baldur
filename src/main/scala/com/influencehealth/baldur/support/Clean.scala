package com.influencehealth.baldur.support

import java.util.regex.Pattern

import org.joda.time.DateTime
import org.joda.time.format._

/**
 * Cleanser
 */
object Clean {
  private val byTypeFn = (fieldValue: String, fieldMeta: Product) => fieldMeta match {
    case (fieldName, "title") =>
      try {
        Clean.titleCase(fieldValue)
      } catch {
        case err: Throwable => throw new Error(f"$fieldName is not handled properly when converting to tile case: $fieldValue",err)
      }
    case (fieldName, "string") =>
      try {
        Clean.string(fieldValue)
      } catch {
        case err: Throwable => throw new Error(f"$fieldName is not handled properly like a String: $fieldValue",err)
      }
    case (fieldName, "int") =>
      try {
        Clean.int(fieldValue)
      } catch {
        case err: Throwable => throw new Error(f"$fieldName is not an integer: $fieldValue",err)
      }
    case (fieldName, "boolean") =>
      try {
        Clean.bool(fieldValue)
      } catch {
        case err: Throwable => throw new Error(f"$fieldName is not an boolean: $fieldValue",err)
      }
    case (fieldName, "date") =>
      try {
      Clean.date(fieldValue)
      } catch {
        case err: Throwable => throw new Error(f"Could not parse date: $fieldName. Bad value: $fieldValue", err)
      }
    case (fieldName, "date", format: String) =>
      try {
        Clean.date(fieldValue, Some(format))
      } catch {
        case err: Throwable => throw new Error(f"Could not parse date: $fieldName. Bad value: $fieldValue format $format",
          err)
      }
    case (_, "float") =>
      Clean.float(fieldValue)
    case (_, "long") =>
      Clean.long(fieldValue)
    case (_, "skip") =>
      fieldValue
    case _ =>
      throw new Error("Metadata not understood")
  }

  def byType = byTypeFn.tupled

  def string(x: String): String = {
    val cleansed = x.replace("\uFFFD", " ").trim().replaceAll(" +", " ")

    if (cleansed.matches("(?i)null"))
      return ""

    cleansed
  }

  def titleCase(x: String): String = {
    val y = string(x)

    val words = y.split(" ")

    val regexPattern = Pattern.compile("[A-Z]{2,}")


    words.map{
      case word =>
        regexPattern.matcher(word).matches match{
          case true => word.toLowerCase.capitalize
          case false => word.capitalize
        }
    }.mkString(" ")
  }

  def float(x: String): Any = {
    val y = string(x)

    y match {
       case "" => None
       case _ => y.toFloat
     }
  }

  def long(x: String): Any = {
    val y = string(x)

    y match {
      case "" => None
      case _ => y.toLong
    }
  }

  def int(x: String): Any = {
    val y = string(x)

    y match {
      case "" => None
      case _ => y.toInt
    }
  }

  def bool(x: String): Any = {
    val y = string(x)

    y match {
      case "" => None
      case _ => y.toBoolean
    }
  }

  def date(x: String, format: Option[String]=None): Any = {
    val y = string(x)

    val formatter = format match {
      case None =>
        dateTimeFormatter
      case _ =>
        new DateTimeFormatterBuilder().append(null,
          Array(DateTimeFormat.forPattern(format.get).getParser, dateTimeFormatter.getParser)).toFormatter
    }

    y match {
      case "" => None
      case _ => DateTime.parse(y, formatter)
    }

  }

  val dateParsers = Array(DateTimeFormat.forPattern("yyyy/MM/dd").getParser,
    DateTimeFormat.forPattern("yyyyMMdd").getParser,
    DateTimeFormat.forPattern("yyyy-MM-dd").getParser,
    DateTimeFormat.forPattern("MM/dd/yyyy").getParser,
    DateTimeFormat.forPattern("MM-dd-yyyy").getParser,
    DateTimeFormat.forPattern("yyyy-MM-dd H:mm:ss").getParser,
    DateTimeFormat.forPattern("yyyy/MM/dd H:mm:ss").getParser,
    ISODateTimeFormat.dateOptionalTimeParser().getParser,
    ISODateTimeFormat.dateHour().getParser)

  val dateTimeFormatter = new DateTimeFormatterBuilder().append(null, dateParsers).toFormatter

}
