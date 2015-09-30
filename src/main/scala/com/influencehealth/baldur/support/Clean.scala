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
    case (fieldName, "icdDiag", codeType: Int) =>
      try {
        Clean.icdDiag(fieldValue, codeType)
      } catch {
        case err: Throwable => throw new Error(f"$fieldName is not an icd-Diag code (code, codeType): ($fieldValue,$codeType)",err)
      }
    case (fieldName, "icdProc", codeType: Int) =>
      try {
        Clean.icdProc(fieldValue, codeType)
      } catch {
        case err: Throwable => throw new Error(f"$fieldName is not an icd-Proc code (code, codeType): ($fieldValue,$codeType)",err)
      }
    case (fieldName, "cpt") =>
      try {
        Clean.cpt(fieldValue)
      } catch {
        case err: Throwable => throw new Error(f"$fieldName is not a cpt code: $fieldValue",err)
      }
    case (fieldName, "msDrg") =>
      try {
        Clean.msDrg(fieldValue)
      } catch {
        case err: Throwable => throw new Error(f"$fieldName is not an msDrg code: $fieldValue",err)
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
    
    if (cleansed.matches("(?i)None"))
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

  // Cleansing routine created against the following documentation: https://www.unitypoint.org/waterloo/filesimages/For%20Providers/ICD9-ICD10-Differences.pdf
  def icdDiag(x: String, codeType: Int): Any = {
    val y = string(x)

    // ICD9 codes can only be 5 digits. ICD10 can be 7
    val (minLength, maxLength) = codeType match {
      case 9 => (3, 5) // must be 3-5 characters for icd-9
      case 10 => (3, 7) // must be 3-7 characters for icd-10
    }

    y match {
      case "" => None
      case _ =>
        // Strip decimal
        val cleansed = y.replace(".","")

        val validCleansed = codeType match {
          // Between min/max length, first digit can be alphanumeric, last digits must be numeric
          case 9 => cleansed.length >= minLength && cleansed.length <= maxLength && cleansed.head.isLetterOrDigit && cleansed.drop(1).forall(_.isDigit)

          // Between min/max length, first digit can be alpha, 2nd and 3rd digit must be numeric, last digits must be alphanumeric
          case 10 => cleansed.length >= minLength && cleansed.length <= maxLength && cleansed.head.isLetter && cleansed.slice(1,2).forall(_.isDigit) && cleansed.drop(3).forall(_.isLetterOrDigit)
        }

        validCleansed match {
          case false => None
          case true => cleansed
        }

    }
  }

  // Cleansing routine created against the following documentation: https://www.unitypoint.org/waterloo/filesimages/For%20Providers/ICD9-ICD10-Differences.pdf
  def icdProc(x: String, codeType: Int): Any = {
    val y = string(x)

    // ICD9 codes can only be 5 digits. ICD10 can be 7
    val (minLength, maxLength) = codeType match {
      case 9 => (3, 4) // must be 3-4 characters for icd-9
      case 10 => (7, 7) // must be 7 characters for icd-10
    }

    y match {
      case "" => None
      case _ =>
        // Strip decimal
        val cleansed = y.replace(".","")

        val validCleansed = codeType match {
          // Between min/max length, all must be numeric
          case 9 => cleansed.length >= minLength && cleansed.length <= maxLength && cleansed.forall(_.isDigit)

          // Between min/max length, all must be alphanumeric
          case 10 => cleansed.length >= minLength && cleansed.length <= maxLength && cleansed.forall(_.isLetterOrDigit)
        }

        validCleansed match {
          case false => None
          case true => cleansed
        }

    }
  }

  def cpt(x: String): Any = {
    val y = string(x)

    y match {
      case "" => None
      case _ =>
        // Strip decimal, take the first 5 characters
        val cleansed = y.replace(".","").take(5)

        // Check if all characters are alphanumeric and length is 5, return none if not
        val validCleansed = cleansed.length.equals(5) && cleansed.forall(_.isLetterOrDigit)

        validCleansed match {
          case false => None
          case true => cleansed
        }
    }
  }

  def msDrg(x: String): Any = {
    val y = string(x)

    y match {
      case "" => None
      case _ =>
        // Strip decimal
        val cleansed = y.replace(".","")

        // Length can be up to 3 characters, all numbers
        val validCleansed = cleansed.length <= 3 && cleansed.forall(_.isDigit)

        validCleansed match {
          case false => None
          case true => cleansed
        }
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
