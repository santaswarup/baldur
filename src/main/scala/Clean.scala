import org.joda.time.DateTime
import org.joda.time.format._

/**
 * Cleanser
 */
object Clean {
  private val byTypeFn = (fieldValue: String, fieldMeta: Product) => fieldMeta match {
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
    case (_, "skip") =>
      fieldValue
    case _ =>
      throw new Error("Metadata not understood")
  }

  def byType = byTypeFn.tupled

  def string(x: String): Option[String] = {

    val cleansed = x.replace("\uFFFD", " ").trim().replaceAll(" +", " ")

    if (cleansed.matches("(?i)null"))
      return None

    cleansed match{
      case "" => None
      case _ => Some(cleansed)
    }
  }

  def float(x: String): Option[Float] = {
    val y = string(x)

    y match {
       case None => None
       case _ => Some(y.get.toFloat)
     }
  }

  def int(x: String): Any = {
    val y = string(x)

    y match {
      case None => None
      case _ => Some(y.get.toInt)
    }
  }

  def date(x: String, format: Option[String]=None): Option[DateTime] = {
    val y = string(x)

    val formatter = format match {
      case None => dateTimeFormatter
      case _ => new DateTimeFormatterBuilder().append(null, Array(DateTimeFormat.forPattern(format.get).getParser, dateTimeFormatter.getParser)).toFormatter
    }

    y match {
      case None => None
      case _ => Some(DateTime.parse(y.get, formatter))
    }

  }

  val dateParsers = Array(DateTimeFormat.forPattern("yyyy/MM/dd").getParser,
    DateTimeFormat.forPattern("yyyy-mm-dd").getParser,
    DateTimeFormat.forPattern("MM/dd/yyyy").getParser,
    DateTimeFormat.forPattern("MM-dd-yyyy").getParser,
    DateTimeFormat.forPattern("yyyy-MM-dd H:mm:ss").getParser,
    DateTimeFormat.forPattern("yyyy/MM/dd H:mm:ss").getParser,
    ISODateTimeFormat.dateOptionalTimeParser().getParser,
    ISODateTimeFormat.dateHour().getParser)

  val dateTimeFormatter = new DateTimeFormatterBuilder().append(null, dateParsers).toFormatter

}
