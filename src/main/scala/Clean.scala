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
        case err: Throwable => throw new Error(f"${fieldName} is not handled properly like a String: ${fieldValue}",err)
      }
    case (fieldName, "int") =>
      try {
        Clean.int(fieldValue)
      } catch {
        case err: Throwable => throw new Error(f"${fieldName} is not an integer: ${fieldValue}",err)
      }
    case (fieldName, "date") =>
      try {
      Clean.date(fieldValue)
      } catch {
        case err: Throwable => throw new Error(f"Could not parse date: ${fieldName}. Bad value: ${fieldValue}", err)
      }
    case (fieldName, "date", format: String) =>
      try {
        Clean.date(fieldValue, Some(format))
      } catch {
        case err: Throwable => throw new Error(f"Could not parse date: ${fieldName}. Bad value: ${fieldValue} format ${format}",
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

  def string(x: String): String = {
    val cleansed = x.replace("\uFFFD", " ").trim().replaceAll(" +", " ")

    if (cleansed.matches("(?i)null"))
      return ""

    cleansed
  }

  def float(x: String): Any = {
    val y = string(x)

    y match {
       case "" => None
       case _ => y.toFloat
     }
  }

  def int(x: String): Any = {
    val y = string(x)

    y match {
      case "" => None
      case _ => y.toInt
    }
  }

  def date(x: String, format: Option[String]=None): Any = {
    val y = string(x)

    val formatter = format match {
      case Some(format) =>
        new DateTimeFormatterBuilder().append(null,
          Array(DateTimeFormat.forPattern(format).getParser(), dateTimeFormatter.getParser())).toFormatter()
      case None =>
        dateTimeFormatter
    }

    y match {
      case "" => None
      case _ => DateTime.parse(y, formatter)
    }

  }

  val dateParsers = Array(DateTimeFormat.forPattern("yyyy/MM/dd").getParser(),
    DateTimeFormat.forPattern("yyyy-mm-dd").getParser(),
    DateTimeFormat.forPattern("MM/dd/yyyy").getParser(),
    DateTimeFormat.forPattern("MM-dd-yyyy").getParser(),
    DateTimeFormat.forPattern("yyyy-MM-dd H:mm:ss").getParser(),
    DateTimeFormat.forPattern("yyyy/MM/dd H:mm:ss").getParser(),
    ISODateTimeFormat.dateOptionalTimeParser().getParser(),
    ISODateTimeFormat.dateHour().getParser())

  val dateTimeFormatter = new DateTimeFormatterBuilder().append(null, dateParsers).toFormatter()

}
