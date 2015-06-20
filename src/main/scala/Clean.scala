import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
 * Cleanser
 */
object Clean {
  private val byTypeFn = (fieldValue: String, fieldMeta: Product) => fieldMeta match {
    case (_, "string") =>
      Clean.string(fieldValue)
    case (_, "int") =>
      Clean.int(fieldValue)
    case (_, "date") =>
      Clean.date(fieldValue)
    case (_, "date", format: String) =>
      Clean.date(fieldValue, format)
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

  def float(x: String): Float = {
    x.toFloat
  }

  def int(x: String): Int = {
    x.toInt
  }

  def date(x: String, format: String="dd/MM/yyyy"): DateTime = {
    DateTime.parse(x, DateTimeFormat.forPattern(format))
  }
}
