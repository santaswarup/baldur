import java.text.SimpleDateFormat
import java.util.Date

/**
 * Cleanser
 */
object Clean {
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

  def date(x: String, format: String="dd/MM/yyyy"): Date = {
    new SimpleDateFormat(format).parse(x)
  }
}
