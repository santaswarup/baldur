package com.influencehealth.baldur.identity_load.meta.experian

import scala.io.Source

/**
 * Experian constants
 */
object ExperianConstants {

  val beehiveToFinancialClass: Map[Int, (Option[Int], Option[String], Option[String])] =
    Map(1 ->(Some(9910), Some("TARGET PAYER - PROFITABLE"), Some("PI")),
        2 ->(Some(9910), Some("TARGET PAYER - PROFITABLE"), Some("PI")),
        3 ->(Some(9910), Some("TARGET PAYER - PROFITABLE"), Some("PI")),
        4 ->(Some(9910), Some("TARGET PAYER - PROFITABLE"), Some("PI")),
        5 ->(Some(9910), Some("TARGET PAYER - PROFITABLE"), Some("PI")),
        6 ->(Some(9910), Some("TARGET PAYER - PROFITABLE"), Some("PI")),
        7 ->(Some(9910), Some("TARGET PAYER - PROFITABLE"), Some("PI")),
        8 ->(Some(9910), Some("TARGET PAYER - PROFITABLE"), Some("PI")),
        9 ->(Some(9910), Some("TARGET PAYER - PROFITABLE"), Some("PI")),
        10 ->(Some(9910), Some("TARGET PAYER - PROFITABLE"), Some("PI")),
        11 ->(Some(9910), Some("TARGET PAYER - PROFITABLE"), Some("PI")),
        12 ->(Some(9920), Some("TARGET PAYER - UNPROFITABLE"), Some("NI")),
        13 ->(Some(9920), Some("TARGET PAYER - UNPROFITABLE"), Some("NI")),
        14 ->(Some(9910), Some("TARGET PAYER - PROFITABLE"), Some("PI")),
        15 ->(Some(9910), Some("TARGET PAYER - PROFITABLE"), Some("PI")),
        16 ->(Some(9910), Some("TARGET PAYER - PROFITABLE"), Some("PI")),
        17 ->(Some(9920), Some("TARGET PAYER - UNPROFITABLE"), Some("NI")),
        18 ->(Some(9920), Some("TARGET PAYER - UNPROFITABLE"), Some("NI")),
        19 ->(Some(9920), Some("TARGET PAYER - UNPROFITABLE"), Some("NI")),
        20 ->(Some(9920), Some("TARGET PAYER - UNPROFITABLE"), Some("NI")),
        21 ->(Some(9920), Some("TARGET PAYER - UNPROFITABLE"), Some("NI")),
        22 ->(Some(9920), Some("TARGET PAYER - UNPROFITABLE"), Some("NI")),
        23 ->(Some(9920), Some("TARGET PAYER - UNPROFITABLE"), Some("NI")),
        24 ->(Some(9920), Some("TARGET PAYER - UNPROFITABLE"), Some("NI")),
        25 ->(Some(9920), Some("TARGET PAYER - UNPROFITABLE"), Some("NI")),
        26 ->(Some(9920), Some("TARGET PAYER - UNPROFITABLE"), Some("NI")),
        27 ->(Some(9920), Some("TARGET PAYER - UNPROFITABLE"), Some("NI")),
        28 ->(Some(9920), Some("TARGET PAYER - UNPROFITABLE"), Some("NI")),
        29 ->(Some(9920), Some("TARGET PAYER - UNPROFITABLE"), Some("NI")),
        30 ->(Some(9920), Some("TARGET PAYER - UNPROFITABLE"), Some("NI"))
    )

  val presenceOfChildCodes: Map[String, String] =
    Map("Y" -> "45",
        "N" -> "44",
        "U" -> "44"
    )

  val incomeCodes: Map[String, String] =
    Map("A" -> "08",
        "B" -> "09",
        "C" -> "10",
        "D" -> "11",
        "E" -> "12",
        "F" -> "13",
        "G" -> "14",
        "H" -> "15",
        "I" -> "16",
        "J" -> "17",
        "K" -> "18",
        "L" -> "19",
        "U" -> "20"
    )

  val maritalStatusCodes: Map[String, String] =
    Map("S" -> "07",
        "M" -> "06",
        "U" -> "07")

  val educationCodes: Map[Int, String] =
    Map(3 -> "01",
        4 -> "02",
        1 -> "03",
        5 -> "04",
        2 -> "05"
    )


  val occupationCodes: Map[Int, String] =
    Map(0 -> "43",
        1 -> "37",
        2 -> "37",
        3 -> "37",
        4 -> "38",
        5 -> "38",
        6 -> "40",
        7 -> "39",
        8 -> "41",
        9 -> "42")

  val ageCodes: Map[Int, String] =
    Source.fromURL(getClass.getResource("/beehive_age_cw.csv"))
      .getLines()
      .map(line => line.split(","))
      .map { case line => (line(0).toInt, line(1))}
      .toMap

  val zipToClient: Map[Int, String] =
    Source.fromURL(getClass.getResource("/client_experian_zips.csv"))
      .getLines()
      .map(line => line.split(","))
      .map { case line => (line(0).toInt, line(1))}
      .toMap

  val lastNameCodes: Map[String, String] =
    Source.fromURL(getClass.getResource("/beehive_ethnic_cw.csv"))
      .getLines()
      .map(line => line.split(","))
      .map { case line => (line(0), line(1))}
      .toMap

  val combinedToCluster: Map[String, Int] =
    Source.fromURL(getClass.getResource("/beehive_combined_code_cw.csv"))
      .getLines()
      .map(line => line.split(","))
      .map { case line => (line(0), line(1).toInt)}
      .toMap
}