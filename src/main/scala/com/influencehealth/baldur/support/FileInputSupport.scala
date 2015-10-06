package com.influencehealth.baldur.support

import java.util.UUID
import org.joda.time.{PeriodType, Period, DateTime}

import scala.io.Source

/**
 * Defines the contract for input metadata defining implementors.
 */
object FileInputSupport {

  // County -> (CBSA, Type)
  val countyToCbsa: Map[String, (String, String)] =
    Source.fromURL(getClass.getResource("/county_cbsa_cw.csv"))
      .getLines()
      .drop(1)
      .map(line => line.split(","))
      .map { case line => (line(0), (line(1), line(2)))}
      .toMap

  val codeGroups: Map[Int, Set[(String, Int)]] =
    Source.fromURL(getClass.getResource("/code_groups.txt"))
      .getLines()
      .drop(1)
      .map(line => line.split("\\|"))
      .map{ case line => (line(0).toInt, (line(1).toInt, line(2))) }
      .toSeq
      .groupBy{ case (a,b) => a }
      .map{
        case (codeGroup, sequence) =>
          val newSet = sequence.map{ case (x, (codeType, code)) => (code.toLowerCase, codeType) }.toSet
          (codeGroup, newSet)  }


  val diagnosisServiceLines: Map[(String, Int), (Int, String, Int, String, String)] =
    Source.fromURL(getClass.getResource("/diagnosis_service_lines.txt"))
      .getLines()
      .drop(1)
      .map(line => line.split("\\|"))
      .map{
        case line =>
          val list = line.toList
          list.isDefinedAt(6) match {
            case true => ((line(0),line(1).toInt),(line(2).toInt,line(3),line(4).toInt,line(5),line(6)))
            case false => ((line(0),line(1).toInt),(line(2).toInt,line(3),line(4).toInt,line(5),""))
          } }
      .toMap

  val procedureServiceLines: Map[(String, Int), (Int, String, Int, String, Int, String, String)] =
    Source.fromURL(getClass.getResource("/procedure_service_lines.txt"))
      .getLines()
      .drop(1)
      .map(line => line.split("\\|"))
      .map {
        case line =>
          val list = line.toList
          list.isDefinedAt(8) match {
            case true => ((line(0), line(1).toInt), (line(2).toInt, line(3), line(4).toInt, line(5), line(6).toInt, line(7), line(8)))
            case false => ((line(0), line(1).toInt), (line(2).toInt, line(3), line(4).toInt, line(5), line(6).toInt, line(7), ""))
          } }
      .toMap

  val drgServiceLines: Map[String, (Int, String, Int, String, Int, String, String)] =
    Source.fromURL(getClass.getResource("/ms_drg_service_lines.txt"))
      .getLines()
      .drop(1)
      .map(line => line.split("\\|"))
      .map{
        case line =>
          val list = line.toList
          list.isDefinedAt(7) match {
            case true => (line(0),(line(1).toInt,line(2),line(3).toInt,line(4),line(5).toInt, line(6), line(7)))
            case false => (line(0),(line(1).toInt,line(2),line(3).toInt,line(4),line(5).toInt, line(6), ""))
        } }
      .toMap

  val primaryServiceLines: Map[(Int, Int), Int] =
    Source.fromURL(getClass.getResource("/primary_service_lines.txt"))
      .getLines()
      .drop(1)
      .map(line => line.split("\\|"))
      .map{ case line => ((line(0).toInt, line(1).toInt),line(3).toInt)}
      .toMap

  def getServiceLines(codes: Option[List[String]], sex: Option[String]): Option[Set[String]] = {

    val serviceLines: Option[Set[String]] = codes.isEmpty match {
      case true => None
      case false =>
        val codesSplit: List[(String, Int)] =
          codes.get.map{ case codeAndType =>
            val split = codeAndType.split(";")
            (split(0).toLowerCase, split(1).toInt) }

        mapServiceLines(codesSplit, sex)
    }

    serviceLines

  }

  def mapServiceLines(codes: List[(String, Int)], sex: Option[String]): Option[Set[String]] = {

    val diagCodes = codes.filter{ case (code, codeType) => codeType.equals(31) || codeType.equals(32) }
    val procCodes = codes.filter{ case (code, codeType) => codeType.equals(41) || codeType.equals(42) }.take(5)
    val cptCodes = codes.filter{ case (code, codeType) => codeType.equals(11) }.take(5)
    val msDrg = codes.filter{ case (code, codeType) => codeType.equals(51) }

    val msDrgServiceLines: Option[String] = getMsDrgServiceLines(msDrg, sex)
    val diagServiceLines: Option[String] = getDiagServiceLines(diagCodes, sex)
    val procServiceLines: Option[String] = getProcServiceLines(procCodes, cptCodes, sex, 5)
    val primaryServiceLine: Option[String] = getPrimaryServiceLine(msDrgServiceLines, procServiceLines)

    serviceLinesToSet(msDrgServiceLines, diagServiceLines, procServiceLines, primaryServiceLine)

  }

  def serviceLinesToSet(msDrgServiceLines: Option[String], diagServiceLines: Option[String], procServiceLines: Option[String], primaryServiceLine: Option[String]): Option[Set[String]] = {
    val concatenated: Set[String] =
      serviceLineStringToSet(primaryServiceLine) ++
        serviceLineStringToSet(msDrgServiceLines) ++
        serviceLineStringToSet(procServiceLines) ++
        serviceLineStringToSet(diagServiceLines)
        .mkString(",")
        .split(",")

    concatenated.isEmpty match {
      case true => None
      case false => Some(concatenated)
    }
  }

  def serviceLineStringToSet(strOpt: Option[String]): Set[String] = {
    strOpt.isDefined match {
      case false => Set()
      case true => Set(strOpt.get)
    }
  }

  def checkPrimaryServiceLine(serviceLineToCheck: Array[(Int, Int)]): Option[String] = {
    serviceLineToCheck.isEmpty match {
      case true => None
      case false =>
        val checked = primaryServiceLines.get(serviceLineToCheck.head)

        checked.isDefined match {
          case false => None
          case true => Some(checked.get.toString + ";0")
        }
    }
  }

  def getPrimaryServiceLine(msDrgServiceLines: Option[String], procServiceLines: Option[String]): Option[String] = {
    val msDrgServiceLinesSplit: Array[(Int, Int)] = msDrgServiceLines.isDefined match {
      case false => Array()
      case true => msDrgServiceLines.get.split(",").map{ case line =>
        val newLine = line.split(";")
        (newLine(0).toInt, newLine(1).toInt)
      }
    }

    val procServiceLinesSplit: Array[(Int, Int)] = procServiceLines.isDefined match {
      case false => Array()
      case true => procServiceLines.get.split(",").map { case line =>
        val newLine = line.split(";")
        (newLine(0).toInt, newLine(1).toInt)
      }
    }

    val msDrgServiceLineLevel1 = msDrgServiceLinesSplit.filter{ case (code, codeType) => codeType.equals(52) }
    val msDrgServiceLineLevel2 = msDrgServiceLinesSplit.filter{ case (code, codeType) => codeType.equals(53) }
    val procServiceLineLevel1 = procServiceLinesSplit.filter{ case (code, codeType) => codeType.equals(81) }
    val procServiceLineLevel2 = procServiceLinesSplit.filter{ case (code, codeType) => codeType.equals(82) }

    // Check in the following order: Drg Service Line 2, Drg Service Line 1, Proc Service Line 2, Proc Service Line 1
    checkPrimaryServiceLine(msDrgServiceLineLevel2).isDefined match {
      case true => checkPrimaryServiceLine(msDrgServiceLineLevel2)
      case false => checkPrimaryServiceLine(msDrgServiceLineLevel1).isDefined match {
          case true => checkPrimaryServiceLine(msDrgServiceLineLevel1)
          case false => checkPrimaryServiceLine(procServiceLineLevel2).isDefined match {
              case true => checkPrimaryServiceLine(procServiceLineLevel2)
              case false => checkPrimaryServiceLine(procServiceLineLevel1)
            }
        }
    }

  }

  def getProcServiceLines(procCodes: List[(String, Int)], cptCodes: List[(String, Int)], sex: Option[String], numOfCodesToSearch: Int): Option[String] = {

    // Check procedure1, then cpt1, then procedure2, then cpt2, etc.
    var index: Int = 0
    var serviceLines: Option[String] = None

    // Loops for the defined number of codes to search (e.g. 5 procs/cpts) or until the serviceLines value is defined
    // TODO: Redesign this as a recursive function
    while(index < numOfCodesToSearch || serviceLines.isDefined) {

      val procLookup = procCodes.isDefinedAt(index) match {
        case false => None
        case true => procedureServiceLines.get(procCodes(index))
      }

      serviceLines = checkProcedure(procLookup, sex)

      if(serviceLines.isEmpty) {
          val cptLookup = cptCodes.isDefinedAt(index) match{
            case false => None
            case true => procedureServiceLines.get(cptCodes(index))
          }

          serviceLines = checkProcedure(cptLookup, sex)
      }

      index = index + 1
    }

    serviceLines
  }

  def checkProcedure(procMatch: Option[(Int, String, Int, String, Int, String, String)], sex: Option[String]): Option[String] = {
    procMatch.isDefined match {
      case true =>
        val (procLevel1Code, procLevel1Desc, procLevel2Code, procLevel2Desc, procLevel3Code, procLevel3Desc, procsex) = procMatch.get

        procsex match {
          case "" =>
            Some(procLevel1Code.toString + ";81," + procLevel2Code.toString + ";82," + procLevel3Code.toString + ";83")
          case _ => sex.isDefined match {
            case true if sex.get.equalsIgnoreCase(procsex) =>
              Some(procLevel1Code.toString + ";81," + procLevel2Code.toString + ";82," + procLevel3Code.toString + ";83")
            case _ => None
          }
        }
      case false => None
    }
  }

  def getMsDrgServiceLines(msDrg: List[(String, Int)], sex: Option[String]): Option[String] = {
    msDrg.isEmpty match {
      case true => None
      case false =>
        val drg: String = msDrg.head._1

        val drgLookup = drgServiceLines.get(drg)

        drgLookup.isDefined match {
          case false => None
          case true =>
            val (drgLevel1Code, drgLevel1Desc, drgLevel2Code, drgLevel2Desc, drgLevel3Code, drgLevel3Desc, drgsex) = drgLookup.get

            drgsex match {
              case "" => Some(drgLevel1Code.toString + ";52," + drgLevel2Code + ";53")
              case _ => sex.isDefined match {
                case true if sex.get.equalsIgnoreCase(drgsex) => Some(drgLevel1Code.toString + ";52," + drgLevel2Code + ";53")
                case _ => None
              }
            }
        }
    }
  }

  def getDiagServiceLines(diagCodes: List[(String, Int)], sex: Option[String]): Option[String] = {
    diagCodes.isEmpty match {
      case true => None
      case false =>
        val primaryDiag: (String, Int) = diagCodes.head

        val diagLookup = diagnosisServiceLines.get(primaryDiag)

        diagLookup.isDefined match {
          case false => None
          case true =>
            val (diagLevel1Code, diagLevel1Desc, diagLevel2Code, drgLevel2Desc, diagsex) = diagLookup.get

            diagsex match {
              case "" => Some(diagLevel1Code.toString + ";21," + diagLevel2Code + ";22")
              case _ => sex.isDefined match {
                case true if sex.get.equalsIgnoreCase(diagsex) => Some(diagLevel1Code.toString + ";21," + diagLevel2Code + ";22")
                case _ => None
              }
            }
        }
    }
  }

  def getCodeGroups(codes: Option[List[String]]): Option[Set[Int]] = {

    val codeGrps: Option[Set[Int]] = codes.isEmpty match {
      case true => None
      case false =>
        val codesSplit: List[(String, Int)] =
          codes.get.map{ case codeAndType =>
            val split = codeAndType.split(";")
            (split(0).toLowerCase, split(1).toInt) }

        val groups = codeGroups.filter{ case (codeGroup, codeSeq) => codeSeq.exists( codeAndType => codesSplit.contains(codeAndType)) }.keys.toSet

        groups.isEmpty match {
          case true => None
          case false => Some(groups)
        }

    }

    codeGrps
  }

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

    val ageCalculated: Option[Int] = dob.isDefined match {
      case false => ageRaw
      case true =>
        val dobGet = dob.get.toLocalDateTime
        val today = DateTime.now().toLocalDateTime
        try {
          Some(new Period(dobGet, today, PeriodType.yearMonthDay).getYears)
        } catch {
          case err: Throwable => throw new Error(f"issue getting time period for $dobGet",err)
        }
    }

    val ageGroup = ageCalculated.isDefined match {
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

  def getCbsaValues(map: Map[String, Any], validAddressFlag: Option[Boolean]): (Option[String], Option[String]) = {
    val county = getAddressStringValue(map, "county", validAddressFlag)

    validAddressFlag.isDefined match{
      case false => (None, Some("C"))
      case true => validAddressFlag.get match{
        case false => (None, Some("C"))
        case true => county.isDefined match {
          case false => (None, Some("C"))
          case true => countyToCbsa.get(county.get).isDefined match {
            case false => (None, Some("C"))
            case true =>
              val tuple = countyToCbsa.get(county.get).get
              (Some(tuple._1), Some(tuple._2))
          }
        }
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
    (32, "icd10_diag"),
    (41, "icd9_proc"),
    (42, "icd10_proc"),
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

  def cleanseMedicalCodes(codeAndType: String): String = {

    val split = codeAndType.split(";")
    val code: String = split(0)
    val codeType: String = split(1)

    val cleansedCode = codeType match{
      case "11" => Clean.cpt(code)
      case "31" => Clean.icdDiag(code, 9)
      case "32" => Clean.icdDiag(code, 10)
      case "41" => Clean.icdProc(code, 9)
      case "42" => Clean.icdProc(code, 10)
      case "51" => Clean.msDrg(code)
    }

    cleansedCode + ";" + codeType
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


