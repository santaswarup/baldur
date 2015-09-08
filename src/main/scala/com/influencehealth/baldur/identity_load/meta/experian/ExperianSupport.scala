package com.influencehealth.baldur.identity_load.meta.experian

import com.influencehealth.baldur.identity_load.meta._
import com.influencehealth.baldur.support._
import org.joda.time.DateTime

/**
 * Experian schema
 */
object ExperianSupport {

  def getFinancialClass(beehive: Option[Int], age: Option[Int]): (Option[Int], Option[String], Option[String]) = {
    age.isDefined match{
      case false =>
        beehive.isDefined match {
          case false => (None, None, None)
          case true => ExperianConstants.beehiveToFinancialClass.getOrElse(beehive.get, (None,None,None))
      }
      case true => age.get match {
        case value if value >= 65 => (Some(9930), Some("TARGET PAYER  - MEDICARE"), Some("MI"))
        case _ =>
          beehive.isDefined match {
            case false => (None, None, None)
            case true => ExperianConstants.beehiveToFinancialClass.getOrElse(beehive.get, (None,None,None))
        }
      }
    }
  }

  def getBeehiveCluster(map: Map[String, Any]): Option[Int] = {
    val ageDob: (Option[DateTime], Option[Int], Option[String]) = FileInputSupport.getAgeDob(map, "age", "dob")
    val age: Option[Int] = ageDob._2

    val education: Option[Int] = FileInputSupport.getIntOptValue(map, "education")
    val maritalStatus: Option[String] = FileInputSupport.getStringOptValue(map, "maritalStatus")
    val occupationGroup: Option[String] = FileInputSupport.getStringOptValue(map, "occupationGroup")
    val lastName: Option[String] = FileInputSupport.getStringOptValue(map, "lastName")
    val householdIncome: Option[String] = FileInputSupport.getStringOptValue(map, "householdIncome")
    val presenceOfChild: Option[String] = FileInputSupport.getStringOptValue(map, "presenceOfChild")

    val educationCode: Option[String] =
      education.isDefined match{
        case false => None
        case true => ExperianConstants.educationCodes.get(education.get)
      }

    val maritalStatusCode: Option[String] =
      maritalStatus.isDefined match{
        case false => None
        case true => ExperianConstants.maritalStatusCodes.get(maritalStatus.get.toUpperCase)
      }

    val occupationGroupCode: Option[String] =
      occupationGroup.isDefined match{
        case false => None
        case true => ExperianConstants.occupationCodes.get(occupationGroup.get.last.toInt)
      }

    val householdIncomeCode: Option[String] =
      householdIncome.isDefined match{
        case false => None
        case true => ExperianConstants.incomeCodes.get(householdIncome.get.toUpperCase)
      }

    val ageCode: Option[String] =
      age.isDefined match{
        case false => None
        case true => ExperianConstants.ageCodes.get(age.get)
      }

    val pocCode: Option[String] =
      presenceOfChild.isDefined match {
        case false => None
        case true => Some(ExperianConstants.presenceOfChildCodes.getOrElse(presenceOfChild.get.toUpperCase, "44"))
      }

    val ethnicCode: Option[String] =
      lastName.isDefined match {
        case false => Some("34")
        case true => Some(ExperianConstants.presenceOfChildCodes.getOrElse(lastName.get.toUpperCase, "34"))
      }

    val allDefined: Boolean = educationCode.isDefined && maritalStatusCode.isDefined && householdIncomeCode.isDefined &&
      ageCode.isDefined && ethnicCode.isDefined && occupationGroupCode.isDefined && pocCode.isDefined

    val combinedCode: String = allDefined match {
      case false => ""
      case true => educationCode.get + maritalStatusCode.get + householdIncomeCode.get + ageCode.get + ethnicCode.get +
        occupationGroupCode.get + pocCode.get
    }

    Some(ExperianConstants.combinedToCluster.getOrElse(combinedCode,99))
  }


  def getPhoneNumbers(map: Map[String, Any]): Option[List[String]] = {
    val phoneString: Option[String] = FileInputSupport.getStringOptValue(map, "phoneNumbers")
    val phoneStringCleansed: Option[String] = phoneString.isDefined match {
      case false => None
      case true => Some(phoneString.get.replace("[","").replace("]","").replace("u","").replace("'","").replace(" ",""))
    }

    phoneStringCleansed.isDefined match {
      case false => None
      case true => Some(phoneStringCleansed.get.split(",").toList)
    }

  }

  def getChildAgeBuckets(map: Map[String, Any]): Option[Set[String]] = {
    val childZeroToThreeBkt = FileInputSupport.getStringOptValue(map, "childZeroToThreeBkt")
    val childFourToSixBkt = FileInputSupport.getStringOptValue(map, "childFourToSixBkt")
    val childSevenToNineBkt = FileInputSupport.getStringOptValue(map, "childSevenToNineBkt")
    val childTenToTwelveBkt = FileInputSupport.getStringOptValue(map, "childTenToTwelveBkt")
    val childThirteenToFifteenBkt = FileInputSupport.getStringOptValue(map, "childThirteenToFifteenBkt")
    val childSixteenToEighteenBkt = FileInputSupport.getStringOptValue(map, "childSixteenToEighteenBkt")

    var childAgeBuckets: Option[Set[String]] = Some(Set())

    childAgeBuckets = appendToChildAgeBuckets(childZeroToThreeBkt, childAgeBuckets, "A")
    childAgeBuckets = appendToChildAgeBuckets(childFourToSixBkt, childAgeBuckets, "B")
    childAgeBuckets = appendToChildAgeBuckets(childSevenToNineBkt, childAgeBuckets, "C")
    childAgeBuckets = appendToChildAgeBuckets(childTenToTwelveBkt, childAgeBuckets, "D")
    childAgeBuckets = appendToChildAgeBuckets(childThirteenToFifteenBkt, childAgeBuckets, "E")
    childAgeBuckets = appendToChildAgeBuckets(childSixteenToEighteenBkt, childAgeBuckets, "F")

    childAgeBuckets.get.isEmpty match{
      case true => None
      case false => childAgeBuckets
    }
  }

  def appendToChildAgeBuckets(stringOpt: Option[String], setOpt: Option[Set[String]], setValue: String) = {
    stringOpt.isDefined match {
      case false => setOpt
      case true => stringOpt.get match {
        case "Y" => Some(setOpt.get + setValue)
        case _ => setOpt
      }
    }
  }

  def appendClientIds(map: Map[String, Any]): Seq[Map[String, Any]] = {
    val validAddressFlag = FileInputSupport.getValidAddressFlag(FileInputSupport.getStringOptValue(map, "ncoaActionCode"))
    val zip5 = FileInputSupport.getAddressStringValue(map, "zip5", validAddressFlag)

    val clientIdSeq: Iterable[Int] =
      zip5.isDefined match{
        case false => Iterable()
        case true =>
          ExperianConstants.zipToClient
          .filter{ case (clientId, zip) => zip.equals(zip5.get)}
          .keys
      }

    clientIdSeq.nonEmpty match {
      case false => Seq()
      case true => clientIdSeq.map{ case id => map + ("customerId" -> id) }.toSeq
    }
  }

}
