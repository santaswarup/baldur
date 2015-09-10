package com.influencehealth.baldur.identity_load.meta.baldur

import java.util.UUID

import com.influencehealth.baldur.identity_load.meta._
import com.influencehealth.baldur.support._

/**
 * Baldur schema
 */
object BaldurSchema extends FileInputMeta with Serializable {
  override def originalFields(): Seq[Product] = wrapRefArray(Array(
    ("personId", "string"),
    ("customerId", "int"),
    ("addressId", "string"),
    ("householdId", "string"),
    ("messageType", "string"),
    ("source", "string"),
    ("sourceType", "string"),
    ("personType", "string"),
    ("sourcePersonId", "string"),
    ("sourceRecordId", "string"),
    ("trackingDate", "date","yyyyMMdd"),
    ("firstName", "title"),
    ("middleName", "title"),
    ("lastName", "title"),
    ("prefix", "string"),
    ("personalSuffix", "string"),
    ("dob", "date","yyyyMMdd"),
    ("age", "int"),
    ("ageGroup", "string"),
    ("sex", "string"),
    ("payerType", "string"),
    ("maritalStatus", "string"),
    ("ethnicInsight", "string"),
    ("race", "string"),
    ("religion", "string"),
    ("language", "string"),
    ("occupationGroup", "string"),
    ("occupation", "string"),
    ("phoneNumbers", "string"),
    ("emails", "string"),
    ("dwellType", "string"),
    ("combinedOwner", "string"),
    ("householdIncome", "string"),
    ("recipientReliabilityCode", "int"),
    ("mailResponder", "string"),
    ("lengthOfResidence", "int"),
    ("personsInLivingUnit", "int"),
    ("adultsInLivingUnit", "int"),
    ("childrenInLivingUnit", "int"),
    ("homeYearBuilt", "int"),
    ("homeLandValue", "float"),
    ("estimatedHomeValue", "string"),
    ("donatesToCharity", "string"),
    ("mosaicZip4", "string"),
    ("mosaicGlobalZip4", "string"),
    ("hhComp", "string"),
    ("presenceOfChild", "string"),
    ("childZeroToThreeBkt", "string"),
    ("childFourToSixBkt", "string"),
    ("childSevenToNineBkt", "string"),
    ("childTenToTwelveBkt", "string"),
    ("childThirteenToFifteenBkt", "string"),
    ("childSixteenToEighteenBkt", "string"),
    ("childAgeBuckets", "string"),
    ("wealthRating", "int"),
    ("addressQualityIndicator", "string"),
    ("education", "int"),
    ("addressType", "string"),
    ("validAddressFlagOld", "boolean"),
    ("address1Old", "string"),
    ("address2Old", "string"),
    ("cityOld", "string"),
    ("stateOld", "string"),
    ("zip5Old", "string"),
    ("zip4Old", "string"),
    ("countyOld", "string"),
    ("carrierRouteOld", "string"),
    ("dpbcOld", "string"),
    ("latOld", "float"),
    ("lonOld", "float"),
    ("streetPreDirOld","string"),
    ("streetNameOld","string"),
    ("streetPostDirOld","string"),
    ("streetSuffixOld","string"),
    ("streetSecondNumberOld","string"),
    ("streetSecondUnitOld","string"),
    ("streetHouseNumOld","string"),
    ("msaOld","string"),
    ("pmsaOld","string"),
    ("cbsaOld","string"),
    ("cbsaTypeOld","string"),
    ("dpvOld","string"),
    ("countyCodeOld","string"),
    ("censusBlockOld","string"),
    ("censusTractOld","string"),
    ("beehiveCluster","int"),
    ("primaryCarePhysician","long"),
    ("servicedOn", "date","yyyyMMdd"),
    ("locationId", "int"),
    ("activityType", "string"),
    ("mxCodes", "string"),
    ("mxGroups", "string"),
    ("providers", "string"),
    ("erPatient", "boolean"),
    ("financialClassId", "int"),
    ("financialClass", "string"),
    ("serviceLines", "string"),
    ("patientType", "string"),
    ("dischargeStatus", "int"),
    ("admittedAt", "date","yyyyMMdd"),
    ("dischargedAt", "date","yyyyMMdd"),
    ("finalBillDate", "date","yyyyMMdd"),
    ("transactionDate", "date","yyyyMMdd"),
    ("activityDate", "date","yyyyMMdd"),
    ("hospitalId", "string"),
    ("hospital", "string"),
    ("businessUnitId", "string"),
    ("businessUnit", "string"),
    ("siteId", "string"),
    ("site", "string"),
    ("clinicId", "string"),
    ("clinic", "string"),
    ("practiceLocationId", "string"),
    ("practiceLocation", "string"),
    ("facilityId", "string"),
    ("facility", "string"),
    ("insuranceId", "string"),
    ("insurance", "string"),
    ("charges", "float"),
    ("cost", "float"),
    ("revenue", "float"),
    ("contributionMargin", "float"),
    ("profit", "float"),
    ("systolic", "float"),
    ("diastolic", "float"),
    ("height", "float"),
    ("weight", "float"),
    ("bmi", "float"),
    ("guarantorFirstName", "string"),
    ("guarantorLastName", "string"),
    ("guarantorMiddleName", "string"),
    ("activityId", "string"),
    ("activity", "string"),
    ("activityGroupId", "string"),
    ("activityGroup", "string"),
    ("activityLocationId", "string"),
    ("activityLocation", "string"),
    ("assessments", "string"),
    ("assessmentQuestions", "string"),
    ("assessmentAnswers", "string"),
    ("reasonId", "string"),
    ("reason", "string"),
    ("address1", "string"),
    ("address2", "string"),
    ("city", "string"),
    ("state", "string"),
    ("zip5", "string"),
    ("zip10", "string"),
    ("zip4", "string"),
    ("streetPreDir", "string"),
    ("streetName", "string"),
    ("streetPostDir", "string"),
    ("streetSuffix", "string"),
    ("streetSecondNumber", "string"),
    ("streetSecondUnit", "string"),
    ("lat", "string"),
    ("lon", "string"),
    ("msa", "string"),
    ("pmsa", "string"),
    ("dpbc", "string"),
    ("dpv", "string"),
    ("countyCode", "string"),
    ("county", "string"),
    ("carrierRoute", "string"),
    ("censusBlock", "string"),
    ("censusTract", "string"),
    ("eLot", "string"),
    ("streetHouseNum", "string"),
    ("ncoaActionCode", "string"),
    ("ncoaAnkCode", "string"),
    ("ncoaMoveType", "string"),
    ("ncoaMoveDate", "string"),
    ("lacsFootnote", "string"),
    ("barcode", "string"),
    ("lacs", "string"),
    ("cassStandardizeIndicator", "string"),
    ("dpvFootnote", "string")
  ))

  override def mapping(input: Map[String, Any]): ActivityOutput = {

    val validAddressFlag = FileInputSupport.getValidAddressFlag(FileInputSupport.getStringOptValue(input, "ncoaActionCode"))
    val lat = getAnchorLatLon(FileInputSupport.getAddressStringValue(input, "lat", validAddressFlag))
    val lon = getAnchorLatLon(FileInputSupport.getAddressStringValue(input, "lon", validAddressFlag))

    val (cbsa, cbsa_type) = FileInputSupport.getCbsaValues(input, validAddressFlag)

    // Logic below temporarily added to handle Piedmont's duplicate issue in the physician office data set
    val customerId = FileInputSupport.getIntValue(input, "customerId")
    val sourceType = FileInputSupport.getStringValue(input, "sourceType")

    val sourceRecordId = customerId match{
      case 1 => sourceType match {
        case "hospital" => FileInputSupport.getStringValue(input, "sourceRecordId")
        case "physician office" => UUID.randomUUID().toString
      }
      case _ => FileInputSupport.getStringValue(input, "sourceRecordId")
    }

    ActivityOutput(
      personId = FileInputSupport.getUUIDOptValue(input, "personId"),
      customerId = customerId,
      addressId = FileInputSupport.getUUIDOptValue(input, "addressId"),
      householdId = FileInputSupport.getUUIDOptValue(input, "householdId"),
      messageType = FileInputSupport.getStringValue(input, "messageType"),
      source = FileInputSupport.getStringValue(input, "source"),
      sourceType = sourceType,
      personType = FileInputSupport.getStringValue(input, "personType"),
      sourcePersonId = FileInputSupport.getStringValue(input, "sourcePersonId"),
      sourceRecordId = sourceRecordId,
      trackingDate = FileInputSupport.getDateValue(input, "trackingDate"),
      firstName = FileInputSupport.getStringOptValue(input, "firstName"),
      middleName = FileInputSupport.getStringOptValue(input, "middleName"),
      lastName = FileInputSupport.getStringOptValue(input, "lastName"),
      prefix = FileInputSupport.getStringOptValue(input, "prefix"),
      personalSuffix = FileInputSupport.getStringOptValue(input, "personalSuffix"),
      dob = FileInputSupport.getDateOptValue(input, "dob"),
      age = FileInputSupport.getIntOptValue(input, "age"),
      ageGroup = FileInputSupport.getStringOptValue(input, "ageGroup"),
      sex = FileInputSupport.getStringOptValue(input, "sex"),
      payerType = FileInputSupport.getStringOptValue(input, "payerType"),
      maritalStatus = FileInputSupport.getStringOptValue(input, "maritalStatus"),
      ethnicInsight = FileInputSupport.getStringOptValue(input, "ethnicInsight"),
      race = FileInputSupport.getStringOptValue(input, "race"),
      religion = FileInputSupport.getStringOptValue(input, "religion"),
      language = FileInputSupport.getStringOptValue(input, "language"),
      occupationGroup = FileInputSupport.getStringOptValue(input, "occupationGroup"),
      occupation = FileInputSupport.getStringOptValue(input, "occupation"),
      phoneNumbers = FileInputSupport.getListOptValue(input, "phoneNumbers"),
      emails = FileInputSupport.getListOptValue(input, "emails"),
      dwellType = FileInputSupport.getStringOptValue(input, "dwellType"),
      combinedOwner = FileInputSupport.getStringOptValue(input, "combinedOwner"),
      householdIncome = FileInputSupport.getStringOptValue(input, "householdIncome"),
      recipientReliabilityCode = FileInputSupport.getIntOptValue(input, "recipientReliabilityCode"),
      mailResponder = FileInputSupport.getStringOptValue(input, "mailResponder"),
      lengthOfResidence = FileInputSupport.getIntOptValue(input, "lengthOfResidence"),
      personsInLivingUnit = FileInputSupport.getIntOptValue(input, "personsInLivingUnit"),
      adultsInLivingUnit = FileInputSupport.getIntOptValue(input, "adultsInLivingUnit"),
      childrenInLivingUnit = FileInputSupport.getIntOptValue(input, "childrenInLivingUnit"),
      homeYearBuilt = FileInputSupport.getIntOptValue(input, "homeYearBuilt"),
      homeLandValue = FileInputSupport.getFloatOptValue(input, "homeLandValue"),
      estimatedHomeValue = FileInputSupport.getStringOptValue(input, "estimatedHomeValue"),
      donatesToCharity = FileInputSupport.getStringOptValue(input, "donatesToCharity"),
      mosaicZip4 = FileInputSupport.getStringOptValue(input, "mosaicZip4"),
      mosaicGlobalZip4 = FileInputSupport.getStringOptValue(input, "mosaicGlobalZip4"),
      hhComp = FileInputSupport.getStringOptValue(input, "hhComp"),
      presenceOfChild = FileInputSupport.getStringOptValue(input, "presenceOfChild"),
      childZeroToThreeBkt = FileInputSupport.getStringOptValue(input, "childZeroToThreeBkt"),
      childFourToSixBkt = FileInputSupport.getStringOptValue(input, "childFourToSixBkt"),
      childSevenToNineBkt = FileInputSupport.getStringOptValue(input, "childSevenToNineBkt"),
      childTenToTwelveBkt = FileInputSupport.getStringOptValue(input, "childTenToTwelveBkt"),
      childThirteenToFifteenBkt = FileInputSupport.getStringOptValue(input, "childThirteenToFifteenBkt"),
      childSixteenToEighteenBkt = FileInputSupport.getStringOptValue(input, "childSixteenToEighteenBkt"),
      childAgeBuckets = FileInputSupport.getSetOptValue(input, "childAgeBuckets"),
      wealthRating = FileInputSupport.getIntOptValue(input, "wealthRating"),
      addressQualityIndicator = FileInputSupport.getStringOptValue(input, "addressQualityIndicator"),
      education = FileInputSupport.getIntOptValue(input, "education"),
      addressType = FileInputSupport.getStringOptValue(input, "addressType"),
      validAddressFlag = validAddressFlag,
      address1 = FileInputSupport.getAddressStringValue(input, "address1", validAddressFlag),
      address2 = FileInputSupport.getAddressStringValue(input, "address2", validAddressFlag),
      city = FileInputSupport.getAddressStringValue(input, "city", validAddressFlag),
      state = FileInputSupport.getAddressStringValue(input, "state", validAddressFlag),
      zip5 = FileInputSupport.getAddressStringValue(input, "zip5", validAddressFlag),
      zip4 = FileInputSupport.getAddressStringValue(input, "zip4", validAddressFlag),
      county = FileInputSupport.getAddressStringValue(input, "county", validAddressFlag),
      carrierRoute = FileInputSupport.getAddressStringValue(input, "carrierRoute", validAddressFlag),
      dpbc = FileInputSupport.getAddressStringValue(input, "dpbc", validAddressFlag),
      lat = lat,
      lon = lon,
      streetPreDir = FileInputSupport.getAddressStringValue(input, "streetPreDir", validAddressFlag),
      streetName = FileInputSupport.getAddressStringValue(input, "streetName", validAddressFlag),
      streetPostDir = FileInputSupport.getAddressStringValue(input, "streetPostDir", validAddressFlag),
      streetSuffix = FileInputSupport.getAddressStringValue(input, "streetSuffix", validAddressFlag),
      streetSecondNumber = FileInputSupport.getAddressStringValue(input, "streetSecondNumber", validAddressFlag),
      streetSecondUnit = FileInputSupport.getAddressStringValue(input, "streetSecondUnit", validAddressFlag),
      streetHouseNum = FileInputSupport.getAddressStringValue(input, "streetHouseNum", validAddressFlag),
      msa = FileInputSupport.getAddressStringValue(input, "msa", validAddressFlag),
      pmsa = FileInputSupport.getAddressStringValue(input, "pmsa", validAddressFlag),
      cbsa = cbsa,
      cbsa_type = cbsa_type,
      dpv = FileInputSupport.getAddressStringValue(input, "dpv", validAddressFlag),
      countyCode = FileInputSupport.getAddressStringValue(input, "countyCode", validAddressFlag),
      censusBlock = FileInputSupport.getAddressStringValue(input, "censusBlock", validAddressFlag),
      censusTract = FileInputSupport.getAddressStringValue(input, "censusTract", validAddressFlag),
      beehiveCluster = FileInputSupport.getIntOptValue(input, "beehiveCluster"),
      primaryCarePhysician = FileInputSupport.getLongOptValue(input, "primaryCarePhysician"),
      servicedOn = FileInputSupport.getDateOptValue(input, "servicedOn"),
      locationId = FileInputSupport.getIntOptValue(input, "locationId"),
      activityType = FileInputSupport.getStringOptValue(input, "activityType"),
      mxCodes = FileInputSupport.getListOptValue(input, "mxCodes"),
      mxGroups = FileInputSupport.getIntSetOptValue(input, "mxGroups"),
      providers = FileInputSupport.getSetOptValue(input, "providers"),
      erPatient = FileInputSupport.getBoolOptValue(input, "erPatient"),
      financialClassId = FileInputSupport.getIntOptValue(input, "financialClassId"),
      financialClass = FileInputSupport.getStringOptValue(input, "financialClass"),
      serviceLines = FileInputSupport.getSetOptValue(input, "serviceLines"),
      patientType = FileInputSupport.getStringOptValue(input, "patientType"),
      dischargeStatus = FileInputSupport.getIntOptValue(input, "dischargeStatus"),
      admittedAt = FileInputSupport.getDateOptValue(input, "admittedAt"),
      dischargedAt = FileInputSupport.getDateOptValue(input, "dischargedAt"),
      finalBillDate = FileInputSupport.getDateOptValue(input, "finalBillDate"),
      transactionDate = FileInputSupport.getDateOptValue(input, "transactionDate"),
      activityDate = FileInputSupport.getDateOptValue(input, "activityDate"),
      hospitalId = FileInputSupport.getStringOptValue(input, "hospitalId"),
      hospital = FileInputSupport.getStringOptValue(input, "hospital"),
      businessUnitId = FileInputSupport.getStringOptValue(input, "businessUnitId"),
      businessUnit = FileInputSupport.getStringOptValue(input, "businessUnit"),
      siteId = FileInputSupport.getStringOptValue(input, "siteId"),
      site = FileInputSupport.getStringOptValue(input, "site"),
      clinicId = FileInputSupport.getStringOptValue(input, "clinicId"),
      clinic = FileInputSupport.getStringOptValue(input, "clinic"),
      practiceLocationId = FileInputSupport.getStringOptValue(input, "practiceLocationId"),
      practiceLocation = FileInputSupport.getStringOptValue(input, "practiceLocation"),
      facilityId = FileInputSupport.getStringOptValue(input, "facilityId"),
      facility = FileInputSupport.getStringOptValue(input, "facility"),
      insuranceId = FileInputSupport.getStringOptValue(input, "insuranceId"),
      insurance = FileInputSupport.getStringOptValue(input, "insurance"),
      charges = FileInputSupport.getDoubleOptValue(input, "charges"),
      cost = FileInputSupport.getDoubleOptValue(input, "cost"),
      revenue = FileInputSupport.getDoubleOptValue(input, "revenue"),
      contributionMargin = FileInputSupport.getDoubleOptValue(input, "contributionMargin"),
      profit = FileInputSupport.getDoubleOptValue(input, "profit"),
      systolic = FileInputSupport.getDoubleOptValue(input, "systolic"),
      diastolic = FileInputSupport.getDoubleOptValue(input, "diastolic"),
      height = FileInputSupport.getDoubleOptValue(input, "height"),
      weight = FileInputSupport.getDoubleOptValue(input, "weight"),
      bmi = FileInputSupport.getDoubleOptValue(input, "bmi"),
      guarantorFirstName = FileInputSupport.getStringOptValue(input, "guarantorFirstName"),
      guarantorLastName = FileInputSupport.getStringOptValue(input, "guarantorLastName"),
      guarantorMiddleName = FileInputSupport.getStringOptValue(input, "guarantorMiddleName"),
      activityId = FileInputSupport.getStringOptValue(input, "activityId"),
      activity = FileInputSupport.getStringOptValue(input, "activity"),
      activityGroupId = FileInputSupport.getStringOptValue(input, "activityGroupId"),
      activityGroup = FileInputSupport.getStringOptValue(input, "activityGroup"),
      activityLocationId = FileInputSupport.getStringOptValue(input, "activityLocationId"),
      activityLocation = FileInputSupport.getStringOptValue(input, "activityLocation"),
      assessments = FileInputSupport.getSetOptValue(input, "assessments"),
      assessmentQuestions = FileInputSupport.getSetOptValue(input, "assessmentQuestions"),
      assessmentAnswers = FileInputSupport.getSetOptValue(input, "assessmentAnswers"),
      reasonId = FileInputSupport.getStringOptValue(input, "reasonId"),
      reason = FileInputSupport.getStringOptValue(input, "reason")
    )
  }
}
