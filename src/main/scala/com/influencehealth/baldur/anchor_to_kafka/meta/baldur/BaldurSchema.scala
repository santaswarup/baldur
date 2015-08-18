package com.influencehealth.baldur.anchor_to_kafka.meta.baldur

import com.influencehealth.baldur.anchor_to_kafka.meta._
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
    ("trackingDate", "date"),
    ("firstName", "string"),
    ("middleName", "string"),
    ("lastName", "string"),
    ("prefix", "string"),
    ("personalSuffix", "string"),
    ("dob", "date"),
    ("age", "int"),
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
    ("estimatedHomeValue", "float"),
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
    ("wealthRating", "int"),
    ("addressQualityIndicator", "string"),
    ("addressType", "string"),
    ("validAddressFlag", "string"),
    ("address1", "string"),
    ("address2", "string"),
    ("city", "string"),
    ("state", "string"),
    ("zip5", "string"),
    ("zip4", "string"),
    ("county", "string"),
    ("carrierRoute", "string"),
    ("dpbc", "string"),
    ("lat", "float"),
    ("lon", "float"),
    ("servicedOn", "date"),
    ("locationId", "int"),
    ("activityType", "string"),
    ("mxCodes", "string"),
    ("mxGroups", "string"),
    ("providers", "string"),
    ("erPatient", "string"),
    ("financialClassId", "int"),
    ("financialClass", "string"),
    ("serviceLines", "string"),
    ("patientType", "string"),
    ("dischargeStatus", "int"),
    ("admittedAt", "date"),
    ("dischargedAt", "date"),
    ("finalBillDate", "date"),
    ("transactionDate", "date"),
    ("activityDate", "date"),
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
    ("reason", "string")
  ))

  override def mapping(input: Map[String, Any]): ActivityOutput = {
    ActivityOutput(
      personId = FileInputSupport.getUUIDOptValue(input, "personId"),
      customerId = FileInputSupport.getIntValue(input, "customerId"),
      addressId = FileInputSupport.getUUIDOptValue(input, "addressId"),
      householdId = FileInputSupport.getUUIDOptValue(input, "householdId"),
      messageType = FileInputSupport.getStringValue(input, "messageType"),
      source = FileInputSupport.getStringValue(input, "source"),
      sourceType = FileInputSupport.getStringValue(input, "sourceType"),
      personType = FileInputSupport.getStringValue(input, "personType"),
      sourcePersonId = FileInputSupport.getStringValue(input, "sourcePersonId"),
      sourceRecordId = FileInputSupport.getStringValue(input, "sourceRecordId"),
      trackingDate = FileInputSupport.getDateValue(input, "trackingDate"),
      firstName = FileInputSupport.getStringOptValue(input, "firstName"),
      middleName = FileInputSupport.getStringOptValue(input, "middleName"),
      lastName = FileInputSupport.getStringOptValue(input, "lastName"),
      prefix = FileInputSupport.getStringOptValue(input, "prefix"),
      personalSuffix = FileInputSupport.getStringOptValue(input, "personalSuffix"),
      dob = FileInputSupport.getDateOptValue(input, "dob"),
      age = FileInputSupport.getIntOptValue(input, "age"),
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
      estimatedHomeValue = FileInputSupport.getFloatOptValue(input, "estimatedHomeValue"),
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
      wealthRating = FileInputSupport.getIntOptValue(input, "wealthRating"),
      addressQualityIndicator = FileInputSupport.getStringOptValue(input, "addressQualityIndicator"),
      addressType = FileInputSupport.getStringOptValue(input, "addressType"),
      validAddressFlag = FileInputSupport.getBoolOptValue(input, "validAddressFlag"),
      address1 = FileInputSupport.getStringOptValue(input, "address1"),
      address2 = FileInputSupport.getStringOptValue(input, "address2"),
      city = FileInputSupport.getStringOptValue(input, "city"),
      state = FileInputSupport.getStringOptValue(input, "state"),
      zip5 = FileInputSupport.getStringOptValue(input, "zip5"),
      zip4 = FileInputSupport.getStringOptValue(input, "zip4"),
      county = FileInputSupport.getStringOptValue(input, "county"),
      carrierRoute = FileInputSupport.getStringOptValue(input, "carrierRoute"),
      dpbc = FileInputSupport.getStringOptValue(input, "dpbc"),
      lat = FileInputSupport.getFloatOptValue(input, "lat"),
      lon = FileInputSupport.getFloatOptValue(input, "lon"),
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
