package com.influencehealth.baldur.intake.meta.piedmont

import com.influencehealth.baldur.intake.meta._
import com.influencehealth.baldur.support._

/**
 * Piedmont Physicians Office
 */
object PhysicianOffice extends ClientInputMeta with Piedmont with Serializable {
  override def originalFields(): Seq[Product] = wrapRefArray(Array(
    ("sourceRecordId", "string"),
    ("sourcePersonId", "string"),
    ("facilityId", "int"),
    ("facilityName", "string"),
    ("facilityAddress", "string"),
    ("facilityCity", "string"),
    ("facilityState", "string"),
    ("facilityZip", "string"),
    ("lastName", "string"),
    ("firstName", "string"),
    ("address1", "string"),
    ("city", "string"),
    ("state", "string"),
    ("zip5", "string"),
    ("sex", "string"),
    ("sexDescription", "string"),
    ("age", "int"),
    ("dob", "date", "MM/dd/yyyy"),
    ("homePhone", "string"),
    ("birthYear", "int"),
    ("birthDay", "int"),
    ("birthMonth", "string"),
    ("dischargeDate", "date", "MM/dd/yyyy"),
    ("payorId", "int"),
    ("payorName", "string"),
    ("patientType", "string"),
    ("patientTypeDesc", "string"),
    ("departmentId", "string"),
    ("departmentName", "string"),
    ("patientEmail", "string"),
    ("primaryDxId", "string"),
    ("primaryDxDescription", "string"),
    ("dxTwoId", "string"),
    ("dxThreeId", "string"),
    ("dxFourId", "string"),
    ("dxFiveId", "string"),
    ("dxSixId", "string")))

  override def mapping(map: Map[String, Any]): ActivityOutput = {
    val zipInput = FileInputSupport.getStringOptValue(map, "zip5")

    val zip5: Option[String] = FileInputSupport.containsHyphen(zipInput) match {
      case true => Some(zipInput.get.split("-")(0))
      case false => zipInput
    }

    val zip4: Option[String] = FileInputSupport.containsHyphen(zipInput) match {
      case true => Some(zipInput.get.split("-")(1))
      case false => None
    }

    val financialClass: (Option[Int], Option[String]) = getFinancialClasses(map)

    ActivityOutput(
      personId = None,
      customerId = CustomerId,
      addressId = None,
      householdId = None,
      messageType = "utilization",
      source = "epic",
      sourceType = "physician office",
      personType = "c",
      sourcePersonId = FileInputSupport.getStringValue(map, "sourcePersonId"),
      sourceRecordId = FileInputSupport.getStringValue(map, "sourceRecordId"),
      trackingDate = FileInputSupport.getDateValue(map, "dischargeDate"),
      firstName = FileInputSupport.getStringOptValue(map, "firstName"),
      middleName = None,
      lastName = FileInputSupport.getStringOptValue(map, "lastName"),
      prefix = None,
      personalSuffix = None,
      dob = FileInputSupport.getDateOptValue(map, "dob"),
      age = FileInputSupport.getIntOptValue(map, "age"),
      sex = FileInputSupport.getStringOptValue(map, "sex"),
      payerType = None,
      maritalStatus = None,
      ethnicInsight = None,
      race = None,
      religion = None,
      language = None,
      occupationGroup = None,
      occupation = None,
      phoneNumbers = getPhoneNumbers(map),
      emails = getEmails(map),
      dwellType = None,
      combinedOwner = None,
      householdIncome = None,
      recipientReliabilityCode = None,
      mailResponder = None,
      lengthOfResidence = None,
      personsInLivingUnit = None,
      adultsInLivingUnit = None,
      childrenInLivingUnit = None,
      homeYearBuilt = None,
      homeLandValue = None,
      estimatedHomeValue = None,
      donatesToCharity = None,
      mosaicZip4 = None,
      mosaicGlobalZip4 = None,
      hhComp = None,
      presenceOfChild = None,
      childZeroToThreeBkt = None,
      childFourToSixBkt = None,
      childSevenToNineBkt = None,
      childTenToTwelveBkt = None,
      childThirteenToFifteenBkt = None,
      childSixteenToEighteenBkt = None,
      wealthRating = None,
      addressQualityIndicator = None,
      addressType = Some("home"),
      validAddressFlag = None,
      address1 = FileInputSupport.getStringOptValue(map, "address1"),
      address2 = None,
      city = FileInputSupport.getStringOptValue(map, "city"),
      state = FileInputSupport.getStringOptValue(map, "state"),
      zip5 = zip5,
      zip4 = zip4,
      county = None,
      carrierRoute = None,
      dpbc = None,
      lat = None,
      lon = None,
      streetPreDir = None,
      streetName = None,
      streetPostDir = None,
      streetSuffix = None,
      streetSecondNumber = None,
      streetSecondUnit = None,
      streetHouseNum = None,
      msa = None,
      pmsa = None,
      dpv = None,
      countyCode = None,
      censusBlock = None,
      censusTract = None,
      servicedOn = FileInputSupport.getDateOptValue(map, "dischargeDate"),
      locationId = getLocationIdFromUtil(map),
      activityType = Some("encounter"),
      mxCodes = getMedicalCodes(map),
      mxGroups = None,
      providers = None,
      erPatient = Some(false),
      financialClassId = financialClass._1,
      financialClass = financialClass._2,
      serviceLines = None,
      patientType = Some("o"),
      dischargeStatus = None,
      admittedAt = None,
      dischargedAt = FileInputSupport.getDateOptValue(map, "dischargeDate"),
      finalBillDate = None,
      transactionDate = None,
      activityDate = FileInputSupport.getDateOptValue(map, "dischargeDate"),
      hospitalId = None,
      hospital = None,
      businessUnitId = FileInputSupport.getStringOptValue(map, "departmentId"),
      businessUnit = FileInputSupport.getStringOptValue(map, "departmentName"),
      siteId = None,
      site = None,
      clinicId = None,
      clinic = None,
      practiceLocationId = None,
      practiceLocation = None,
      facilityId = FileInputSupport.getStringOptValue(map, "facilityId"),
      facility = FileInputSupport.getStringOptValue(map, "facilityName"),
      insuranceId = FileInputSupport.getStringOptValue(map, "payorId"),
      insurance = FileInputSupport.getStringOptValue(map, "payorName"),
      charges = None,
      cost = None,
      revenue = None,
      contributionMargin = None,
      profit = None,
      systolic = None,
      diastolic = None,
      height = None,
      weight = None,
      bmi = None,
      guarantorFirstName = None,
      guarantorLastName = None,
      guarantorMiddleName = None,
      activityId = None,
      activity = None,
      activityGroupId = None,
      activityGroup = None,
      activityLocationId = FileInputSupport.getStringOptValue(map, "facilityId"),
      activityLocation = FileInputSupport.getStringOptValue(map, "facilityName"),
      assessments = None,
      assessmentQuestions = None,
      assessmentAnswers = None,
      reasonId = None,
      reason = None
    )

  }



  def getMedicalCodes(map: Map[String, Any]): Option[List[String]] = {
    val primaryDxId: Option[String] = FileInputSupport.getMedicalCodeString(map, "primaryDxId", "icd9_diag", "|")
    val dx2: Option[String] = FileInputSupport.getMedicalCodeString(map, "dxTwoId", "icd9_diag", "|")
    val dx3: Option[String] = FileInputSupport.getMedicalCodeString(map, "dxThreeId", "icd9_diag", "|")
    val dx4: Option[String] = FileInputSupport.getMedicalCodeString(map, "dxFourId", "icd9_diag", "|")
    val dx5: Option[String] = FileInputSupport.getMedicalCodeString(map, "dxFiveId", "icd9_diag", "|")
    val dx6: Option[String] = FileInputSupport.getMedicalCodeString(map, "dxSixId", "icd9_diag", "|")

    concatonateMedicalCodes(primaryDxId, dx2, dx3, dx4, dx5, dx6)

  }

  def concatonateMedicalCodes(primaryDxId: Option[String],
                              dx2: Option[String],
                              dx3: Option[String],
                              dx4: Option[String],
                              dx5: Option[String],
                              dx6: Option[String]): Option[List[String]] ={
    Some(Seq(primaryDxId, dx2, dx3, dx4, dx5, dx6)
      .filter(_.nonEmpty)
      .map(_.get)
      .mkString(",")
      .split(",")
      .toList)
  }

}
