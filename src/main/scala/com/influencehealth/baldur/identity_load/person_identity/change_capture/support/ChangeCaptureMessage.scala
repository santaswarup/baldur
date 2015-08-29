package com.influencehealth.baldur.identity_load.person_identity.change_capture.support

import java.util.UUID
import com.influencehealth.baldur.identity_load.person_identity.support.{Support, SupportImpl}
import org.joda.time.DateTime
import play.api.libs.json.JsObject


case class ChangeCaptureMessage(
    //person-columns
    personId: UUID,
    customerId: Int,
    addressId: Option[UUID],
    householdId: Option[UUID],
    messageType: String,
    source: String,
    sourceType: String,
    personType: String,
    sourcePersonId: String,
    sourceRecordId: String,
    trackingDate: DateTime,
    firstName: Option[String],
    middleName: Option[String],
    lastName: Option[String],
    prefix: Option[String],
    personalSuffix: Option[String],
    dob: Option[DateTime],
    age: Option[Int],
    ageGroup: Option[String],
    sex: Option[String],
    payerType: Option[String],
    maritalStatus: Option[String],
    ethnicInsight: Option[String],
    race: Option[String],
    religion: Option[String],
    language: Option[Int],
    occupationGroup: Option[String],
    occupation: Option[String],
    phoneNumbers: Option[List[String]],
    emails: Option[List[String]],
    dwellType: Option[String],
    combinedOwner: Option[String],
    householdIncome: Option[String],
    recipientReliabilityCode: Option[Int],
    mailResponder: Option[String],
    lengthOfResidence: Option[Int],
    personsInLivingUnit: Option[Int],
    adultsInLivingUnit: Option[Int],
    childrenInLivingUnit: Option[Int],
    homeYearBuilt: Option[Int],
    homeLandValue: Option[Float],
    estimatedHomeValue: Option[String],
    donatesToCharity: Option[String],
    mosaicZip4: Option[String],
    mosaicGlobalZip4: Option[String],
    hhComp: Option[String],
    presenceOfChild: Option[String],
    childZeroToThreeBkt: Option[String],
    childFourToSixBkt: Option[String],
    childSevenToNineBkt: Option[String],
    childTenToTwelveBkt: Option[String],
    childThirteenToFifteenBkt: Option[String],
    childSixteenToEighteenBkt: Option[String],
    childAgeBuckets: Option[Set[String]],
    wealthRating: Option[Int],
    addressQualityIndicator: Option[String],
    education: Option[String],
    addressType: Option[String],
    validAddressFlag: Option[Boolean],
    address1: Option[String],
    address2: Option[String],
    city: Option[String],
    state: Option[String],
    zip5: Option[String],
    zip4: Option[String],
    county: Option[String],
    carrierRoute: Option[String],
    dpbc: Option[String],
    lat: Option[Float],
    lon: Option[Float],
    streetPreDir: Option[String],
    streetName: Option[String],
    streetPostDir: Option[String],
    streetSuffix: Option[String],
    streetSecondNumber: Option[String],
    streetSecondUnit: Option[String],
    streetHouseNum: Option[String],
    msa: Option[String],
    pmsa: Option[String],
    dpv: Option[String],
    countyCode: Option[String],
    censusBlock: Option[String],
    censusTract: Option[String],
    beehiveCluster: Option[Int],
    primaryCarePhysician: Option[Long],

    //activity columns
    servicedOn: Option[DateTime],
    locationId: Option[Int],
    activityType: Option[String],
    mxCodes: Option[List[String]],
    mxGroups: Option[Set[Int]],
    providers: Option[Set[String]],
    erPatient: Option[Boolean],
    financialClassId: Option[Int],
    financialClass: Option[String],
    serviceLines: Option[Set[String]],
    patientType: Option[String],
    dischargeStatus: Option[Int],

    //External dates
    admittedAt: Option[DateTime],
    dischargedAt: Option[DateTime],
    finalBillDate: Option[DateTime],
    transactionDate: Option[DateTime],
    activityDate: Option[DateTime],

    //External locations
    hospitalId: Option[String],
    hospital: Option[String],
    businessUnitId: Option[String],
    businessUnit: Option[String],
    siteId: Option[String],
    site: Option[String],
    clinicId: Option[String],
    clinic: Option[String],
    practiceLocationId: Option[String],
    practiceLocation: Option[String],
    facilityId: Option[String],
    facility: Option[String],

    //External financials
    insuranceId: Option[String],
    insurance: Option[String],
    charges: Option[Double],
    cost: Option[Double],
    revenue: Option[Double],
    contributionMargin: Option[Double],
    profit: Option[Double],

    //External biometrics
    systolic: Option[Double],
    diastolic: Option[Double],
    height: Option[Double],
    weight: Option[Double],
    bmi: Option[Double],

    //External guarantor
    guarantorFirstName: Option[String],
    guarantorLastName: Option[String],
    guarantorMiddleName: Option[String],

    //Activity fields derived from external attributes
    activityId: Option[String],
    activity: Option[String],
    activityGroupId: Option[String],
    activityGroup: Option[String],
    activityLocationId: Option[String],
    activityLocation: Option[String],

    //External assessments
    assessments: Option[Set[String]],
    assessmentQuestions: Option[Set[String]],
    assessmentAnswers: Option[Set[String]],

    //External reasons
    reasonId: Option[String],
    reason: Option[String],
    //Updated at
    updatedAt: DateTime = DateTime.now()
)

object ChangeCaptureMessage {

  var support: Support = SupportImpl

  def create(json: JsObject): ChangeCaptureMessage = {

    ChangeCaptureMessage(
      personId = (json \ "personId").as[UUID],
      customerId = (json \ "customerId").as[Int],
      addressId = (json \ "addressId").asOpt[UUID],
      householdId = (json \ "householdId").asOpt[UUID],
      messageType = (json \ "messageType").as[String],
      source = (json \ "source").as[String],
      sourceType = (json \ "sourceType").as[String],
      personType = (json \ "personType").as[String],
      sourcePersonId = (json \ "sourcePersonId").as[String],
      sourceRecordId = (json \ "sourceRecordId").as[String],
      trackingDate = support.toDateTime((json \ "trackingDate").as[String]),
      firstName = (json \ "firstName").asOpt[String],
      middleName = (json \ "middleName").asOpt[String],
      lastName = (json \ "lastName").asOpt[String],
      prefix = (json \ "prefix").asOpt[String],
      personalSuffix = (json \ "personalSuffix").asOpt[String],
      dob = support.toDateTimeOpt((json \ "dob").asOpt[String]),
      age = (json \ "age").asOpt[Int],
      ageGroup = (json \ "ageGroup").asOpt[String],
      sex = (json \ "sex").asOpt[String],
      payerType = (json \ "payerType").asOpt[String],
      maritalStatus = (json \ "maritalStatus").asOpt[String],
      ethnicInsight = (json \ "ethnicInsight").asOpt[String],
      race = (json \ "race").asOpt[String],
      religion = (json \ "religion").asOpt[String],
      language = (json \ "language").asOpt[Int],
      occupationGroup = (json \ "occupationGroup").asOpt[String],
      occupation = (json \ "occupation").asOpt[String],
      phoneNumbers = (json \ "phoneNumbers").asOpt[List[String]],
      emails = (json \ "emails").asOpt[List[String]],
      dwellType = (json \ "dwellType").asOpt[String],
      combinedOwner = (json \ "combinedOwner").asOpt[String],
      householdIncome = (json \ "householdIncome").asOpt[String],
      recipientReliabilityCode = (json \ "recipientReliabilityCode").asOpt[Int],
      mailResponder = (json \ "mailResponder").asOpt[String],
      lengthOfResidence = (json \ "lengthOfResidence").asOpt[Int],
      personsInLivingUnit = (json \ "personsInLivingUnit").asOpt[Int],
      adultsInLivingUnit = (json \ "adultsInLivingUnit").asOpt[Int],
      childrenInLivingUnit = (json \ "childrenInLivingUnit").asOpt[Int],
      homeYearBuilt = (json \ "homeYearBuilt").asOpt[Int],
      homeLandValue = (json \ "homeLandValue").asOpt[Float],
      estimatedHomeValue = (json \ "estimatedHomeValue").asOpt[String],
      donatesToCharity = (json \ "donatesToCharity").asOpt[String],
      mosaicZip4 = (json \ "mosaicZip4").asOpt[String],
      mosaicGlobalZip4 = (json \ "mosaicGlobalZip4").asOpt[String],
      hhComp = (json \ "hhComp").asOpt[String],
      presenceOfChild = (json \ "presenceOfChild").asOpt[String],
      childZeroToThreeBkt = (json \ "childZeroToThreeBkt").asOpt[String],
      childFourToSixBkt = (json \ "childFourToSixBkt").asOpt[String],
      childSevenToNineBkt = (json \ "childSevenToNineBkt").asOpt[String],
      childTenToTwelveBkt = (json \ "childTenToTwelveBkt").asOpt[String],
      childThirteenToFifteenBkt = (json \ "childThirteenToFifteenBkt").asOpt[String],
      childSixteenToEighteenBkt = (json \ "childSixteenToEighteenBkt").asOpt[String],
      childAgeBuckets = (json \ "childAgeBuckets").asOpt[Set[String]],
      wealthRating = (json \ "wealthRating").asOpt[Int],
      addressQualityIndicator = (json \ "addressQualityIndicator").asOpt[String],
      education = (json \ "education").asOpt[String],
      addressType = (json \ "addressType").asOpt[String],
      validAddressFlag = (json \ "validAddressFlag").asOpt[Boolean],
      address1 = (json \ "address1").asOpt[String],
      address2 = (json \ "address2").asOpt[String],
      city = (json \ "city").asOpt[String],
      state = (json \ "state").asOpt[String],
      zip5 = (json \ "zip5").asOpt[String],
      zip4 = (json \ "zip4").asOpt[String],
      county = (json \ "county").asOpt[String],
      carrierRoute = (json \ "carrierRoute").asOpt[String],
      dpbc = (json \ "dpbc").asOpt[String],
      lat = (json \ "lat").asOpt[Float],
      lon = (json \ "lon").asOpt[Float],
      streetPreDir = (json \ "streetPreDir").asOpt[String],
      streetName = (json \ "streetName").asOpt[String],
      streetPostDir = (json \ "streetPostDir").asOpt[String],
      streetSuffix = (json \ "streetSuffix").asOpt[String],
      streetSecondNumber = (json \ "streetSecondNumber").asOpt[String],
      streetSecondUnit = (json \ "streetSecondUnit").asOpt[String],
      streetHouseNum = (json \ "streetHouseNum").asOpt[String],
      msa = (json \ "msa").asOpt[String],
      pmsa = (json \ "pmsa").asOpt[String],
      dpv = (json \ "dpv").asOpt[String],
      countyCode = (json \ "countyCode").asOpt[String],
      censusBlock = (json \ "censusBlock").asOpt[String],
      censusTract = (json \ "censusTract").asOpt[String],
      beehiveCluster = (json \ "beehiveCluster").asOpt[Int],
      primaryCarePhysician = (json \ "primaryCarePhysician").asOpt[Long],

      //Activity columns
      servicedOn = support.toDateTimeOpt((json \ "servicedOn").asOpt[String]),
      locationId = (json \ "locationId").asOpt[Int],
      activityType = (json \ "activityType").asOpt[String],
      mxCodes = (json \ "mxCodes").asOpt[List[String]],
      mxGroups = (json \ "mxGroups").asOpt[Set[Int]],
      providers = (json \ "providers").asOpt[Set[String]],
      erPatient = (json \ "erPatient").asOpt[Boolean],
      financialClassId = (json \ "financialClassId").asOpt[Int],
      financialClass = (json \ "financialClass").asOpt[String],
      serviceLines = (json \ "serviceLines").asOpt[Set[String]],
      patientType = (json \ "patientType").asOpt[String],
      dischargeStatus = (json \ "dischargeStatus").asOpt[Int],
      admittedAt = support.toDateTimeOpt((json \ "admittedAt").asOpt[String]),
      dischargedAt = support.toDateTimeOpt((json \ "dischargedAt").asOpt[String]),
      finalBillDate = support.toDateTimeOpt((json \ "finalBillDate").asOpt[String]),
      transactionDate = support.toDateTimeOpt((json \ "transactionDate").asOpt[String]),
      activityDate = support.toDateTimeOpt((json \ "activityDate").asOpt[String]),
      hospitalId = (json \ "hospitalId").asOpt[String],
      hospital = (json \ "hospital").asOpt[String],
      businessUnitId = (json \ "businessUnitId").asOpt[String],
      businessUnit = (json \ "businessUnit").asOpt[String],
      siteId = (json \ "siteId").asOpt[String],
      site = (json \ "site").asOpt[String],
      clinicId = (json \ "clinicId").asOpt[String],
      clinic = (json \ "clinic").asOpt[String],
      practiceLocationId = (json \ "practiceLocationId").asOpt[String],
      practiceLocation = (json \ "practiceLocation").asOpt[String],
      facilityId = (json \ "facilityId").asOpt[String],
      facility = (json \ "facility").asOpt[String],
      insuranceId = (json \ "insuranceId").asOpt[String],
      insurance = (json \ "insurance").asOpt[String],
      charges = (json \ "charges").asOpt[Double],
      cost = (json \ "cost").asOpt[Double],
      revenue = (json \ "revenue").asOpt[Double],
      contributionMargin = (json \ "contributionMargin").asOpt[Double],
      profit = (json \ "profit").asOpt[Double],
      systolic = (json \ "systolic").asOpt[Double],
      diastolic = (json \ "diastolic").asOpt[Double],
      height = (json \ "height").asOpt[Double],
      weight = (json \ "weight").asOpt[Double],
      bmi = (json \ "bmi").asOpt[Double],
      guarantorFirstName = (json \ "guarantorFirstName").asOpt[String],
      guarantorLastName = (json \ "guarantorLastName").asOpt[String],
      guarantorMiddleName = (json \ "guarantorMiddleName").asOpt[String],
      activityId = (json \ "activityId").asOpt[String],
      activity = (json \ "activity").asOpt[String],
      activityGroupId = (json \ "activityGroupId").asOpt[String],
      activityGroup = (json \ "activityGroup").asOpt[String],
      activityLocationId = (json \ "activityLocationId").asOpt[String],
      activityLocation = (json \ "activityLocation").asOpt[String],
      assessments = (json \ "assessments").asOpt[Set[String]],
      assessmentQuestions = (json \ "assessmentQuestions").asOpt[Set[String]],
      assessmentAnswers = (json \ "assessmentAnswers").asOpt[Set[String]],
      reasonId = (json \ "reasonId").asOpt[String],
      reason = (json \ "reason").asOpt[String])
  }

  def extractFieldNames[T<:Product:Manifest] = {
    implicitly[Manifest[T]].runtimeClass.getDeclaredFields.map(_.getName)
  }
}