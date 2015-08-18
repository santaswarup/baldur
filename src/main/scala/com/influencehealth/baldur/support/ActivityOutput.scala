package com.influencehealth.baldur.support

import java.util.UUID

import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import play.api.libs.json._

/**
 * Defines the schema for the output message
 */
case class ActivityOutput(//person-columns
    personId: Option[UUID]=None,
    customerId: Int,
    addressId: Option[UUID]=None,
    householdId: Option[UUID]=None,
    messageType: String,
    source: String,
    sourceType: String,
    personType: String,
    sourcePersonId: String,
    sourceRecordId: String,
    trackingDate: DateTime,
    firstName: Option[String]=None,
    middleName: Option[String]=None,
    lastName: Option[String]=None,
    prefix: Option[String]=None,
    personalSuffix: Option[String]=None,
    dob: Option[DateTime]=None,
    age: Option[Int]=None,
    sex: Option[String]=None,
    payerType: Option[String]=None,
    maritalStatus: Option[String]=None,
    ethnicInsight: Option[String]=None,
    race: Option[String]=None,
    religion: Option[String]=None,
    language: Option[String]=None,
    occupationGroup: Option[String]=None,
    occupation: Option[String]=None,
    phoneNumbers: Option[List[String]]=None,
    emails: Option[List[String]]=None,
    dwellType: Option[String]=None,
    combinedOwner: Option[String]=None,
    householdIncome: Option[String]=None,
    recipientReliabilityCode: Option[Int]=None,
    mailResponder: Option[String]=None,
    lengthOfResidence: Option[Int]=None,
    personsInLivingUnit: Option[Int]=None,
    adultsInLivingUnit: Option[Int]=None,
    childrenInLivingUnit: Option[Int]=None,
    homeYearBuilt: Option[Int]=None,
    homeLandValue: Option[Float]=None,
    estimatedHomeValue: Option[Float]=None,
    donatesToCharity: Option[String]=None,
    mosaicZip4: Option[String]=None,
    mosaicGlobalZip4: Option[String]=None,
    hhComp: Option[String]=None,
    presenceOfChild: Option[String]=None,
    childZeroToThreeBkt: Option[String]=None,
    childFourToSixBkt: Option[String]=None,
    childSevenToNineBkt: Option[String]=None,
    childTenToTwelveBkt: Option[String]=None,
    childThirteenToFifteenBkt: Option[String]=None,
    childSixteenToEighteenBkt: Option[String]=None,
    wealthRating: Option[Int]=None,
    addressQualityIndicator: Option[String]=None,
    addressType: Option[String]=None,
    validAddressFlag: Option[Boolean]=None,
    address1: Option[String]=None,
    address2: Option[String]=None,
    city: Option[String]=None,
    state: Option[String]=None,
    zip5: Option[String]=None,
    zip4: Option[String]=None,
    county: Option[String]=None,
    carrierRoute: Option[String]=None,
    dpbc: Option[String]=None,
    lat: Option[Float]=None,
    lon: Option[Float]=None,

    //activity columns
    servicedOn: Option[DateTime]=None,
    locationId: Option[Int]=None,
    activityType: Option[String]=None,
    mxCodes: Option[List[String]]=None,
    mxGroups: Option[Set[Int]]=None,
    providers: Option[Set[String]]=None,
    erPatient: Option[Boolean]=None,
    financialClassId: Option[Int]=None,
    financialClass: Option[String]=None,
    serviceLines: Option[Set[String]]=None,
    patientType: Option[String]=None,
    dischargeStatus: Option[Int]=None,

    //External dates
    admittedAt: Option[DateTime]=None,
    dischargedAt: Option[DateTime]=None,
    finalBillDate: Option[DateTime]=None,
    transactionDate: Option[DateTime]=None,
    activityDate: Option[DateTime]=None,

    //External locations
    hospitalId: Option[String]=None,
    hospital: Option[String]=None,
    businessUnitId: Option[String]=None,
    businessUnit: Option[String]=None,
    siteId: Option[String]=None,
    site: Option[String]=None,
    clinicId: Option[String]=None,
    clinic: Option[String]=None,
    practiceLocationId: Option[String]=None,
    practiceLocation: Option[String]=None,
    facilityId: Option[String]=None,
    facility: Option[String]=None,

    //External financials
    insuranceId: Option[String]=None,
    insurance: Option[String]=None,
    charges: Option[Double]=None,
    cost: Option[Double]=None,
    revenue: Option[Double]=None,
    contributionMargin: Option[Double]=None,
    profit: Option[Double]=None,

    //External biometrics
    systolic: Option[Double]=None,
    diastolic: Option[Double]=None,
    height: Option[Double]=None,
    weight: Option[Double]=None,
    bmi: Option[Double]=None,

    //External guarantor
    guarantorFirstName: Option[String]=None,
    guarantorLastName: Option[String]=None,
    guarantorMiddleName: Option[String]=None,

    //Activity fields derived from external attributes
    activityId: Option[String]=None,
    activity: Option[String]=None,
    activityGroupId: Option[String]=None,
    activityGroup: Option[String]=None,
    activityLocationId: Option[String]=None,
    activityLocation: Option[String]=None,

    //External assessments
    assessments: Option[Set[String]]=None,
    assessmentQuestions: Option[Set[String]]=None,
    assessmentAnswers: Option[Set[String]]=None,

    //External reasons
    reasonId: Option[String]=None,
    reason: Option[String]=None
) extends Serializable

object ActivityOutput {
  def mapJsonFields(activityOutput: ActivityOutput): Map[String, JsValue] = {
    Map(
      "personId" -> jsStringFromOptionOther(activityOutput.personId),
      "customerId" -> JsNumber(activityOutput.customerId),
      "addressId" -> jsStringFromOptionOther(activityOutput.addressId),
      "householdId" -> jsStringFromOptionOther(activityOutput.householdId),
      "messageType" -> JsString(activityOutput.messageType),
      "source" -> JsString(activityOutput.source),
      "sourceType" -> JsString(activityOutput.sourceType),
      "personType" -> JsString(activityOutput.personType),
      "sourcePersonId" -> JsString(activityOutput.sourcePersonId),
      "sourceRecordId" -> JsString(activityOutput.sourceRecordId),
      "trackingDate" -> jsDate(activityOutput.trackingDate),
      "firstName" -> jsStringFromOptionString(activityOutput.firstName),
      "middleName" -> jsStringFromOptionString(activityOutput.middleName),
      "lastName" -> jsStringFromOptionString(activityOutput.lastName),
      "prefix" -> jsStringFromOptionString(activityOutput.prefix),
      "personalSuffix" -> jsStringFromOptionString(activityOutput.personalSuffix),
      "dob" -> jsDateFromOption(activityOutput.dob),
      "age" -> jsNumberFromOptionInt(activityOutput.age),
      "sex" -> jsStringFromOptionString(activityOutput.sex),
      "payerType" -> jsStringFromOptionString(activityOutput.payerType),
      "maritalStatus" -> jsStringFromOptionString(activityOutput.maritalStatus),
      "ethnicInsight" -> jsStringFromOptionString(activityOutput.ethnicInsight),
      "race" -> jsStringFromOptionString(activityOutput.race),
      "religion" -> jsStringFromOptionString(activityOutput.religion),
      "language" -> jsStringFromOptionString(activityOutput.language),
      "occupationGroup" -> jsStringFromOptionString(activityOutput.occupationGroup),
      "occupation" -> jsStringFromOptionString(activityOutput.occupation),
      "phoneNumbers" -> jsArrayFromOptionListString(activityOutput.phoneNumbers),
      "emails" -> jsArrayFromOptionListString(activityOutput.emails),
      "dwellType" -> jsStringFromOptionString(activityOutput.dwellType),
      "combinedOwner" -> jsStringFromOptionString(activityOutput.combinedOwner),
      "householdIncome" -> jsStringFromOptionString(activityOutput.householdIncome),
      "recipientReliabilityCode" -> jsNumberFromOptionInt(activityOutput.recipientReliabilityCode),
      "mailResponder" -> jsStringFromOptionString(activityOutput.mailResponder),
      "lengthOfResidence" -> jsNumberFromOptionInt(activityOutput.lengthOfResidence),
      "personsInLivingUnit" -> jsNumberFromOptionInt(activityOutput.personsInLivingUnit),
      "adultsInLivingUnit" -> jsNumberFromOptionInt(activityOutput.adultsInLivingUnit),
      "childrenInLivingUnit" -> jsNumberFromOptionInt(activityOutput.childrenInLivingUnit),
      "homeYearBuilt" -> jsNumberFromOptionInt(activityOutput.homeYearBuilt),
      "homeLandValue" -> jsNumberFromOptionFloat(activityOutput.homeLandValue),
      "estimatedHomeValue" -> jsNumberFromOptionFloat(activityOutput.estimatedHomeValue),
      "donatesToCharity" -> jsStringFromOptionString(activityOutput.donatesToCharity),
      "mosaicZip4" -> jsStringFromOptionString(activityOutput.mosaicZip4),
      "mosaicGlobalZip4" -> jsStringFromOptionString(activityOutput.mosaicGlobalZip4),
      "hhComp" -> jsStringFromOptionString(activityOutput.hhComp),
      "presenceOfChild" -> jsStringFromOptionString(activityOutput.presenceOfChild),
      "childZeroToThreeBkt" -> jsStringFromOptionString(activityOutput.childZeroToThreeBkt),
      "childFourToSixBkt" -> jsStringFromOptionString(activityOutput.childFourToSixBkt),
      "childSevenToNineBkt" -> jsStringFromOptionString(activityOutput.childSevenToNineBkt),
      "childTenToTwelveBkt" -> jsStringFromOptionString(activityOutput.childTenToTwelveBkt),
      "childThirteenToFifteenBkt" -> jsStringFromOptionString(activityOutput.childThirteenToFifteenBkt),
      "childSixteenToEighteenBkt" -> jsStringFromOptionString(activityOutput.childSixteenToEighteenBkt),
      "wealthRating" -> jsNumberFromOptionInt(activityOutput.wealthRating),
      "addressQualityIndicator" -> jsStringFromOptionString(activityOutput.addressQualityIndicator),
      "addressType" -> jsStringFromOptionString(activityOutput.addressType),
      "validAddressFlag" -> jsBoolFromOption(activityOutput.validAddressFlag),
      "address1" -> jsStringFromOptionString(activityOutput.address1),
      "address2" -> jsStringFromOptionString(activityOutput.address2),
      "city" -> jsStringFromOptionString(activityOutput.city),
      "state" -> jsStringFromOptionString(activityOutput.state),
      "zip5" -> jsStringFromOptionString(activityOutput.zip5),
      "zip4" -> jsStringFromOptionString(activityOutput.zip4),
      "county" -> jsStringFromOptionString(activityOutput.county),
      "carrierRoute" -> jsStringFromOptionString(activityOutput.carrierRoute),
      "dpbc" -> jsStringFromOptionString(activityOutput.dpbc),
      "lat" -> jsNumberFromOptionFloat(activityOutput.lat),
      "lon" -> jsNumberFromOptionFloat(activityOutput.lon),

      "servicedOn" -> jsDateFromOption(activityOutput.servicedOn),
      "locationId" -> jsNumberFromOptionInt(activityOutput.locationId),
      "activityType" -> jsStringFromOptionString(activityOutput.activityType),
      "mxCodes" -> jsArrayFromOptionListString(activityOutput.mxCodes),
      "mxGroups" -> jsArrayFromOptionSetNum(activityOutput.mxGroups),
      "providers" -> jsArrayFromOptionSetString(activityOutput.providers),
      "erPatient" -> jsBoolFromOption(activityOutput.erPatient),
      "financialClassId" -> jsNumberFromOptionInt(activityOutput.financialClassId),
      "financialClass" -> jsStringFromOptionString(activityOutput.financialClass),
      "serviceLines" -> jsArrayFromOptionSetString(activityOutput.serviceLines),
      "patientType" -> jsStringFromOptionString(activityOutput.patientType),
      "dischargeStatus" -> jsNumberFromOptionInt(activityOutput.dischargeStatus),

      "admittedAt" -> jsDateFromOption(activityOutput.admittedAt),
      "dischargedAt" -> jsDateFromOption(activityOutput.dischargedAt),
      "finalBillDate" -> jsDateFromOption(activityOutput.finalBillDate),
      "transactionDate" -> jsDateFromOption(activityOutput.transactionDate),
      "activityDate" -> jsDateFromOption(activityOutput.activityDate),

      "hospitalId" -> jsStringFromOptionString(activityOutput.hospitalId),
      "hospital" -> jsStringFromOptionString(activityOutput.hospital),
      "businessUnitId" -> jsStringFromOptionString(activityOutput.businessUnitId),
      "businessUnit" -> jsStringFromOptionString(activityOutput.businessUnit),
      "siteId" -> jsStringFromOptionString(activityOutput.siteId),
      "site" -> jsStringFromOptionString(activityOutput.site),
      "clinicId" -> jsStringFromOptionString(activityOutput.clinicId),
      "clinic" -> jsStringFromOptionString(activityOutput.clinic),
      "practiceLocationId" -> jsStringFromOptionString(activityOutput.practiceLocationId),
      "practiceLocation" -> jsStringFromOptionString(activityOutput.practiceLocation),
      "facilityId" -> jsStringFromOptionString(activityOutput.facilityId),
      "facility" -> jsStringFromOptionString(activityOutput.facility),

      "insuranceId" -> jsStringFromOptionString(activityOutput.insuranceId),
      "insurance" -> jsStringFromOptionString(activityOutput.insurance),
      "charges" -> jsNumberFromOptionDouble(activityOutput.charges),
      "cost" -> jsNumberFromOptionDouble(activityOutput.cost),
      "revenue" -> jsNumberFromOptionDouble(activityOutput.revenue),
      "contributionMargin" -> jsNumberFromOptionDouble(activityOutput.contributionMargin),
      "profit" -> jsNumberFromOptionDouble(activityOutput.profit),

      "systolic" -> jsNumberFromOptionDouble(activityOutput.systolic),
      "diastolic" -> jsNumberFromOptionDouble(activityOutput.diastolic),
      "height" -> jsNumberFromOptionDouble(activityOutput.height),
      "weight" -> jsNumberFromOptionDouble(activityOutput.weight),
      "bmi" -> jsNumberFromOptionDouble(activityOutput.bmi),

      "guarantorFirstName" -> jsStringFromOptionString(activityOutput.guarantorFirstName),
      "guarantorLastName" -> jsStringFromOptionString(activityOutput.guarantorLastName),
      "guarantorMiddleName" -> jsStringFromOptionString(activityOutput.guarantorMiddleName),

      "activityId" -> jsStringFromOptionString(activityOutput.activityId),
      "activity" -> jsStringFromOptionString(activityOutput.activity),
      "activityGroupId" -> jsStringFromOptionString(activityOutput.activityGroupId),
      "activityGroup" -> jsStringFromOptionString(activityOutput.activityGroup),
      "activityLocationId" -> jsStringFromOptionString(activityOutput.activityLocationId),
      "activityLocation" -> jsStringFromOptionString(activityOutput.activityLocation),

      "assessments" -> jsArrayFromOptionSetString(activityOutput.assessments),
      "assessmentQuestions" -> jsArrayFromOptionSetString(activityOutput.assessmentQuestions),
      "assessmentAnswers" -> jsArrayFromOptionSetString(activityOutput.assessmentAnswers),

      "reasonId" -> jsStringFromOptionString(activityOutput.reasonId),
      "reason" -> jsStringFromOptionString(activityOutput.reason)

    )
  }

  def toStringFromActivity(headValue: Any): String = {
      headValue match{
        case headValue: Option[_] => headValue.isDefined match {
          case true => headValue.get match{
            case value: List[_] => value.mkString(",")
            case value: Set[_] => value.mkString(",")
            case value: DateTime => ISODateTimeFormat.basicDate().print(value)
            case _ => headValue.get.toString
          }
          case false => ""
        }
        case _ => headValue.toString
      }
  }

  def jsBoolFromOption(value: Option[Boolean]): JsValue = {
    value match {
      case None => JsNull
      case _ => JsBoolean(value.get)
    }
  }

  def jsDate(value: DateTime): JsValue = {
    JsString(ISODateTimeFormat.basicDate().print(value))
  }

  def jsDateFromOption(value: Option[DateTime]): JsValue = {
    value match {
      case None => JsNull
      case _ => JsString(ISODateTimeFormat.basicDate().print(value.get))
    }
  }

  def jsStringFromOptionOther(value: Option[Any]): JsValue = {
    value match {
      case None => JsNull
      case _ => JsString(value.get.toString)
    }
  }

  def jsStringFromOptionString(value: Option[String]): JsValue = {
    value match {
      case None => JsNull
      case _ => JsString(value.get)
    }
  }

  def jsNumberFromOptionFloat(value: Option[Float]): JsValue = {
    value match {
      case None => JsNull
      case _ => JsNumber(BigDecimal.valueOf(value.get.toDouble))
    }
  }

  def jsNumberFromOptionInt(value: Option[Int]): JsValue = {
    value match {
      case None => JsNull
      case _ => JsNumber(value.get)
    }
  }

  def jsNumberFromOptionDouble(value: Option[Double]): JsValue = {
    value match {
      case None => JsNull
      case _ => JsNumber(value.get)
    }
  }

  def jsNumberFromOptionLong(value: Option[Long]): JsValue = {
    value match {
      case None => JsNull
      case _ => JsNumber(value.get)
    }
  }

  def jsArrayFromOptionListString(value: Option[List[String]]): JsValue = {
    value match {
      case None => JsNull
      case _ => JsArray(value.get.map { case x => JsString(x) }.toSeq)
    }
  }

  def jsArrayFromOptionSetString(value: Option[Set[String]]): JsValue = {
    value match {
      case None => JsNull
      case _ => JsArray(value.get.map { case x => JsString(x) }.toSeq)
    }
  }

  def jsArrayFromOptionListNum(value: Option[List[Int]]): JsValue = {
    value match {
      case None => JsNull
      case _ => JsArray(value.get.map { case x => JsNumber(x) }.toSeq)
    }
  }

  def jsArrayFromOptionSetNum(value: Option[Set[Int]]): JsValue = {
    value match {
      case None => JsNull
      case _ => JsArray(value.get.map { case x => JsNumber(x) }.toSeq)
    }
  }
}