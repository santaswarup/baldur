package com.influencehealth.baldur.intake.meta.piedmont

import com.influencehealth.baldur.intake.meta._
import com.influencehealth.baldur.support._

/**
 * Piedmont Hospital
 */
object Hospital extends ClientInputMeta with Piedmont with Serializable {
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
    ("patientTypeShort", "string"),
    ("visitType", "string"),
    ("departmentId", "string"),
    ("departmentName", "string"),
    ("patientEmail", "string"),
    ("patientIdExtra", "string"),
    ("primaryDxId", "string"),
    ("primaryDxDescription", "string"),
    ("secondaryDxIds", "string"),
    ("primaryProcedureId", "string"),
    ("secondaryProcedures", "string"),
    ("finalDrgCd", "string")))

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
      //Person
      customerId = CustomerId,
      messageType = "utilization",
      source = "epic",
      sourceType = "hospital",
      personType = "c",
      activityType = Some("encounter"),
      sourcePersonId = FileInputSupport.getStringValue(map, "sourcePersonId"),
      sourceRecordId = FileInputSupport.getStringValue(map, "sourceRecordId"),
      trackingDate = FileInputSupport.getDateValue(map, "dischargeDate"),
      firstName = FileInputSupport.getStringOptValue(map, "firstName"),
      lastName = FileInputSupport.getStringOptValue(map, "lastName"),
      addressType = Some("home"),
      address1 = FileInputSupport.getStringOptValue(map, "address1"),
      city = FileInputSupport.getStringOptValue(map, "city"),
      state = FileInputSupport.getStringOptValue(map, "state"),
      zip5 = zip5,
      zip4 = zip4,
      sex = FileInputSupport.getStringOptValue(map, "sex"),
      age = FileInputSupport.getIntOptValue(map, "age"),
      dob = FileInputSupport.getDateOptValue(map, "dob"),
      emails = getEmails(map),
      phoneNumbers = getPhoneNumbers(map),

      locationId = getLocationIdFromUtil(map),
      dischargedAt = FileInputSupport.getDateOptValue(map, "dischargeDate"),
      servicedOn = FileInputSupport.getDateOptValue(map, "dischargeDate"),
      activityDate = FileInputSupport.getDateOptValue(map, "dischargeDate"),
      mxCodes = getMedicalCodes(map),
      patientType = getPatientType(map),
      erPatient = getErFlag(map),
      financialClassId = financialClass._1,
      financialClass = financialClass._2,

      insuranceId = FileInputSupport.getStringOptValue(map, "payorId"),
      insurance = FileInputSupport.getStringOptValue(map, "payorName"),

      facilityId = FileInputSupport.getStringOptValue(map, "facilityId"),
      facility = FileInputSupport.getStringOptValue(map, "facilityName"),
      businessUnitId = FileInputSupport.getStringOptValue(map, "departmentId"),
      businessUnit = FileInputSupport.getStringOptValue(map, "departmentName")

    )
  }



  def getMedicalCodes(map: Map[String, Any]): Option[List[String]] = {
    val primaryDxId: Option[String] = FileInputSupport.getMedicalCodeString(map, "primaryDxId", "icd9_diag", "|")
    val otherDxIds: Option[String] = FileInputSupport.getMedicalCodeString(map, "secondaryDxIds", "icd9_diag", "|")
    val primaryCpt: Option[String] = FileInputSupport.getMedicalCodeString(map, "primaryProcedureId", "cpt", "|")
    val otherCpts: Option[String] = FileInputSupport.getMedicalCodeString(map, "primaryProcedureId", "cpt", "|")
    val msDrg: Option[String] = FileInputSupport.getMedicalCodeString(map, "finalDrgCd", "ms_drg", "|")

    concatonateMedicalCodes(msDrg, primaryCpt, otherCpts, primaryDxId, otherDxIds)

  }

  def concatonateMedicalCodes(msDrg: Option[String],
                              primaryCpt: Option[String],
                              otherCpts: Option[String],
                              primaryDxId: Option[String],
                              otherDxIds: Option[String]): Option[List[String]] ={
    Some(Seq(msDrg, primaryCpt, otherCpts, primaryDxId, otherDxIds)
      .filter(_.nonEmpty)
      .map(_.get)
      .mkString(",")
      .split(",")
      .toList)
  }

  def getPatientType(map: Map[String, Any]): Option[String] ={
    val newMap =
    map
      .filter { case (key, value) => key.equals("patientTypeShort") }
      .map { case (key, value) => value }
    .map { case value => value match{
        case None => None
        case "IP" => Some("i")
        case "ED" => Some("o")
      }}

    newMap.nonEmpty match {
      case false => None
      case true => newMap.head
    }
  }

  def getErFlag(map: Map[String, Any]): Option[Boolean] ={
    val newMap =
    map
      .filter { case (key, value) => key.equals("patientTypeShort") }
      .map { case (key, value) => value }
      .map { case value => value match{
      case None => None
      case "IP" => Some(false)
      case "ED" => Some(true)
    }}

    newMap.nonEmpty match {
      case false => None
      case true => newMap.head
    }
  }

}