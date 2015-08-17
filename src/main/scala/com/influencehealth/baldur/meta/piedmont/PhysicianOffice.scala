package com.influencehealth.baldur.meta.piedmont

import com.influencehealth.baldur.meta._

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
    val zipInput = getStringOptValue(map, "zip5")

    val zip5: Option[String] = containsHyphen(zipInput) match {
      case true => Some(zipInput.get.split("-")(0))
      case false => zipInput
    }

    val zip4: Option[String] = containsHyphen(zipInput) match {
      case true => Some(zipInput.get.split("-")(1))
      case false => zipInput
    }

    val financialClass: (Option[Int], Option[String]) = getFinancialClasses(map)

    ActivityOutput(
      //Person
      customerId = CustomerId,
      messageType = "utilization",
      source = "epic",
      sourceType = "physician office",
      personType = "c",
      activityType = Some("encounter"),
      sourcePersonId = getStringValue(map, "sourcePersonId"),
      sourceRecordId = getStringValue(map, "sourceRecordId"),
      trackingDate = getDateValue(map, "dischargeDate"),
      firstName = getStringOptValue(map, "firstName"),
      lastName = getStringOptValue(map, "lastName"),
      address1 = getStringOptValue(map, "address1"),
      city = getStringOptValue(map, "city"),
      state = getStringOptValue(map, "state"),
      zip5 = zip5,
      zip4 = zip4,
      sex = getStringOptValue(map, "sex"),
      age = getIntOptValue(map, "age"),
      dob = getDateOptValue(map, "dob"),
      emails = getEmails(map),
      phoneNumbers = getPhoneNumbers(map),

      locationId = getLocationIdFromUtil(map),
      dischargedAt = getDateOptValue(map, "dischargeDate"),
      servicedOn = getDateOptValue(map, "dischargeDate"),
      mxCodes = getMedicalCodes(map),
      patientType = Some("o"),
      erPatient = Some(false),
      financialClassId = financialClass._1,
      financialClass = financialClass._2,

      insuranceId = getStringOptValue(map, "payorId"),
      insurance = getStringOptValue(map, "payorName"),

      facilityId = getStringOptValue(map, "facilityId"),
      facility = getStringOptValue(map, "facilityName"),
      businessUnitId = getStringOptValue(map, "departmentId"),
      businessUnit = getStringOptValue(map, "departmentName")

    )
  }



  def getMedicalCodes(map: Map[String, Any]): Option[List[String]] = {
    val primaryDxId: Option[String] = getMedicalCodeString(map, "primaryDxId", "icd9_diag", "|")
    val dx2: Option[String] = getMedicalCodeString(map, "dxTwoId", "icd9_diag", "|")
    val dx3: Option[String] = getMedicalCodeString(map, "dxThreeId", "icd9_diag", "|")
    val dx4: Option[String] = getMedicalCodeString(map, "dxFourId", "icd9_diag", "|")
    val dx5: Option[String] = getMedicalCodeString(map, "dxFiveId", "icd9_diag", "|")
    val dx6: Option[String] = getMedicalCodeString(map, "dxSixId", "icd9_diag", "|")

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
