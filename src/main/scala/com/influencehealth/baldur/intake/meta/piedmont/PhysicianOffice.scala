package com.influencehealth.baldur.intake.meta.piedmont

import com.influencehealth.baldur.intake.meta._
import com.influencehealth.baldur.support._
import org.joda.time.DateTime

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
    ("lastName", "title"),
    ("firstName", "title"),
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

    val medicalCodes = getMedicalCodes(map)
    val codeGroups = FileInputSupport.getCodeGroups(medicalCodes)

    val zip5: Option[String] = FileInputSupport.containsHyphen(zipInput) match {
      case true => Some(zipInput.get.split("-")(0))
      case false => zipInput
    }

    val zip4: Option[String] = FileInputSupport.containsHyphen(zipInput) match {
      case true => Some(zipInput.get.split("-")(1))
      case false => None
    }

    val financialClass: (Option[Int], Option[String], Option[String]) = getFinancialClasses(map)
    val ageDob: (Option[DateTime], Option[Int], Option[String]) = FileInputSupport.getAgeDob(map, "age", "dob")

    ActivityOutput(
      //Person
      customerId = CustomerId,
      messageType = "utilization",
      source = "epic",
      sourceType = "physician office",
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
      age = ageDob._2,
      ageGroup = ageDob._3,
      dob = ageDob._1,
      emails = getEmails(map),
      phoneNumbers = getPhoneNumbers(map),

      locationId = getLocationIdFromUtil(map),
      dischargedAt = FileInputSupport.getDateOptValue(map, "dischargeDate"),
      servicedOn = FileInputSupport.getDateOptValue(map, "dischargeDate"),
      activityDate = FileInputSupport.getDateOptValue(map, "dischargeDate"),
      mxCodes = medicalCodes,
      mxGroups = codeGroups,
      patientType = Some("o"),
      erPatient = Some(false),
      financialClassId = financialClass._1,
      financialClass = financialClass._2,
      payerType = financialClass._3,

      insuranceId = FileInputSupport.getStringOptValue(map, "payorId"),
      insurance = FileInputSupport.getStringOptValue(map, "payorName"),

      facilityId = FileInputSupport.getStringOptValue(map, "facilityId"),
      facility = FileInputSupport.getStringOptValue(map, "facilityName"),
      businessUnitId = FileInputSupport.getStringOptValue(map, "departmentId"),
      businessUnit = FileInputSupport.getStringOptValue(map, "departmentName"),
      activityLocationId = FileInputSupport.getStringOptValue(map, "facilityId"),
      activityLocation = FileInputSupport.getStringOptValue(map, "facilityName")

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
      .map(FileInputSupport.cleanseMedicalCodes)
      .toList)
  }

}
