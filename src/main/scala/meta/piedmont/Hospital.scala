package meta.piedmont

import meta.{ActivityOutput, ClientInputMeta}

/**
 * Piedmont Hospital
 */
object Hospital extends ClientInputMeta with Piedmont {
  override def originalFields(): Seq[Product] = wrapRefArray(Array(
    ("sourceRecordId", "string"),
    ("sourcePersonId", "string"),
    ("facilityId", "string"),
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
    ("payorId", "string"),
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

    val zipInput = getStringOptValue(map, "zip5")

    val zip5: Option[String] = containsHyphen(zipInput) match {
      case true => Some(zipInput.get.split("-")(0))
      case false => zipInput
    }

    val zip4: Option[String] = containsHyphen(zipInput) match {
      case true => Some(zipInput.get.split("-")(1))
      case false => zipInput
    }

    ActivityOutput(
      //Person
      customerId = 1,
      messageType = "utilization",
      source = "epic",
      sourceType = "hospital",
      personType = "c",
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
      emails = getListOptValue(map, "patientEmail"),
      phoneNumbers = getListOptValue(map, "homePhone"),

      dischargedAt = getDateOptValue(map, "dischargeDate")
    )
  }

  def containsHyphen(str: Option[String]): Boolean = {
    str match {
      case None => false
      case Some(x) => Some(x).get.contains("-")
    }
  }
}