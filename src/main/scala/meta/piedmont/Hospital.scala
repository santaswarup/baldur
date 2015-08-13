package meta.piedmont

import meta.{ActivityOutput, ClientInputMeta}

/**
 * Piedmont Hospital
 */
object Hospital extends ClientInputMeta with Piedmont with Serializable {
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

    val financialClass: (Option[Int], Option[String]) = getFinancialClasses(map)

    ActivityOutput(
      //Person
      customerId = 1,
      messageType = "utilization",
      source = "epic",
      sourceType = "hospital",
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
      patientType = getPatientType(map),
      erPatient = getErFlag(map),
      financialClassId = financialClass._1,
      financialClass = financialClass._2,

      insuranceId = getStringOptValue(map, "payorId"),
      insurance = getStringOptValue(map, "payorName"),

      facilityId = getStringOptValue(map, "facilityId"),
      facility = getStringOptValue(map, "facility"),
      businessUnitId = getStringOptValue(map, "departmentId"),
      businessUnit = getStringOptValue(map, "departmentName")

    )
  }



  def getMedicalCodes(map: Map[String, Any]): Option[List[String]] = {
    val primaryDxId: Option[String] = getMedicalCodeString(map, "primaryDxId", "icd9_diag", "|")
    val otherDxIds: Option[String] = getMedicalCodeString(map, "secondaryDxIds", "icd9_diag", "|")
    val primaryCpt: Option[String] = getMedicalCodeString(map, "primaryProcedureId", "cpt", "|")
    val otherCpts: Option[String] = getMedicalCodeString(map, "primaryProcedureId", "cpt", "|")
    val msDrg: Option[String] = getMedicalCodeString(map, "finalDrgCd", "ms_drg", "|")

    concatonateMedicalCodes(msDrg, primaryCpt, otherCpts, primaryDxId, otherDxIds)

  }

  def concatonateMedicalCodes(msDrg: Option[String],
                              primaryCpt: Option[String],
                              otherCpts: Option[String],
                              primaryDxId: Option[String],
                              otherDxIds: Option[String]): Option[List[String]] ={
    Some(Seq(msDrg, primaryCpt, otherCpts, primaryDxId, otherDxIds)
      .filter(_.nonEmpty)
      .mkString(",")
      .split(",")
      .toList)
  }

  def getPatientType(map: Map[String, Any]): Option[String] ={
    map
      .filter { case (key, value) => key.equals("patientTypeShort") }
      .map { case (key, value) => value }
    .map { case value => value match{
        case None => None
        case "IP" => Some("i")
        case "ED" => Some("o")
      }}
      .head
  }

  def getErFlag(map: Map[String, Any]): Option[Boolean] ={
    map
      .filter { case (key, value) => key.equals("patientTypeShort") }
      .map { case (key, value) => value }
      .map { case value => value match{
      case None => None
      case "IP" => Some(false)
      case "ED" => Some(true)
    }}
      .head
  }

}