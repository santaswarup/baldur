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

      locationId = getLocationId(map),
      dischargedAt = getDateOptValue(map, "dischargeDate"),
      mxCodes = getMedicalCodes(map),
      patientType = getPatientType(map)
    )
  }

  def containsHyphen(str: Option[String]): Boolean = {
    str match {
      case None => false
      case Some(x) => Some(x).get.contains("-")
    }
  }

  def getMedicalCodes(map: Map[String, Any]): Option[List[String]] = {
    val primaryDxId: Option[String] = getMedicalCodeString(map, "primaryDxId", "icd9_diag", "|")
    val otherDxIds: Option[String] = getMedicalCodeString(map, "secondaryDxIds", "icd9_diag", "|")
    val primaryCpt: Option[String] = getMedicalCodeString(map, "primaryProcedureId", "cpt", "|")
    val otherCpts: Option[String] = getMedicalCodeString(map, "primaryProcedureId", "cpt", "|")
    val msDrg: Option[String] = getMedicalCodeString(map, "finalDrgCd", "ms_drg", "|")

    concatonateMedicalCodes(msDrg, primaryCpt, otherCpts, primaryDxId, otherDxIds)

  }

  def getLocationId(map: Map[String, Any]): Option[Int] = {
    map
      .filter { case (key, value) => key.equals("facilityId") }
      .map { case (key, value) => value }
      .map(mapLocationId)
      .head
  }

  def mapLocationId(value: Any): Option[Int] = {
    value match {
      case 10500 => Some(600) // Atlanta
      case 10501 => Some(601) // Fayette
      case 10502 => Some(602) // Mountainside
      case 10503 => Some(603) // Newnan
      case 10504 => Some(604) // Henry
      case _ => None
    }
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

  def getMedicalCodeString(map: Map[String, Any], columnName: String, codeType: String, delimiter: String = ","): Option[String] = {
    map
      .filter { case (key, value) => key.equals(columnName) }
      .map{case x => mapMedicalCode(x, codeType, delimiter)}
      .head
  }

  def mapMedicalCode(value: Any, codeType: String, delimiter: String): Option[String] = {
    value match {
      case None => None
      case value: String => Some(value.replace(delimiter, ";" + getCodeType(codeType) + ",") + ";" + getCodeType(codeType))
    }
  }

  def getPatientType(map: Map[String, Any]): Option[String] ={
    Some(map
      .filter { case (key, value) => key.equals("patientTypeShort") }
      .map { case (key, value) => value }
    .map { case value => value match{
        case None => None
        case "IP" => "i"
        case "OP" => "o"
      }}
      .head
      .toString)
  }

}