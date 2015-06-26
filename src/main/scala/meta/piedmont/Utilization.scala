package meta.piedmont

import meta.ClientInputMeta

/**
 * Piedmont Utilization
 */
object Utilization extends ClientInputMeta with Piedmont {
  override def mapping(): Seq[Product] = wrapRefArray(Array(
    ("sourceId", "string"),
    ("patientId", "string"),
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
}
