package meta.piedmont

import meta.ClientInputMeta

/**
 * Piedmont Utilization
 */
object Utilization extends ClientInputMeta with Piedmont {
  override def mapping(): Seq[Product] = wrapRefArray(Array(
    ("hospital_account_id", "string"),
    ("patient_id", "string"),
    ("facility_id", "string"),
    ("facility_name", "string"),
    ("facility_address", "string"),
    ("facility_city", "string"),
    ("facility_state", "string"),
    ("facility_zip", "string"),
    ("last_name", "string"),
    ("first_name", "string"),
    ("patient_address", "string"),
    ("patient_city", "string"),
    ("patient_state", "string"),
    ("patient_zip", "string"),
    ("sex", "string"),
    ("sex_description", "string"),
    ("age", "int"),
    ("dob", "date", "dd/MM/yyyy"),
    ("home_phone", "string"),
    ("birth_year", "int"),
    ("birth_day", "int"),
    ("birth_month", "string"),
    ("discharge_date", "date", "dd/MM/yyyy"),
    ("payor_id", "string"),
    ("payor_name", "string"),
    ("patient_type", "string"),
    ("patient_type_short", "string"),
    ("visit_type", "string"),
    ("department_id", "string"),
    ("department_name", "string"),
    ("patient_email", "string"),
    ("patient_id_extra", "string"),
    ("primary_dx_id", "string"),
    ("primary_dx_description", "string"),
    ("secondary_dx_ids", "string"),
    ("primary_procedure_id", "string"),
    ("secondary_procedures", "string"),
    ("final_drg_cd", "string")))
}
