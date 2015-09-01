package com.influencehealth.baldur.identity_load.person_identity.identity.support

import com.influencehealth.baldur.identity_load.person_identity.support._
import org.joda.time.DateTime

import play.api.libs.json._

case class SourceIdentityUntrusted(customerId: Int,
  sourcePersonId: String,
  source: String,
  sourceType: String,
  address1: Option[String],
  address2: Option[String],
  streetSecondNumber: Option[String],
  zip5: Option[String],
  firstName: Option[String],
  middleName: Option[String],
  lastName: Option[String],
  suffix: Option[String],
  dob: Option[DateTime],
  sex: Option[String])

object SourceIdentityUntrusted {
  var support: Support = SupportImpl

  def fromJson(obj: JsObject) = SourceIdentityUntrusted(
    (obj \ "customerId").as[Int],
    (obj \ support.ExternalPersonIdField).as[String],
    (obj \ "source").as[String],
    (obj \ "sourceType").as[String],
    (obj \ "address1").asOpt[String],
    (obj \ "address2").asOpt[String],
    (obj \ "streetSecondNumber").asOpt[String],
    (obj \ "zip5").asOpt[String],
    (obj \ "firstName").asOpt[String],
    (obj \ "middleName").asOpt[String],
    (obj \ "lastName").asOpt[String],
    (obj \ "suffix").asOpt[String],
    (obj \ "dob").asOpt[DateTime],
    (obj \ "sex").asOpt[String])

  implicit class ConvertToSourceIdentityUntrusted(personIdentityColumns: PersonIdentityColumns) {
    def sourceIdentityUntrusted = {
      SourceIdentityUntrusted(personIdentityColumns.customerId,
        personIdentityColumns.sourcePersonId,
        personIdentityColumns.source,
        personIdentityColumns.sourceType,
        personIdentityColumns.address1,
        personIdentityColumns.address2,
        personIdentityColumns.streetSecondNumber,
        personIdentityColumns.zip5,
        personIdentityColumns.firstName,
        personIdentityColumns.middleName,
        personIdentityColumns.lastName,
        personIdentityColumns.suffix,
        personIdentityColumns.dob,
        personIdentityColumns.sex)
    }
  }
}
