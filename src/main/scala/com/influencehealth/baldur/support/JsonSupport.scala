package com.influencehealth.baldur.support

import com.influencehealth.baldur.identity_load.person_identity.change_capture.support._
import com.influencehealth.baldur.identity_load.person_identity.householding.support._
import com.influencehealth.baldur.identity_load.person_identity.identity.support._
import com.influencehealth.baldur.identity_load.person_identity.support.{SupportImpl, Support}

import org.joda.time.DateTime
import org.joda.time.format._
import play.api.libs.functional.syntax._
import play.api.libs.json._
import java.util.UUID

object JsonSupport {
  var support: Support = SupportImpl

  implicit val personIdentityColumnsReads: Reads[PersonIdentityColumns] = (
    (JsPath \ "customerId").read[Int] and
    (JsPath \ "sourcePersonId").read[String] and
    (JsPath \ "source").read[String] and
    (JsPath \ "sourceType").read[String] and
    (JsPath \ "address1").readNullable[String] and
    (JsPath \ "address2").readNullable[String] and
    (JsPath \ "addressSecondaryNumber").readNullable[String] and
    (JsPath \ "zip5").readNullable[String] and
    (JsPath \ "firstName").readNullable[String] and
    (JsPath \ "middleName").readNullable[String] and
    (JsPath \ "lastName").readNullable[String] and
    (JsPath \ "suffix").readNullable[String] and
    (JsPath \ "dob").readNullable[String].map[Option[DateTime]](support.toDateTimeOpt) and
    (JsPath \ "sex").readNullable[String] and
    (JsPath \ "personId").readNullable[UUID] 
  )(PersonIdentityColumns.apply _)

  implicit val columnChangeReads = (
    (__ \ "customerId").read[Int] ~
      (__ \ "personId").read[UUID] ~
      (__ \ "columnName").read[String] ~
      (__ \ "oldValue").read[Option[String]] ~
      (__ \ "newValue").read[String] ~
      (__ \ "source").read[String] ~
      (__ \ "sourceType").read[String] ~
      (__ \ "sourceRecordId").read[String] ~
      (__ \ "trackingDate").read[String].map[DateTime](support.toDateTime) ~
      (__ \ "updatedAt").read[String].map[DateTime](support.toDateTime)
  )(ColumnChange.apply _)

  implicit val householdAddressColumnReads: Reads[HouseholdAddress] = (
    (__ \ "personId").read[UUID] and
      (__ \ "customerId").read[Int] and
      (__ \ "address1").readNullable[String] and
      (__ \ "address2").readNullable[String] and
      (__ \ "city").readNullable[String] and
      (__ \ "state").readNullable[String] and
      (__ \ "zip5").readNullable[String] and
      (__ \ "zip4").readNullable[String] and
      (__ \ "lat").readNullable[Float] and
      (__ \ "lon").readNullable[Float] and
      (__ \ "validAddressFlag").readNullable[Boolean] and
      (__ \ "lastName").readNullable[String] and
      (__ \ "addressId").readNullable[UUID] and
      (__ \ "householdId").readNullable[UUID]
    )(HouseholdAddress.apply _)

  implicit val columnChangeWrites = (
    (__ \ "customerId").write[Int] ~
      (__ \ "personId").write[UUID] ~
      (__ \ "columnName").write[String] ~
      (__ \ "oldValue").write[Option[String]] ~
      (__ \ "newValue").write[String] ~
      (__ \ "source").write[String] ~
      (__ \ "sourceType").write[String] ~
      (__ \ "sourceRecordId").write[String] ~
      (__ \ "trackingDate").write[DateTime] ~
      (__ \ "updatedAt").write[DateTime]
    )(unlift(ColumnChange.unapply))

}