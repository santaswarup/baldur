package com.influencehealth.baldur.identity_load.person_identity.identity.support

import com.influencehealth.baldur.identity_load.person_identity.support._

import play.api.libs.json._
import scala.math.Ordered.orderingToOrdered

case class SourceIdentity(customerId: Int, sourcePersonId: String,
  source: String, sourceType: String) {
  def compare(that: SourceIdentity): Int = (customerId, sourcePersonId, source, sourceType) compare (that.customerId, that.sourcePersonId, that.source, that.sourceType)
}

object SourceIdentity {
  var support: Support = SupportImpl

  def fromJson(obj: JsObject) = SourceIdentity(
    (obj \ "customerId").as[Int],
    (obj \ support.ExternalPersonIdField).as[String].toLowerCase,
    (obj \ "source").as[String].toLowerCase,
    (obj \ "sourceType").as[String].toLowerCase)

  implicit class ConvertToSourceIdentity(personIdentityColumns: PersonIdentityColumns) {
    def sourceIdentity = {
      SourceIdentity(
        personIdentityColumns.customerId,
        personIdentityColumns.sourcePersonId.toLowerCase,
        personIdentityColumns.source.toLowerCase,
        personIdentityColumns.sourceType.toLowerCase)
    }
  }
}
