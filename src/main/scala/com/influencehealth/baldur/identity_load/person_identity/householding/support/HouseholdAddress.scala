package com.influencehealth.baldur.identity_load.person_identity.householding.support

import java.util.UUID
import com.influencehealth.baldur.identity_load.person_identity.support.{SupportImpl, Support}
import play.api.libs.json.JsObject

case class HouseholdAddress(personId: UUID,
                                    customerId: Int,
                                    address1: Option[String],
                                    address2: Option[String],
                                    city: Option[String],
                                    state: Option[String],
                                    zip5: Option[String],
                                    zip4: Option[String],
                                    lat: Option[Float],
                                    lon: Option[Float],
                                    validAddressFlag: Option[Boolean]=Some(false),
                                    lastName: Option[String],
                                    var addressId: Option[UUID]=None,
                                    var householdId: Option[UUID]=None){
  def hasAddressColumns = address1.isDefined && city.isDefined && state.isDefined && zip5.isDefined && zip4.isDefined && lat.isDefined && lon.isDefined && validAddressFlag.get
  def hasHouseholdColumns = addressId.isDefined && lastName.isDefined
}


object HouseholdAddress {
  var support: Support = SupportImpl
  def create(jsObj: JsObject): HouseholdAddress =

      HouseholdAddress(
        (jsObj \ "personId").as[UUID],
        (jsObj \ "customerId").as[Int],
        support.toLowerCaseOption((jsObj \ "address1").asOpt[String]),
        support.toLowerCaseOption((jsObj \ "address2").asOpt[String]),
        support.toLowerCaseOption((jsObj \ "city").asOpt[String]),
        support.toLowerCaseOption((jsObj \ "state").asOpt[String]),
        (jsObj \ "zip5").asOpt[String],
        (jsObj \ "zip4").asOpt[String],
        (jsObj \ "lat").asOpt[Float],
        (jsObj \ "lon").asOpt[Float],
        (jsObj \ "validAddressFlag").asOpt[Boolean],
        support.toLowerCaseOption((jsObj \ "lastName").asOpt[String])
  )

}