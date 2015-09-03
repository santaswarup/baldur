package com.influencehealth.baldur.identity_load.person_identity.identity_table.support

import com.influencehealth.baldur.identity_load.person_identity.support._

import java.util.UUID
import com.rockymadden.stringmetric.StringAlgorithm._
import org.joda.time.DateTime

case class Identity2(
  customerId: Int,
  address1: String,
  zip5: String,
  soundexLastName: String,
  personId: UUID,
  rootFirstName: Option[String],
  streetSecondNumber: Option[String],
  dob: Option[DateTime],
  emails: Set[String],
  sex: Option[String],
  middleName: Option[String],
  mrids: Set[String],
  address2: Option[String],
  suffix: Option[String]
) extends IdentityTable

object Identity2 {
  var support: Support = SupportImpl

  def create(personIdentity: PersonIdentity) = {
    val soundexLastName: Option[String] = personIdentity.lastName match {
      case Some(name) => Soundex.compute(name)
      case None => None
    }

    Identity2(personIdentity.customerId, personIdentity.address1.get, personIdentity.zip5.get, soundexLastName.get,
      personIdentity.personId, support.rootFirstName(personIdentity.firstName, personIdentity.sex), personIdentity.streetSecondNumber, personIdentity.dob, personIdentity.emails.toSet, personIdentity.sex, personIdentity.middleName,
      personIdentity.mrids, personIdentity.address2, personIdentity.personalSuffix)
  }
}
