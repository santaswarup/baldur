package com.influencehealth.baldur.identity_load.person_identity.identity_table.support

import com.influencehealth.baldur.identity_load.person_identity.support._

import java.util.UUID
import com.rockymadden.stringmetric.StringAlgorithm._
import org.joda.time.DateTime

case class Identity4(
  customerId: Int,
  zip5: String,
  rootFirstName: String,
  soundexLastName: String,
  mrid: String,
  personId: UUID,
  address1: Option[String],
  streetSecondNumber: Option[String],
  dob: Option[DateTime],
  emails: Set[String],
  sex: Option[String],
  middleName: Option[String],
  mrids: Set[String],
  address2: Option[String],
  suffix: Option[String]
) extends IdentityTable

object Identity4 {
  var support: Support = SupportImpl

  def create(personIdentity: PersonIdentity): Iterable[Identity4] = {
    val soundexLastName: Option[String] = personIdentity.lastName match {
      case Some(name) => Soundex.compute(name)
      case None => None
    }

    personIdentity.mrids.map { mrid =>
      Identity4(personIdentity.customerId,  personIdentity.zip5.get, support.rootFirstName(personIdentity.firstName, personIdentity.sex).get, soundexLastName.get,
        mrid, personIdentity.personId, personIdentity.address1, personIdentity.streetSecondNumber, personIdentity.dob, personIdentity.emails.toSet, personIdentity.sex, personIdentity.middleName,
        personIdentity.mrids, personIdentity.address2, personIdentity.personalSuffix)
    }
  }
}
