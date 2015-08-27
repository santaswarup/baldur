package com.influencehealth.baldur.identity_load.person_identity.identity.support

import com.influencehealth.baldur.identity_load.person_identity.support._

import java.util.UUID
import org.joda.time.DateTime

case class PersonIdentityColumns(customerId: Int,
  recordId: String,
  externalPersonId: String,
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
  mrn: Option[String],
  dob: Option[DateTime],
  sex: Option[String],
  email: Option[String],
  var personId: Option[UUID]=None) {

  var support: Support = SupportImpl
  var rootFirstName: Option[String] = support.rootFirstName(firstName, sex)
 
  val soundexLastName: Option[String] = lastName match {
    case Some(lastName) => Some(soundex(lastName))
    case None => None
  }

  def soundex(originalName: String): String = {
    /*RefinedSoundexAlgorithm.compute(originalName) match {
      case Some(name) => name
      case None => originalName
    }*/
    originalName.toLowerCase
  }

}


