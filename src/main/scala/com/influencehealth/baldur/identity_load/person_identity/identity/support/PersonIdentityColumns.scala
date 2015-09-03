package com.influencehealth.baldur.identity_load.person_identity.identity.support

import com.influencehealth.baldur.identity_load.person_identity.support._
import com.rockymadden.stringmetric.StringAlgorithm.Soundex
import java.util.UUID
import org.joda.time.DateTime

case class PersonIdentityColumns(customerId: Int,
  sourcePersonId: String,
  source: String,
  sourceType: String,
  address1: Option[String]=None,
  address2: Option[String]=None,
  streetSecondNumber: Option[String]=None,
  zip5: Option[String]=None,
  firstName: Option[String]=None,
  middleName: Option[String]=None,
  lastName: Option[String]=None,
  suffix: Option[String]=None,
  dob: Option[DateTime]=None,
  sex: Option[String]=None,
  var personId: Option[UUID]=None) {

  var support: Support = SupportImpl
  var rootFirstName: Option[String] = support.rootFirstName(firstName, sex)
 
  val soundexLastName: Option[String] = lastName match {
    case Some(name) => Soundex.compute(name)
    case None => None
  }

}


