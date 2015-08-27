package com.influencehealth.baldur.identity_load.person_identity.identity_table.support

import java.util.UUID
import org.joda.time.DateTime

case class PersonIdentity(
  customerId: Int,
  personId: UUID,
  zip5: Option[String],
  firstName: Option[String],
  lastName: Option[String],
  streetSecondNumber: Option[String],
  dob: Option[DateTime],
  emails: List[String],
  sex: Option[String],
  middleName: Option[String],
  mrids: Set[String],
  address1: Option[String],
  address2: Option[String],
  personalSuffix: Option[String]) {

  def hasKey1 = firstName.isDefined && lastName.isDefined && address1.isDefined && zip5.isDefined

  def hasKey2 = lastName.isDefined && zip5.isDefined && address1.isDefined

  def hasKey3 = firstName.isDefined && zip5.isDefined && address1.isDefined

  def hasKey4 = firstName.isDefined && lastName.isDefined && zip5.isDefined && mrids.nonEmpty
  
}

