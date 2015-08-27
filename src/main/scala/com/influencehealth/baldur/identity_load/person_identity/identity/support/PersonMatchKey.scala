package com.influencehealth.baldur.identity_load.person_identity.identity.support

abstract class PersonMatchKey()

case class PersonMatchKey1(
  sourceIdentity: SourceIdentity,
  record: PersonIdentityColumns,
  rootFirstName: String,
  soundexLastName: String,
  zip5: String,
  address1: String) extends PersonMatchKey()

case class PersonMatchKey2(
  sourceIdentity: SourceIdentity,
  record: PersonIdentityColumns,
  soundexLastName: String,
  zip5: String,
  address1: String) extends PersonMatchKey()

case class PersonMatchKey3(
  sourceIdentity: SourceIdentity,
  record: PersonIdentityColumns,
  rootFirstName: String,
  zip5: String,
  address1: String) extends PersonMatchKey()

case class PersonMatchKey4(
  sourceIdentity: SourceIdentity,
  record: PersonIdentityColumns,
  rootFirstName: String,
  soundexLastName: String,
  zip5: String,
  mrn: String) extends PersonMatchKey()

object PersonMatchKey {
  implicit class ToPersonMatchKeys(personIdentityColumns: PersonIdentityColumns) {
    def toPersonMatchKeys: (Option[PersonMatchKey1], Option[PersonMatchKey2], Option[PersonMatchKey3], Option[PersonMatchKey4]) = {
      (personIdentityColumns.toPersonMatchKey1,
        personIdentityColumns.toPersonMatchKey2,
        personIdentityColumns.toPersonMatchKey3,
        personIdentityColumns.toPersonMatchKey4)
    }

    def toPersonMatchKey1 = {
      (personIdentityColumns.rootFirstName, personIdentityColumns.soundexLastName, personIdentityColumns.zip5, personIdentityColumns.address1) match {
        case (Some(rootFirstName), Some(soundexLastName), Some(zip5), Some(address1)) =>
          Some(PersonMatchKey1(personIdentityColumns.sourceIdentity, personIdentityColumns, rootFirstName, soundexLastName, zip5, address1))
        case _ => None
      }
    }

    def toPersonMatchKey2 = {
      (personIdentityColumns.soundexLastName, personIdentityColumns.zip5, personIdentityColumns.address1) match {
        case (Some(soundexLastName), Some(zip5), Some(address1)) => Some(PersonMatchKey2(personIdentityColumns.sourceIdentity, personIdentityColumns, soundexLastName, zip5, address1))
        case _ => None
      }
    }

    def toPersonMatchKey3 = {
      (personIdentityColumns.rootFirstName, personIdentityColumns.zip5, personIdentityColumns.address1) match {
        case (Some(rootFirstName), Some(zip5), Some(address1)) => Some(PersonMatchKey3(personIdentityColumns.sourceIdentity, personIdentityColumns, rootFirstName, zip5, address1))
        case _ => None
      }
    }

    def toPersonMatchKey4 = {
      (personIdentityColumns.rootFirstName, personIdentityColumns.soundexLastName, personIdentityColumns.zip5, personIdentityColumns.mrn) match {
        case (Some(rootFirstName), Some(soundexLastName), Some(zip5), Some(mrn)) => Some(PersonMatchKey4(personIdentityColumns.sourceIdentity, personIdentityColumns, rootFirstName, soundexLastName, zip5, mrn))
        case _ => None
      }
    }

    def sourceIdentity = {
      SourceIdentity(personIdentityColumns.customerId, personIdentityColumns.externalPersonId, personIdentityColumns.source, personIdentityColumns.sourceType)
    }
  }
}
