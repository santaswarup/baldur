package com.influencehealth.baldur.identity_load.person_identity.identity

import java.util.UUID

import com.influencehealth.baldur.identity_load.person_identity.identity.support.PersonIdentityColumns
import com.influencehealth.baldur.identity_load.person_identity.support.SupportImpl
import org.joda.time.DateTime
import org.scalatest._
import com.datastax.spark.connector.CassandraRow

class IdentityScoreSpec extends FlatSpec {

  behavior of "identityScore"

  val transientId = UUID.randomUUID()
  
  val unidentified = PersonIdentityColumns(customerId = 1, sourcePersonId = "source_person_id",
    source = "hospital", sourceType = "epic",
    address1 = Some("1234 Fake Street"), address2 = Some(""), streetSecondNumber = Some(""), zip5 = Some("35123"),
    firstName = Some("Bob"), middleName = Some("randy"), lastName = Some("Smith"), suffix = Some(""),
    dob = Some(new DateTime("1930-10-10")), sex = Some("M"),
    personId = None)

  val cassandraRow = CassandraRow.fromMap(Map("first_name" -> "Bob", "last_name" -> "Smith", "mrids" -> Set("ABC123"), "sex" -> "M",
    "dob" -> "1930-10-10", "address2" -> "", "street_second_number" -> "", "middle_name" -> "randy", "suffix" -> "",
    "root_first_name" -> "robert", "mrids" -> Set("hospital.epic.source_person_id")))
  
  it should "calculate correct score when everything matches" in {
    val (_, score) = SupportImpl.identityScore((unidentified, cassandraRow))

    assert(score == 2112110)
  }

  it should "be 2,000,000 less when first name root does not match" in {
    val (_, score) = SupportImpl.identityScore(
      (unidentified.copy(firstName = Some("Richard")), cassandraRow))

    assert(score == 112110)
  }

  it should "be 200,000 less when dob does not match" in {
    val (_, score) = SupportImpl.identityScore(
      (unidentified.copy(dob = Some(new DateTime("1897-01-01"))), cassandraRow))

    assert(score == 1912110)
  }

  it should "be 20,000 less when sex does not match" in {
    val (_, score) = SupportImpl.identityScore(
      (unidentified.copy(sex = Some("U")), cassandraRow))

    assert(score == 92110)
  }

  it should "be 2,000 less when address2 does not match" in {
    val (_, score) = SupportImpl.identityScore(
      (unidentified.copy(address2 = Some("Apt 909")), cassandraRow))

    assert(score == 2110110)
  }

  it should "be 2,000 less when address_secondary_number does not match" in {
    val (_, score) = SupportImpl.identityScore(
      (unidentified.copy(streetSecondNumber = Some("STE# 101")), cassandraRow))

    assert(score == 2110110)
  }

  it should "be 200 less when middle_name does not mach" in {
    val (_, score) = SupportImpl.identityScore(
      (unidentified.copy(middleName = Some("Boozer")), cassandraRow))

    assert(score == 2111910)
  }

  it should "be 20 less when suffix does not match" in {
    val (_, score) = SupportImpl.identityScore(
      (unidentified.copy(suffix = Some("Sr")), cassandraRow))

    assert(score == 2112090)
  }
}
