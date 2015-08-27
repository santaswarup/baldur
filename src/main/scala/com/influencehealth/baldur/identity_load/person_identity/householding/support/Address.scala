package com.influencehealth.baldur.identity_load.person_identity.householding.support

case class Address(address1: String,
                             address2: String,
                             city: String,
                             state: String,
                             zip5: String,
                             zip4: String,
                             lat: Float,
                             lon: Float)

object Address{

  def create(householdAddressColumns: HouseholdAddress): (Address, HouseholdAddress) = {

    val oldAddress2: Option[String] = householdAddressColumns.address2

    val address2: String = oldAddress2 match {
      case None => ""
      case oldAddress2 => oldAddress2.get
    }

    (Address(householdAddressColumns.address1.get,
                    address2,
                    householdAddressColumns.city.get,
                    householdAddressColumns.state.get,
                    householdAddressColumns.zip5.get,
                    householdAddressColumns.zip4.get,
                    householdAddressColumns.lat.get,
                    householdAddressColumns.lon.get), householdAddressColumns)
  }

}