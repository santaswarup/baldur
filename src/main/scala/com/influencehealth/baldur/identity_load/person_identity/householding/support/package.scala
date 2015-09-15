package com.influencehealth.baldur.identity_load.person_identity.householding

package object support {
  implicit class HouseholdAddressToAddressIdentified(val householdAddress: HouseholdAddress) {
    def toAddress: AddressIdentified = {
      AddressIdentified(
        householdAddress.address1.get.toLowerCase,
        if (householdAddress.address2.isDefined) householdAddress.address2.get.toLowerCase else "",
        householdAddress.city.get.toLowerCase,
        householdAddress.state.get.toLowerCase,
        householdAddress.zip5.get,
        householdAddress.zip4.get,
        householdAddress.lat.get,
        householdAddress.lon.get,
        householdAddress.addressId.get)
    }
  }

  implicit class HouseholdAddressToHousehold(val householdAddress: HouseholdAddress) {
    def toHousehold: HouseholdIdentified = {
      HouseholdIdentified(
        householdAddress.addressId.get,
        householdAddress.lastName.get.toLowerCase,
        householdAddress.householdId.get)
    }
  }
}
