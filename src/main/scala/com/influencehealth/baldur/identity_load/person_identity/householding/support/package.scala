package com.influencehealth.baldur.identity_load.person_identity.householding

package object support {
  implicit class HouseholdAddressToAddressIdentified(val householdAddress: HouseholdAddress) {
    def toAddress: AddressIdentified = {
      AddressIdentified(householdAddress.address1.get, if (householdAddress.address2.isDefined) householdAddress.address2.get else "",
        householdAddress.city.get, householdAddress.state.get, householdAddress.zip5.get, householdAddress.zip4.get,
        householdAddress.lat.get, householdAddress.lon.get, householdAddress.addressId.get)
    }
  }

  implicit class HouseholdAddressToHousehold(val householdAddress: HouseholdAddress) {
    def toHousehold: HouseholdIdentified = {
      HouseholdIdentified(householdAddress.addressId.get, householdAddress.lastName.get, householdAddress.householdId.get)
    }
  }
}
