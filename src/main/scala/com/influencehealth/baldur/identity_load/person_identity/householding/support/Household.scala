package com.influencehealth.baldur.identity_load.person_identity.householding.support

import java.util.UUID

case class Household(addressId: UUID,
                            lastName: String)

object Household {
  def create(householdAddressColumns: HouseholdAddress): (Household, HouseholdAddress) = {

    (Household(householdAddressColumns.addressId.get,
                      householdAddressColumns.lastName.get),householdAddressColumns)
  }
}
