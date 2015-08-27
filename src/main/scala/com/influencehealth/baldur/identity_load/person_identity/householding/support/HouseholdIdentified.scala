package com.influencehealth.baldur.identity_load.person_identity.householding.support

import java.util.UUID
import org.joda.time.DateTime

case class HouseholdIdentified(addressId: UUID,
                            lastName: String,
                            householdId: UUID,
                            updatedAt: DateTime = DateTime.now())

object HouseholdIdentified {
  def identifyHouseholdColumns(householdColumns: Household): HouseholdIdentified = {
    val householdId: UUID = UUID.randomUUID()
    HouseholdIdentified(householdColumns.addressId,
      householdColumns.lastName,
      householdId)
  }

  def convertToHouseholdsWithId(identifiedColumns: (HouseholdIdentified, UUID)): (Household,UUID) = {
    (Household(identifiedColumns._1.addressId,
      identifiedColumns._1.lastName),identifiedColumns._2)
  }

  def convertToHouseholdsWithTuple(identifiedColumns: (HouseholdIdentified, UUID)): (Household,(HouseholdIdentified,UUID)) = {
    (Household(identifiedColumns._1.addressId,
      identifiedColumns._1.lastName),identifiedColumns)
  }

  def isNewHousehold(householdInput: (Household,((HouseholdIdentified,UUID),Option[(HouseholdIdentified,UUID)]))): Boolean = {
    householdInput._2._2.isEmpty
  }
}
