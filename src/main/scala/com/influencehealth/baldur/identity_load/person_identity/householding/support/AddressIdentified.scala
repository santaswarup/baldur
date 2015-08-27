package com.influencehealth.baldur.identity_load.person_identity.householding.support

import java.util.UUID
import org.joda.time.DateTime

case class AddressIdentified(address1: String,
                             address2: String,
                             city: String,
                             state: String,
                             zip5: String,
                             zip4: String,
                             lat: Float,
                             lon: Float,
                             addressId: UUID,
                             updatedAt: DateTime=DateTime.now())

object AddressIdentified{

  def identifyAddressColumns(addressColumns: Address): AddressIdentified = {
    val addressId: UUID = UUID.randomUUID()
    AddressIdentified(addressColumns.address1,
                    addressColumns.address2,
                    addressColumns.city,
                    addressColumns.state,
                    addressColumns.zip5,
                    addressColumns.zip4,
                    addressColumns.lat,
                    addressColumns.lon,
                    addressId)
  }

  def convertToAddressesWithId(identifiedColumns: (AddressIdentified, UUID)): (Address,UUID) = {
    (Address(identifiedColumns._1.address1,
      identifiedColumns._1.address2,
      identifiedColumns._1.city,
      identifiedColumns._1.state,
      identifiedColumns._1.zip5,
      identifiedColumns._1.zip4,
      identifiedColumns._1.lat,
      identifiedColumns._1.lon),identifiedColumns._2)
  }

  def convertToAddressesWithTuple(identifiedColumns: (AddressIdentified, UUID)): ((Address),(AddressIdentified,UUID)) = {
    (Address(identifiedColumns._1.address1,
      identifiedColumns._1.address2,
      identifiedColumns._1.city,
      identifiedColumns._1.state,
      identifiedColumns._1.zip5,
      identifiedColumns._1.zip4,
      identifiedColumns._1.lat,
      identifiedColumns._1.lon),identifiedColumns)
  }

  def isNewAddress(addressInput: (Address,((AddressIdentified,UUID),Option[(AddressIdentified,UUID)]))): Boolean = {
    addressInput._2._2.isEmpty
  }

}