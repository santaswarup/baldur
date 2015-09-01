package com.influencehealth.baldur.identity_load.person_identity.householding

import java.util.UUID

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.extensions._
import com.influencehealth.baldur.identity_load.person_identity.change_capture.support._
import com.influencehealth.baldur.identity_load.person_identity.householding.support._
import com.influencehealth.baldur.identity_load.person_identity.identity_table.support.IdentityTableCreatorConfig
import com.influencehealth.baldur.identity_load.person_identity.support._
import com.influencehealth.baldur.support._
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import play.api.libs.json._


object HouseholdStream {
  var support: Support = SupportImpl

  def processHouseholds(householdConfig: HouseholdConfig, identifiedRdd: RDD[JsObject],
                        kafkaProducerConfig: Map[String, Object]): RDD[JsObject] = {

    // HouesholdAddress to input record
    val householdAddressToJs: RDD[(HouseholdAddress, JsObject)] = identifiedRdd
      .map { record => (HouseholdAddress.create(record), record) }

    // Just household address
    val records: RDD[HouseholdAddress] = householdAddressToJs
      .map{ case (householdAddress, js) => householdAddress }
      .distinct()

    // Get the records that we can not assign an address id to because we do not have enough information
    val unaddressable: RDD[HouseholdAddress] =
      records
        .filter(x => !x.hasAddressColumns)

    // Get the records that we can not assign a household id to because we do not have enough information
    val unhouseholdable: RDD[HouseholdAddress] =
      records
        .filter(x => x.hasAddressColumns && x.lastName.isEmpty)

    // Defining records for addressing
    val addressRecords = records
      .filter(_.hasAddressColumns)
      .map { householdAddress =>
      if (householdAddress.address2.isEmpty)
        householdAddress.copy(address2 = Some(""))
      else
        householdAddress
    }
      .leftOuterJoinWithCassandraTable[UUID](householdConfig.keyspace, householdConfig.addressTable).select("address_id")
      .map {
      case (address, Some(addressId)) => address.copy(addressId = Some(addressId))
      case (address, None) => address
    }.persist(StorageLevel.MEMORY_AND_DISK)

    // Generate new address ids for those records that do not have an address id
    val newAddresses: RDD[HouseholdAddress] =
      addressRecords
      .filter(x => x.addressId.isEmpty)
      .groupBy(x => (x.address1, x.address2, x.city, x.state, x.zip5, x.zip4))
      .flatMap {
      case (_, householdAddresses) =>
        val addressId = UUID.randomUUID()
        householdAddresses.map(_.copy(addressId = Some(addressId)))
    }.persist(StorageLevel.MEMORY_AND_DISK)

    newAddresses.map(_.toAddress).saveToCassandra(householdConfig.keyspace, householdConfig.addressTable)

    // Generate new household ids for the new addresses
    val newAddressNewHouseholds: RDD[HouseholdAddress] =
      newAddresses
      .groupBy(x => x.addressId.toString + x.lastName)
      .flatMap {
        case (_, householdAddresses) =>
          val householdId = UUID.randomUUID()
          householdAddresses.map(_.copy(householdId = Some(householdId)))
      }
      .filter(_.hasHouseholdColumns)
      .persist(StorageLevel.MEMORY_AND_DISK)

    newAddressNewHouseholds.map(_.toHousehold).saveToCassandra(householdConfig.keyspace, householdConfig.householdTable)

    val existingAddresses: RDD[HouseholdAddress] =
      addressRecords
      .filter(_.addressId.isDefined)
      .persist(StorageLevel.MEMORY_AND_DISK)

    // Get household ids for existing addresses
    val householdRecords: RDD[HouseholdAddress] =
      existingAddresses
      .filter(_.hasHouseholdColumns)
      .leftOuterJoinWithCassandraTable[UUID](householdConfig.keyspace, householdConfig.householdTable)
      .select("household_id")
      .map {
      case (address, Some(householdId)) => address.copy(householdId = Some(householdId))
      case (address, None) => address
    }.persist(StorageLevel.MEMORY_AND_DISK)

    val existingHouseholds: RDD[HouseholdAddress] = householdRecords.filter(_.householdId.isDefined)

    val existingAddressesWithNewHouseholdIds: RDD[HouseholdAddress] =
      householdRecords
      .filter{x => x.householdId.isEmpty}
      .groupBy(x => (x.addressId.get, x.lastName.get))
      .flatMap {
      case (_, householdAddress) =>
        val householdId = UUID.randomUUID()
        householdAddress.map(_.copy(householdId = Some(householdId)))
    }.persist(StorageLevel.MEMORY_AND_DISK)

    existingAddressesWithNewHouseholdIds.map(_.toHousehold)
      .saveToCassandra(householdConfig.keyspace, householdConfig.householdTable)


    val result: RDD[JsObject] = householdAddressToJs
      .join(
        existingAddressesWithNewHouseholdIds
          .union(existingHouseholds)
          .union(newAddressNewHouseholds)
          .union(unaddressable)
          .union(unhouseholdable)
          .map((_,None)))
      .map {
        case (householdAddress, (record, nothing)) =>
          def addressId = if (householdAddress.addressId.isDefined)
            JsString(householdAddress.addressId.get.toString)
          else
            JsNull

          def householdId = if (householdAddress.householdId.isDefined)
            JsString(householdAddress.householdId.get.toString)
          else
            JsNull

          record +
            ("addressId", addressId) +
            ("householdId", householdId)
      }

    val stats = Seq(
      "inbound records" -> identifiedRdd.count(),
      "existing addresses" -> existingAddresses.count(),
      "existing addresses with new households" -> existingAddressesWithNewHouseholdIds.count(),
      "existing addresses with existing households" -> existingHouseholds.count(),
      "new addresses new households" -> newAddressNewHouseholds.count(),
      "unaddressable" -> unaddressable.count(),
      "unhouseholdable" -> unhouseholdable.count(),
      "outbound records" -> result.count())
      .map {
      case (key, value) => (key, JsNumber(value))
    }

    stats.foreach(println)

    support.sendToTopic(ProducerObject.get(kafkaProducerConfig),
      new ProducerRecord[String, String](householdConfig.householdStatsTopic,
        Json.stringify(JsObject(stats))))


    records.unpersist()
    addressRecords.unpersist()
    newAddresses.unpersist()
    existingAddresses.unpersist()
    householdRecords.unpersist()
    existingAddressesWithNewHouseholdIds.unpersist()

    result
  }

  implicit class HouseholdStreamStreamingContext(val streamingContext: StreamingContext) {
    def createHouseholdStream(householdConfig: HouseholdConfig,
                              changeCaptureConfig: ChangeCaptureConfig,
                              identityTableCreatorConfig: IdentityTableCreatorConfig,
                              cassandraConnector: CassandraConnector,
                              kafkaParams: Map[String, String],
                              kafkaProducerConfig: Map[String, Object],
                              emptyJsonRdd: RDD[JsObject],
                              emptyChangeRdd: RDD[ColumnChange]) = {
      val householdStream = support.createDirectStream(streamingContext,
        kafkaParams,
        householdConfig.householdInputTopics.split(",").toSet)

      householdStream
    }
  }

}
