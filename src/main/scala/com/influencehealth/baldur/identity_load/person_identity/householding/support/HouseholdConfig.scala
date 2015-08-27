package com.influencehealth.baldur.identity_load.person_identity.householding.support

import com.influencehealth.baldur.support._

import org.apache.spark.SparkConf


case class HouseholdConfig(householdInputTopics: String,
                                householdStatsTopic: String,
                                brokerList: String,
                                keyspace: String,
                                addressTable: String,
                                householdTable: String,
                                kafkaReset: String) {

  def kafkaParams: Map[String, String] = Map("metadata.broker.list" -> brokerList,
    "auto.offset.reset" -> kafkaReset,
    "client.id" -> "person_identity")
}

object HouseholdConfig {
  def read(sc: SparkConf): HouseholdConfig = {

    // Specific to PersonIdentity
    val householdInputTopics = sc.get("spark.app.topic.household_input", "household_input")
    val householdStatsTopic = sc.get("spark.app.topic.household_stats", "household_stats")

    // Broker list exists in Common Config as we only have 1 Kafka cluster
    val brokerList = CommonConfig.kafkaBrokerList(sc)

    // All table references are in the CommonConfig class to avoid duplication
    val keyspace = CommonConfig.datahubKeyspace(sc)
    val addressTable = CommonConfig.addressTable(sc)
    val householdTable = CommonConfig.householdTable(sc)

    // Defaults to largest
    val kafkaReset = CommonConfig.kafkaReset(sc)

    HouseholdConfig(householdInputTopics,
      householdStatsTopic,
      brokerList,
      keyspace,
      addressTable,
      householdTable,
      kafkaReset)
  }
}

