package com.influencehealth.baldur.identity_load.person_identity.identity.support

import com.influencehealth.baldur.support._

import org.apache.spark.SparkConf


case class PersonIdentityConfig(identityInputTopics: String,
                                identityStatsTopic: String,
                                brokerList: String,
                                keyspace: String,
                                personMasterTable: String,
                                sourceIdentityTable: String,
                                identity1Table: String,
                                identity2Table: String,
                                identity3Table: String,
                                identity4Table: String,
                                trustSourceId: Boolean,
                                kafkaReset: String) {

  def kafkaParams: Map[String, String] = Map("metadata.broker.list" -> brokerList,
    "auto.offset.reset" -> kafkaReset,
    "client.id" -> "person_identity")
}

object PersonIdentityConfig {
  def read(sc: SparkConf): PersonIdentityConfig = {

    //Specific to PersonIdentity
    val identityInputTopics = sc.get("spark.app.topic.identity", "identity_input")
    val identityStatsTopic = sc.get("spark.app.topic.identity_stats", "identity_stats")

    val trustSourceId = sc.get("spark.app.identity.trust_source_id", "true").toBoolean

    //Broker list exists in Common Config as we only have 1 Kafka cluster
    val brokerList = CommonConfig.kafkaBrokerList(sc)

    //All table references are in the CommonConfig class to avoid duplication
    val keyspace = CommonConfig.datahubKeyspace(sc)
    val sourceIdentityTable = CommonConfig.sourceIdentityTable(sc)
    val table = CommonConfig.personMasterTable(sc)
    val identityTables = CommonConfig.personIdentityTables(sc)
    val identity1Table = identityTables(0)
    val identity2Table = identityTables(1)
    val identity3Table = identityTables(2)
    val identity4Table = identityTables(3)

    //Defaults to largest
    val kafkaReset = CommonConfig.kafkaReset(sc)

    PersonIdentityConfig(identityInputTopics,
      identityStatsTopic,
      brokerList,
      keyspace,
      table,
      sourceIdentityTable,
      identity1Table,
      identity2Table,
      identity3Table,
      identity4Table,
      trustSourceId,
      kafkaReset)
  }
}

