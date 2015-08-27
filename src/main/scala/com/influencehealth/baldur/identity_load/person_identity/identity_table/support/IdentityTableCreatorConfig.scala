package com.influencehealth.baldur.identity_load.person_identity.identity_table.support

import com.influencehealth.baldur.support._

import org.apache.spark.SparkConf


case class IdentityTableCreatorConfig(identityTableCreatorInputTopics: String,
  keyspace: String,
  personMasterTable: String,
  identityTables: Seq[String],
  brokerList: String,
  kafkaReset: String){

  def kafkaParams: Map[String, String] = Map("metadata.broker.list" -> brokerList,
    "auto.offset.reset" -> kafkaReset,
    "client.id" -> "person_identity")
}


object IdentityTableCreatorConfig {
  def read(sc: SparkConf): IdentityTableCreatorConfig = {

    val identityTableCreatorInputTopics = sc.get("spark.app.topic.identity_table_creator_input", "identity_table_creator_input")

    val keyspace = CommonConfig.datahubKeyspace(sc)
    val personMasterTable = CommonConfig.personMasterTable(sc)
    val identityTables = Seq("person_master_identity1",
      "person_master_identity2",
      "person_master_identity3",
      "person_master_identity4")

    val kafkaReset = CommonConfig.kafkaReset(sc)
    val brokerList = CommonConfig.kafkaBrokerList(sc)

    IdentityTableCreatorConfig(identityTableCreatorInputTopics,
    keyspace,
    personMasterTable,
    identityTables,
    brokerList,
    kafkaReset)
  }
}
