package com.influencehealth.baldur.identity_load.person_identity.change_capture.support

import com.influencehealth.baldur.support._

import org.apache.spark.SparkConf


case class ChangeCaptureConfig(changeCaptureInputTopics: String,
                                personChangesOutputTopic: String,
                                activityChangesOutputTopic: String,
                                changeCaptureStatsTopic: String,
                                brokerList: String,
                                keyspace: String,
                                personChangeCaptureTable: String,
                                activityChangeCaptureTable: String,
                                personMasterTable: String,
                                personActivityTable: String,
                                personMasterPrimaryKeyColumns: Seq[String],
                                personActivityPrimaryKeyColumns: Seq[String],
                                kafkaReset: String) {

  def kafkaParams: Map[String, String] = Map("metadata.broker.list" -> brokerList,
    "auto.offset.reset" -> kafkaReset,
    "client.id" -> "person_identity")
}

object ChangeCaptureConfig {
  def read(sc: SparkConf): ChangeCaptureConfig = {

    val personMasterPrimaryKeyColumns = Seq("person_id", "customer_id")
    val personActivityPrimaryKeyColumns = Seq("person_id", "customer_id", "source", "source_type", "source_record_id")
    //Specific to PersonIdentity
    val changeCaptureInputTopics = sc.get("spark.app.topic.change_capture_input", "change_capture_input")
    val personChangesOutputTopic = sc.get("spark.app.topic.person_master_changes", "person_master_changes")
    val activityChangesOutputTopic = sc.get("spark.app.topic.person_activity_changes", "person_activity_changes")
    val changeCaptureStatsTopic = sc.get("spark.app.topic.change_capture_stats", "change_capture_stats")

    //Broker list exists in Common Config as we only have 1 Kafka cluster
    val brokerList = CommonConfig.kafkaBrokerList(sc)

    //All table references are in the CommonConfig class to avoid duplication
    val keyspace = CommonConfig.datahubKeyspace(sc)
    val personChangeCaptureTable = CommonConfig.personChangeCaptureTable(sc)
    val activityChangeCaptureTable = CommonConfig.activityChangeCaptureTable(sc)
    val personMasterTable = CommonConfig.personMasterTable(sc)
    val personActivityTable = CommonConfig.personActivityTable(sc)

    //Defaults to largest
    val kafkaReset = CommonConfig.kafkaReset(sc)

    ChangeCaptureConfig(changeCaptureInputTopics,
      activityChangesOutputTopic,
      personChangesOutputTopic,
      changeCaptureStatsTopic,
      brokerList,
      keyspace,
      personChangeCaptureTable,
      activityChangeCaptureTable,
      personMasterTable,
      personActivityTable,
      personMasterPrimaryKeyColumns,
      personActivityPrimaryKeyColumns,
      kafkaReset)
  }
}

