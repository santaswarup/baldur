package com.influencehealth.baldur.support

import org.apache.spark.SparkConf

object CommonConfig {
  private val PersonMasterKeyspaceKey = "spark.app.keyspace"
  private val PersonMasterTableKey = "spark.app.table.person_master"
  private val KafkaResetKey = "spark.app.kafka.auto.offset.reset"
  private val SourceIdentityTableKey = "spark.app.table.source_identity"
  private val AddressTableKey = "spark.app.table.addresses"
  private val HouseholdTableKey = "spark.app.table.household"
  private val PersonChangeCaptureTableKey = "spark.app.table.person_change_capture"
  private val ActivityChangeCaptureTableKey = "spark.app.table.activity_change_capture"
  private val ActivityTableKey = "spark.app.table.activity"

  def personIdentityTables(sparkConf: SparkConf) =
    (1 to 4).map(x => sparkConf.get(s"spark.app.table.identity$x", s"person_master_identity$x"))

  def personMasterTable(sparkConf: SparkConf) = 
    sparkConf.get(PersonMasterTableKey, "person_master")

  def datahubKeyspace(sparkConf: SparkConf) =
    sparkConf.get(PersonMasterKeyspaceKey, "datahub")

  def sourceIdentityTable(sparkConf: SparkConf)=
    sparkConf.get(SourceIdentityTableKey, "source_identity")

  def addressTable(sparkConf: SparkConf)=
    sparkConf.get(AddressTableKey, "addresses")

  def householdTable(sparkConf: SparkConf)=
    sparkConf.get(HouseholdTableKey, "households")

  def personChangeCaptureTable(sparkConf: SparkConf)=
    sparkConf.get(PersonChangeCaptureTableKey, "person_master_changes")

  def activityChangeCaptureTable(sparkConf: SparkConf)=
    sparkConf.get(ActivityChangeCaptureTableKey, "person_activity_changes")

  def personActivityTable(sparkConf: SparkConf)=
    sparkConf.get(ActivityTableKey, "person_activity")

  def kafkaReset(sparkConf: SparkConf) =
    sparkConf.get(KafkaResetKey, "largest")

  def kafkaBrokerList(sparkConf: SparkConf) =
    sparkConf.get("spark.app.kafka.metadata.broker.list", "localhost:9092")
}
