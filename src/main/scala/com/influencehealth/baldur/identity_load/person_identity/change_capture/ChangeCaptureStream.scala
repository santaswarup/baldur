package com.influencehealth.baldur.identity_load.person_identity.change_capture

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.influencehealth.baldur.identity_load.person_identity.change_capture.support._
import com.influencehealth.baldur.identity_load.person_identity.identity_table.support.IdentityTableCreatorConfig
import com.influencehealth.baldur.support.JsonSupport._
import com.influencehealth.baldur.identity_load.person_identity.support._
import com.influencehealth.baldur.support._
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import play.api.libs.json._


object ChangeCaptureStream {
  var support: Support = SupportImpl

  def processChanges(changeCaptureConfig: ChangeCaptureConfig, cassandraConnector: CassandraConnector, kafkaProducerConfig: Map[String,Object], changeCaptureStream: RDD[ChangeCaptureMessage]): RDD[ColumnChange] = {

    // Do a left join to the person-master-changes table in Cassandra
    // Keying by the customerID and personId because those are the partition keys in the table
    // Doing so prevents shuffles in the spanByKey function
    val existingPersonsDetermined: RDD[(ChangeCaptureMessage, Seq[ColumnChange])] =
      changeCaptureStream
        .distinct()
        .joinWithCassandraTable[ColumnChange](changeCaptureConfig.keyspace, changeCaptureConfig.personChangeCaptureTable)
        .spanByKey
        .map(ChangeCaptureSupport.determineExistingChanges(_,"person_master"))
        .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val newPersonsDetermined: RDD[(ChangeCaptureMessage, Seq[ColumnChange])] =
      changeCaptureStream
        .distinct()
        .map{x => (x,None)}
        .leftOuterJoin(existingPersonsDetermined)
        .filter{case (changeCapture, (None, columnChange)) => columnChange.isEmpty}
        .map{ case (changeCapture, (None, columnChange)) => ChangeCaptureSupport.determineNewChanges(changeCapture, "person_master")}

    val personMasterChanges: RDD[(ChangeCaptureMessage, Seq[ColumnChange])] =
      existingPersonsDetermined
      .union(newPersonsDetermined)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val activityJoinColumns: SomeColumns = SomeColumns("customer_id", "person_id", "source_record_id", "source", "source_type")

    val existingActivitiesDetermined: RDD[(ChangeCaptureMessage, Seq[ColumnChange])] =
      changeCaptureStream
        .distinct()
        .joinWithCassandraTable[ColumnChange](changeCaptureConfig.keyspace, changeCaptureConfig.activityChangeCaptureTable)
        .on(activityJoinColumns)
        .spanByKey
        .map(ChangeCaptureSupport.determineExistingChanges(_,"person_activity"))
        .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val newActivitiesDetermined: RDD[(ChangeCaptureMessage, Seq[ColumnChange])] =
      changeCaptureStream
        .distinct()
        .map{x => (x,None)}
        .leftOuterJoin(existingActivitiesDetermined)
        .filter{case (changeCapture, (None, columnChange)) => columnChange.isEmpty}
        .map{ case (changeCapture, (None, columnChange)) => ChangeCaptureSupport.determineNewChanges(changeCapture, "person_activity")}

    val personActivityChanges: RDD[(ChangeCaptureMessage, Seq[ColumnChange])] =
      existingActivitiesDetermined
        .union(newActivitiesDetermined)
        .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val personMasterChangesFlattened: RDD[ColumnChange] =
      personMasterChanges
      .map{case (personChange, columnChange) => columnChange}
      .flatMap(x => x)

    // Save person master changes to Cassandra
    personMasterChangesFlattened
      .saveToCassandra(changeCaptureConfig.keyspace, changeCaptureConfig.personChangeCaptureTable)

    // Update the person-master table and send to change capture stream
    personMasterChanges
      .foreachPartition { partition =>
        // Start the connector within the foreachPartition
        cassandraConnector.withSessionDo { session =>

            partition.foreach { case (personChange, columnChangesForPerson) => //

                  // Update the person-master table
              ChangeCaptureSupport.updateTable(changeCaptureConfig.keyspace,
                    changeCaptureConfig.personMasterTable,
                    personChange.customerId,
                    personChange.personId,
                    personChange.source,
                    personChange.sourceType,
                    personChange.sourceRecordId,
                    columnChangesForPerson,
                    session,
                    changeCaptureConfig.personMasterPrimaryKeyColumns)

                  // Send changes to person_master_Changes
                  columnChangesForPerson
                    .map { change => Json.stringify(Json.toJson(change)) }
                    .foreach { json =>
                    val kafkaProducer = ProducerObject.get(kafkaProducerConfig)
                    support.sendToTopic(kafkaProducer,
                      new ProducerRecord[String, String](changeCaptureConfig.personChangesOutputTopic, json))
                  }
              }
        }
    }


    personActivityChanges
      .map{case (activityChange, columnChange) => columnChange}
      .flatMap(x => x)
      .saveToCassandra(changeCaptureConfig.keyspace,changeCaptureConfig.activityChangeCaptureTable)

    personActivityChanges
      .foreachPartition { partition =>

      cassandraConnector.withSessionDo { session =>

          partition.foreach { case (activityChange, columnChangesForActivity) =>

                // Update the person-activity table
            ChangeCaptureSupport.updateTable(changeCaptureConfig.keyspace,
                  changeCaptureConfig.personActivityTable,
                  activityChange.customerId,
                  activityChange.personId,
                  activityChange.source,
                  activityChange.sourceType,
                  activityChange.sourceRecordId,
                  columnChangesForActivity,
                  session,
                  changeCaptureConfig.personActivityPrimaryKeyColumns)

                // Send changes to person_activity_Changes
                columnChangesForActivity
                  .map { change => Json.stringify(Json.toJson(change)) }
                  .foreach { json =>
                  val kafkaProducer = ProducerObject.get(kafkaProducerConfig)
                  support.sendToTopic(kafkaProducer,
                    new ProducerRecord[String, String](changeCaptureConfig.activityChangesOutputTopic, json))
                }
          }
      }
    }

    existingActivitiesDetermined.unpersist()
    existingPersonsDetermined.unpersist()
    personMasterChanges.unpersist()
    personActivityChanges.unpersist()


    personMasterChangesFlattened
  }

  implicit class ChangeCaptureStreamingContext(val streamingContext: StreamingContext) {
    def createChangeCaptureStream(changeCaptureConfig: ChangeCaptureConfig,
                                  identityTableCreatorConfig: IdentityTableCreatorConfig,
                                  cassandraConnector: CassandraConnector,
                                  kafkaParams: Map[String, String],
                                  kafkaProducerConfig: Map[String, Object],
                                  emptyChangeRdd: RDD[ColumnChange] ) = {
      val changeCaptureStream = support.createDirectStream(streamingContext, kafkaParams, changeCaptureConfig.changeCaptureInputTopics.split(",").toSet)

      changeCaptureStream
    }
  }

}
