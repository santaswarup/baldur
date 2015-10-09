package com.influencehealth.baldur

import java.util.UUID

import com.datastax.spark.connector.cql.CassandraConnector
import com.influencehealth.baldur.identity_load.person_identity.change_capture.support.ChangeCaptureConfig
import com.influencehealth.baldur.identity_load.person_identity.identity.support.PersonIdentityConfig
import com.influencehealth.location_scoring.support._
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.spark._
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import scala.io.Source

/**
 * Location scoring - batch. Scores all persons in the person master
 */
object DeleteCodeChanges {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Remove Location Changes").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(sparkConf)
    val config: ChangeCaptureConfig = ChangeCaptureConfig.read(sparkConf)
    val connector: CassandraConnector = CassandraConnector(sparkConf)
    val StringSerializer = "org.apache.kafka.common.serialization.StringSerializer"

    val kafkaProducerConfig = Map[String, Object](
      "bootstrap.servers" -> config.brokerList,
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> StringSerializer,
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> StringSerializer)

    val keyspace = config.keyspace
    val table = config.personChangeCaptureTable

    val allPersons: RDD[(Int, UUID)] =
      sc.cassandraTable[(Int, UUID)](keyspace, table)
        .select("customer_id", "person_id")
        .where("column_name in('mx_groups', 'service_lines', 'mx_codes')")

    allPersons
      .foreachPartition{ partition =>

      connector.withSessionDo{ session =>

        partition.foreach{ case row =>
          val customerId: Integer = row._1.asInstanceOf[Integer]
          val personId: UUID = row._2

          val cql = s"DELETE FROM $keyspace.$table WHERE customer_id = ? and person_id = ? and column_name = 'locations'"
          val cqlString = s"DELETE FROM $keyspace.$table WHERE customer_id = $customerId and person_id = $personId and column_name in('mx_groups', 'service_lines', 'mx_codes')"
          val prepared = session.prepare(cql)

          try {
            session.execute( prepared.bind(customerId, personId) )
          } catch {
            case err: Throwable =>
              throw new Error(s"Unable to execute CQL statement: $cqlString", err)
          }
        }

        session.close()

      }
    }
  }

}





