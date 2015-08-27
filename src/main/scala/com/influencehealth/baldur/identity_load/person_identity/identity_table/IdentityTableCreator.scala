package com.influencehealth.baldur.identity_load.person_identity.identity_table

import java.util.UUID

import com.datastax.spark.connector._
import com.influencehealth.baldur.identity_load.person_identity.identity_table.support._
import com.influencehealth.baldur.identity_load.person_identity.support._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

object IdentityTableCreator {
  var support: Support = SupportImpl

  def createIdentityTables(rdd: RDD[(Int,UUID)], config: IdentityTableCreatorConfig) = {
    val joinedRdd = rdd.joinWithCassandraTable[PersonIdentity](config.keyspace,
      config.personMasterTable).persist(StorageLevel.MEMORY_AND_DISK_SER)

    val identity1Candidates = joinedRdd.filter {
      case (_, personIdentity) => personIdentity.hasKey1
      case _ => false
    }.map {
      case (_, personIdentity) => Identity1.create(personIdentity)
    }

    val identity2Candidates = joinedRdd.filter {
      case (_, personIdentity) => personIdentity.hasKey2
      case _ => false
    }.map {
      case (_, personIdentity) => Identity2.create(personIdentity)
    }

    val identity3Candidates = joinedRdd.filter {
      case (_, personIdentity) => personIdentity.hasKey3
      case _ => false
    }.map {
      case (_, personIdentity) => Identity3.create(personIdentity)
    }

    val identity4Candidates = joinedRdd.filter {
      case (_, personIdentity) => personIdentity.hasKey4
      case _ => false
    }.flatMap {
      case (_, personIdentity) => Identity4.create(personIdentity)
    }

    identity1Candidates.saveToCassandra(config.keyspace, config.identityTables(0))
    identity2Candidates.saveToCassandra(config.keyspace, config.identityTables(1))
    identity3Candidates.saveToCassandra(config.keyspace, config.identityTables(2))
    identity4Candidates.saveToCassandra(config.keyspace, config.identityTables(3))

    joinedRdd.unpersist()
    rdd
  }


  implicit class StreamingContextImprovements(val ssc: StreamingContext) {
    def createIdentityTableCreationStream(config: IdentityTableCreatorConfig): DStream[(String, String)] = {
      support.createDirectStream(ssc, config.kafkaParams,config.identityTableCreatorInputTopics.split(",").toSet)
    }
  }
}
