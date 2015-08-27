package com.datastax.spark.connector

import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.rdd.reader._
import com.datastax.spark.connector.rdd.{CassandraLeftOuterJoinRDD, ValidRDDType}
import com.datastax.spark.connector.writer._
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

import scala.language.implicitConversions


package object extensions {

  implicit def toRDDExtensions[T](rdd: RDD[T]): RDDExtensions[T] =
    new RDDExtensions(rdd)

}

/** Provides Cassandra-specific methods on [[org.apache.spark.rdd.RDD RDD]] */
class RDDExtensions[T](rdd: RDD[T]) {

  def leftOuterJoinWithCassandraTable[R](keyspaceName: String, tableName: String,
                                         selectedColumns: ColumnSelector = AllColumns,
                                         joinColumns: ColumnSelector = PartitionKeyColumns)
                                        (implicit connector: CassandraConnector = CassandraConnector(rdd.sparkContext.getConf),
                                         newType: ClassTag[R], rrf: RowReaderFactory[R], ev: ValidRDDType[R],
                                         currentType: ClassTag[T], rwf: RowWriterFactory[T]): CassandraLeftOuterJoinRDD[T, R] = {
    new CassandraLeftOuterJoinRDD[T, R](rdd, keyspaceName, tableName, connector, columnNames = selectedColumns, joinColumns = joinColumns)
  }
}