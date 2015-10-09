#!/bin/sh

$SPARK_HOME/bin/spark-submit \
  --master spark://az-preds-ela-01.ecosmart.local:7077 \
  --total-executor-cores 4 \
  --jars target/scala-2.11/baldur-assembly-1.0.jar \
  --class com.influencehealth.baldur.DeleteCodeChanges \
  --conf spark.app.kafka.metadata.broker.list="az-kafka-01.ecosmart.local:9092" \
  --conf spark.cassandra.connection.host=az-datahub-01.ecosmart.local \
  target/scala-2.11/baldur_2.11-1.0.jar