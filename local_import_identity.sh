#!/bin/sh

$SPARK_HOME/bin/spark-submit \
  --master local[*] \
  --jars target/scala-2.11/baldur-assembly-1.0.jar \
  --class com.influencehealth.baldur.identity_load.IdentityLoadApp \
  --conf spark.app.kafka.metadata.broker.list="localhost:9092" \
  --conf spark.cassandra.connection.host=localhost \
  --conf spark.rdd.compress=true \
  target/scala-2.11/baldur_2.11-1.0.jar \
  -i $1 \
  --inputSource "baldur" \
  --metadata.broker.list "localhost:9092"
