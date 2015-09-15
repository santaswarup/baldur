#!/bin/sh

$SPARK_HOME/bin/spark-submit \
  --master spark://az-preds-ela-01.ecosmart.local:7077 \
  --total-executor-cores 20 \
  --executor-memory 3G \
  --jars target/scala-2.11/baldur-assembly-1.0.jar \
  --class com.influencehealth.baldur.identity_load.IdentityLoadApp \
  --conf spark.app.kafka.metadata.broker.list="az-kafka-01.ecosmart.local:9092,az-kafka-02.ecosmart.local:9092" \
  --conf spark.cassandra.connection.host=az-datahub-01.ecosmart.local \
  --conf spark.rdd.compress=true \
  --conf spark.app.identity.trust_source_id=false \
  target/scala-2.11/baldur_2.11-1.0.jar \
  -i $1 \
  --inputSource "prospects" \
  --metadata.broker.list "az-kafka-01.ecosmart.local:9092,az-kafka-02.ecosmart.local:9092"