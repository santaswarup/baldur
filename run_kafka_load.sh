#!/bin/sh

$SPARK_HOME/bin/spark-submit \
  --master local[*] \
  --total-executor-cores 4 \
  --jars target/scala-2.11/baldur-assembly-1.0.jar \
  --class com.influencehealth.baldur.anchor_to_kafka.AnchorToKafkaApp \
  target/scala-2.11/baldur_2.11-1.0.jar \
  -i $1 \
  --inputSource "baldur" \
  --metadata.broker.list "localhost:9092"
