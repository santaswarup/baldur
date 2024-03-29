#!/bin/sh

$SPARK_HOME/bin/spark-submit \
  --master spark://az-preds-ela-01.ecosmart.local:7077 \
  --total-executor-cores 4 \
  --jars target/scala-2.11/baldur-assembly-1.0.jar \
  --class com.influencehealth.baldur.intake.IntakeApp \
  target/scala-2.11/baldur_2.11-1.0.jar \
  -i $1 \
  -o /data/Baldur/ \
  -c piedmont \
  --source-type "physician office" \
  --source epic \
  --metadata.broker.list "az-kafka-01.ecosmart.local:9092,az-kafka-02.ecosmart.local:9092"
