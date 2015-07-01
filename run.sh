#!/bin/sh

spark-submit --master $3 \
  --total-executor-cores 4 \
  --jars target/scala-2.11/baldur-assembly-1.0.jar \
  target/scala-2.11/baldur_2.11-1.0.jar \
  -i $1 \
  -o $2 \
  -c piedmont \
  --source-type epic \
  --source hb \
  --metadata.broker.list "az-datahub-01.ecosmart.local:9092,az-datahub-02.ecosmart.local:9092,az-datahub-03.ecosmart.local:9092"
