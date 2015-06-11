Customer Streaming Importer
===========================

Prerequisites
-------------

* Kafka
* Spark

Quick Start
-----------

```bash
sbt package
```

```bash
spark-submit --master local[*] --class App --jars lib_managed/jars/com.github.scopt/scopt_2.10/scopt_2.10-3.3.0.jar,lib_managed/jars/org.apache.kafka/kafka_2.10/kafka_2.10-0.8.2.1.jar,lib_managed/jars/org.apache.kafka/kafka-clients/kafka-clients-0.8.2.1.jar,lib_managed/jars/com.yammer.metrics/metrics-core/metrics-core-2.2.0.jar,lib_managed/jars/com.typesafe.play/play-json_2.10/play-json_2.10-2.3.4.jar,lib_managed/jars/com.typesafe.play/play-functional_2.10/play-functional_2.10-2.3.4.jar target/scala-2.10/baldur_2.10-1.0.jar -c piedmont -o /data/tmp -i /data/tmp/in --type utilization -i 30 --metadata.broker.list localhost:9092""
```



