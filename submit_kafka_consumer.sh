#!/bin/bash

# Submit the Kafka consumer job with the required Kafka connector packages
docker exec spark-master spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /opt/spark-apps/simple_kafka_consumer.py
