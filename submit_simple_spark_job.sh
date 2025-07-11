#!/bin/bash
# Submit Simple Credit Card Analytics (Console Output)

echo "🚀 Submitting SIMPLE Credit Card Analytics (Console Output)..."

# Stop any existing Spark job first
echo "Stopping any existing Spark applications..."
docker exec spark-master /opt/bitnami/spark/bin/spark-submit --kill || true

# Wait a bit for cleanup
sleep 5

# Submit the SIMPLE job (console output)
docker exec spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --driver-memory 1g \
  --executor-memory 3g \
  --executor-cores 2 \
  --total-executor-cores 2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,io.delta:delta-core_2.12:2.4.0 \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --conf "spark.sql.streaming.forceDeleteTempCheckpointLocation=true" \
  --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
  --conf "spark.sql.streaming.schemaInference=true" \
  --conf "spark.sql.adaptive.enabled=false" \
  --conf "spark.sql.adaptive.coalescePartitions.enabled=false" \
  --conf "spark.dynamicAllocation.enabled=false" \
  --conf "spark.shuffle.service.enabled=false" \
  --conf "spark.ui.showConsoleProgress=true" \
  --conf "spark.sql.streaming.stateStore.providerClass=org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider" \
  --conf "spark.sql.shuffle.partitions=2" \
  --conf "spark.default.parallelism=2" \
  --conf "spark.sql.streaming.statefulOperator.useStrictDistribution=false" \
  --conf "spark.sql.streaming.maxBatchesToRetainInMemory=1" \
  --verbose \
  /opt/spark-apps/simple_credit_card_analytics.py

echo "✅ Simple Spark job submitted!"
