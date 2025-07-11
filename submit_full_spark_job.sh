#!/bin/bash
# Submit Full Credit Card Analytics with Delta Lake

echo "🚀 Submitting FULL Credit Card Analytics with Delta Lake..."

# Stop any existing Spark job first
echo "Stopping any existing Spark applications..."
docker exec spark-master /opt/bitnami/spark/bin/spark-submit --kill || true

# Wait a bit for cleanup
sleep 5

# Submit the FULL job with Delta Lake
docker exec spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --driver-memory 1g \
  --executor-memory 2g \
  --executor-cores 2 \
  --total-executor-cores 4 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,io.delta:delta-core_2.12:2.4.0 \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --conf "spark.sql.streaming.forceDeleteTempCheckpointLocation=true" \
  --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
  --conf "spark.sql.streaming.schemaInference=true" \
  --conf "spark.sql.adaptive.enabled=true" \
  --conf "spark.sql.adaptive.coalescePartitions.enabled=true" \
  --conf "spark.sql.streaming.stateStore.providerClass=org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider" \
  --conf "spark.dynamicAllocation.enabled=false" \
  --conf "spark.shuffle.service.enabled=false" \
  --conf "spark.ui.showConsoleProgress=false" \
  --verbose \
  /opt/spark-apps/credit_card_analytics.py

echo "✅ Full Spark job with Delta Lake submitted!"
