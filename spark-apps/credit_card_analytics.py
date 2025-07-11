#!/usr/bin/env python3
"""
Spark Streaming Application for Credit Card Transaction Analysis
Đọc real-time data từ Kafka và thực hiện các phân tích theo yêu cầu đề bài
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
import json

class CreditCardAnalytics:
    def __init__(self, app_name="CreditCardAnalytics"):
        """Initialize Spark Session với Kafka và HDFS support"""
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.streaming.schemaInference", "true") \
            .config("spark.executor.memory", "2g") \
            .config("spark.executor.cores", "2") \
            .config("spark.cores.max", "4") \
            .config("spark.driver.memory", "1g") \
            .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        print(f"✅ Spark Session initialized: {app_name} with Delta Lake support")
    
    def create_kafka_stream(self, kafka_servers, topic_name):
        """Tạo streaming DataFrame từ Kafka topic"""
        
        # Schema cho transaction data
        transaction_schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("card_number", StringType(), True),
            StructField("year", IntegerType(), True),
            StructField("month", IntegerType(), True),
            StructField("day", IntegerType(), True),
            StructField("time", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("use_chip", StringType(), True),
            StructField("merchant_name", StringType(), True),
            StructField("merchant_city", StringType(), True),
            StructField("merchant_state", StringType(), True),
            StructField("zip_code", StringType(), True),
            StructField("mcc", StringType(), True),
            StructField("errors", StringType(), True),
            StructField("is_fraud", StringType(), True),
            StructField("ingested_at", StringType(), True),
            StructField("source", StringType(), True)
        ])
        
        # Đọc từ Kafka
        kafka_df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("subscribe", topic_name) \
            .option("startingOffsets", "latest") \
            .load()
        
        # Parse JSON data
        parsed_df = kafka_df.select(
            col("key").cast("string").alias("message_key"),
            col("value").cast("string").alias("message_value"),
            col("timestamp").alias("kafka_timestamp")
        ).select(
            col("message_key"),
            col("kafka_timestamp"),
            from_json(col("message_value"), transaction_schema).alias("transaction")
        ).select(
            col("message_key"),
            col("kafka_timestamp"),
            col("transaction.*")
        )
        
        # Thêm derived columns
        enriched_df = parsed_df \
            .withColumn("transaction_datetime", 
                       concat_ws(" ", 
                               concat_ws("-", col("year"), col("month"), col("day")),
                               col("time"))) \
            .withColumn("hour", split(col("time"), ":")[0].cast("int")) \
            .withColumn("is_weekend", 
                       when(dayofweek(to_date(concat_ws("-", col("year"), col("month"), col("day")), "yyyy-M-d")).isin([1, 7]), "Yes")
                       .otherwise("No")) \
            .withColumn("amount_category",
                       when(col("amount") < 50, "Small")
                       .when(col("amount") < 500, "Medium")
                       .when(col("amount") < 2000, "Large")
                       .otherwise("Very Large")) \
            .withColumn("processing_time", current_timestamp())
        
        print(f"✅ Kafka stream created for topic: {topic_name}")
        return enriched_df
    
    def analyze_hourly_patterns(self, df):
        """
        Yêu cầu 1: Thời điểm nào trong ngày có nhiều giao dịch nhất?
        """
        hourly_analysis = df \
            .groupBy("hour") \
            .agg(
                count("*").alias("transaction_count"),
                sum("amount").alias("total_amount"),
                avg("amount").alias("avg_amount"),
                # countDistinct("user_id").alias("unique_users"),
                sum(when(col("is_fraud") == "Yes", 1).otherwise(0)).alias("fraud_count")
            ) \
            .withColumn("fraud_rate", round(col("fraud_count") / col("transaction_count") * 100, 2)) \
            .withColumn("analysis_type", lit("hourly_patterns")) \
            .withColumn("analysis_timestamp", current_timestamp())
        
        return hourly_analysis
    
    def analyze_city_performance(self, df):
        """
        Yêu cầu 2: Thành phố nào có tổng giá trị giao dịch cao nhất?
        """
        city_analysis = df \
            .groupBy("merchant_city", "merchant_state") \
            .agg(
                count("*").alias("transaction_count"),
                sum("amount").alias("total_amount"),
                avg("amount").alias("avg_amount"),
                # countDistinct("user_id").alias("unique_users"),
                # countDistinct("merchant_name").alias("unique_merchants"),
                sum(when(col("is_fraud") == "Yes", 1).otherwise(0)).alias("fraud_count")
            ) \
            .withColumn("fraud_rate", round(col("fraud_count") / col("transaction_count") * 100, 2)) \
            .withColumn("analysis_type", lit("city_performance")) \
            .withColumn("analysis_timestamp", current_timestamp())
        
        return city_analysis
    
    def analyze_merchant_performance(self, df):
        """
        Yêu cầu 3: Merchant nào có số lượng hoặc giá trị giao dịch cao nhất?
        """
        merchant_analysis = df \
            .groupBy("merchant_name", "merchant_city", "mcc") \
            .agg(
                count("*").alias("transaction_count"),
                sum("amount").alias("total_amount"),
                avg("amount").alias("avg_amount"),
                # countDistinct("user_id").alias("unique_users"),
                sum(when(col("is_fraud") == "Yes", 1).otherwise(0)).alias("fraud_count")
            ) \
            .withColumn("fraud_rate", round(col("fraud_count") / col("transaction_count") * 100, 2)) \
            .withColumn("analysis_type", lit("merchant_performance")) \
            .withColumn("analysis_timestamp", current_timestamp())
        
        return merchant_analysis
    
    def analyze_fraud_patterns(self, df):
        """
        Yêu cầu 4,7: Phân tích fraud patterns
        """
        fraud_analysis = df \
            .filter(col("is_fraud") == "Yes") \
            .groupBy("hour", "merchant_city", "amount_category", "use_chip") \
            .agg(
                count("*").alias("fraud_count"),
                sum("amount").alias("fraud_amount"),
                avg("amount").alias("avg_fraud_amount"),
                # countDistinct("user_id").alias("unique_fraud_users")
            ) \
            .withColumn("analysis_type", lit("fraud_patterns")) \
            .withColumn("analysis_timestamp", current_timestamp())
        
        return fraud_analysis
    
    def analyze_user_behavior(self, df):
        """
        Yêu cầu 5,9: Phân tích user behavior patterns
        """
        user_analysis = df \
            .groupBy("user_id") \
            .agg(
                count("*").alias("transaction_count"),
                sum("amount").alias("total_spent"),
                avg("amount").alias("avg_transaction"),
                # countDistinct("merchant_city").alias("cities_visited"),
                # countDistinct("merchant_name").alias("merchants_used"),
                sum(when(col("is_fraud") == "Yes", 1).otherwise(0)).alias("fraud_count"),
                sum(when(col("errors") != "No", 1).otherwise(0)).alias("error_count")
            ) \
            .withColumn("fraud_rate", round(col("fraud_count") / col("transaction_count") * 100, 2)) \
            .withColumn("error_rate", round(col("error_count") / col("transaction_count") * 100, 2)) \
            .withColumn("analysis_type", lit("user_behavior")) \
            .withColumn("analysis_timestamp", current_timestamp())
        
        return user_analysis
    
    def analyze_weekend_vs_weekday(self, df):
        """
        Yêu cầu 8: So sánh ngày thường vs cuối tuần
        """
        weekend_analysis = df \
            .groupBy("is_weekend") \
            .agg(
                count("*").alias("transaction_count"),
                sum("amount").alias("total_amount"),
                avg("amount").alias("avg_amount"),
                # countDistinct("user_id").alias("unique_users"),
                sum(when(col("is_fraud") == "Yes", 1).otherwise(0)).alias("fraud_count")
            ) \
            .withColumn("fraud_rate", round(col("fraud_count") / col("transaction_count") * 100, 2)) \
            .withColumn("analysis_type", lit("weekend_analysis")) \
            .withColumn("analysis_timestamp", current_timestamp())
        
        return weekend_analysis
    
    def write_to_delta_lake(self, df, table_path, merge_keys, checkpoint_path):
        """
        Write streaming DataFrame to Delta Lake with merge/upsert logic
        """
        def merge_to_delta(batch_df, batch_id):
            if batch_df.count() > 0:
                try:
                    print(f"Processing batch {batch_id} with {batch_df.count()} records for table {table_path}")
                    
                    # Check if Delta table exists
                    if DeltaTable.isDeltaTable(self.spark, table_path):
                        # Load existing Delta table
                        delta_table = DeltaTable.forPath(self.spark, table_path)
                        
                        # Prepare merge conditions
                        merge_condition = " AND ".join([f"existing.{key} = updates.{key}" for key in merge_keys])
                        
                        # Perform upsert
                        (delta_table.alias("existing")
                         .merge(batch_df.alias("updates"), merge_condition)
                         .whenMatchedUpdateAll()
                         .whenNotMatchedInsertAll()
                         .execute())
                        
                        print(f"Batch {batch_id}: Successfully merged {batch_df.count()} records to {table_path}")
                    else:
                        # First time - create Delta table
                        batch_df.write.format("delta").mode("overwrite").save(table_path)
                        print(f"Batch {batch_id}: Created new Delta table with {batch_df.count()} records at {table_path}")
                        
                except Exception as e:
                    print(f"Error processing batch {batch_id} for {table_path}: {str(e)}")
                    # Write as append mode as fallback
                    try:
                        batch_df.write.format("delta").mode("append").save(table_path)
                        print(f"Batch {batch_id}: Fallback append successful for {table_path}")
                    except Exception as e2:
                        print(f"Fallback failed for batch {batch_id}: {str(e2)}")
        
        # Start streaming query with foreachBatch
        return (df.writeStream
                .foreachBatch(merge_to_delta)
                .option("checkpointLocation", checkpoint_path)
                .outputMode("update")
                .trigger(processingTime="6 seconds")
                .start())

def main():
    """Main execution function using Medallion Architecture (Bronze -> Gold)"""
    
    # Configuration
    KAFKA_SERVERS = "kafka-broker:29092"
    TOPIC_NAME = "credit-card-transactions"
    DELTA_OUTPUT_PATH = "hdfs://namenode:9000/delta-lake/credit-card-analytics"
    CHECKPOINT_BASE_PATH = "hdfs://namenode:9000/checkpoints/credit-card-analytics"
    
    print("🚀 Starting Credit Card Analytics Spark Streaming Application with Delta Lake...")
    
    # Initialize analytics class
    analytics = CreditCardAnalytics("CreditCardAnalytics")
    
    # Define paths for the bronze table - DO THIS EARLY
    bronze_table_path = f"{DELTA_OUTPUT_PATH}/bronze/transactions"
    bronze_checkpoint_path = f"{CHECKPOINT_BASE_PATH}/bronze/transactions"
    
    # --------------------------------------------------------------------------
    # STAGE 1: BRONZE LAYER - Ingest and Clean Data from Kafka
    # --------------------------------------------------------------------------
    
    print("🔹 [BRONZE] Setting up Kafka to Delta stream...")
    
    enriched_bronze_df = analytics.create_kafka_stream(KAFKA_SERVERS, TOPIC_NAME)
    
    # --- NEW: INITIALIZATION STEP ---
    # Before we start a continuous stream, we need to ensure the bronze table exists.
    # We run a one-time trigger that processes all available data from Kafka.
    # This will create the Delta table and its schema.
    print("🔹 [BRONZE] Initializing bronze table to ensure schema exists...")
    try:
        (enriched_bronze_df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", bronze_checkpoint_path)
            .trigger(availableNow=True) # Use availableNow=True for Spark 3.3+ or processAllAvailable() for older versions
            .start(bronze_table_path)
            .awaitTermination())
        print("✅ [BRONZE] Initialization complete. Bronze table created.")
    except Exception as e:
        # This might fail if no data is in Kafka. That's okay if the table already exists from a previous run.
        if "DeltaTable.isDeltaTable" in str(e): # A more robust check is needed in production
             print("ℹ️  [BRONZE] Bronze table likely already exists. Proceeding.")
        else:
            print(f"⚠️ [BRONZE] Initialization may have failed if no data in Kafka. Error: {e}")
            # If the table doesn't exist AND there's no data in Kafka, the next step will fail.
            # For a university project, ensure your producer runs first.

    # Now, start the continuous bronze stream for ongoing data
    print("🔹 [BRONZE] Starting continuous bronze stream...")
    bronze_query = (enriched_bronze_df.writeStream
                    .format("delta")
                    .outputMode("append")
                    .option("checkpointLocation", bronze_checkpoint_path)
                    .trigger(processingTime="10 seconds")
                    .start(bronze_table_path))
    
    print(f"✅ [BRONZE] Continuous stream started. Writing raw transactions to: {bronze_table_path}")
    
    # --------------------------------------------------------------------------
    # STAGE 2: GOLD LAYER - Create Aggregated Analytical Tables
    # --------------------------------------------------------------------------

    print("🔸 [GOLD] Setting up analytical streams from Bronze table...")

    # This will now succeed because the bronze table was created in the initialization step
    bronze_stream_df = analytics.spark.readStream.format("delta").load(bronze_table_path)

    # ... (The rest of your gold layer code remains exactly the same) ...
    # Perform various analyses on the single bronze stream
    hourly_df = analytics.analyze_hourly_patterns(bronze_stream_df)
    city_df = analytics.analyze_city_performance(bronze_stream_df)
    merchant_df = analytics.analyze_merchant_performance(bronze_stream_df)
    fraud_df = analytics.analyze_fraud_patterns(bronze_stream_df)
    user_df = analytics.analyze_user_behavior(bronze_stream_df)
    weekend_df = analytics.analyze_weekend_vs_weekday(bronze_stream_df)
    
    # Start the streaming queries for each Gold table
    gold_queries = []
    
    gold_queries.append(analytics.write_to_delta_lake(
        hourly_df, f"{DELTA_OUTPUT_PATH}/gold/hourly_patterns", ["hour"],
        f"{CHECKPOINT_BASE_PATH}/gold/hourly_patterns"))
    
    # ... (append all other gold queries as before) ...
    gold_queries.append(analytics.write_to_delta_lake(
        city_df, f"{DELTA_OUTPUT_PATH}/gold/city_performance", ["merchant_city", "merchant_state"],
        f"{CHECKPOINT_BASE_PATH}/gold/city_performance"))
    
    gold_queries.append(analytics.write_to_delta_lake(
        merchant_df, f"{DELTA_OUTPUT_PATH}/gold/merchant_performance", ["merchant_name", "merchant_city"],
        f"{CHECKPOINT_BASE_PATH}/gold/merchant_performance"))
    
    gold_queries.append(analytics.write_to_delta_lake(
        fraud_df, f"{DELTA_OUTPUT_PATH}/gold/fraud_patterns", ["hour", "merchant_city", "amount_category", "use_chip"],
        f"{CHECKPOINT_BASE_PATH}/gold/fraud_patterns"))
    
    gold_queries.append(analytics.write_to_delta_lake(
        user_df, f"{DELTA_OUTPUT_PATH}/gold/user_behavior", ["user_id"],
        f"{CHECKPOINT_BASE_PATH}/gold/user_behavior"))
    
    gold_queries.append(analytics.write_to_delta_lake(
        weekend_df, f"{DELTA_OUTPUT_PATH}/gold/weekend_analysis", ["is_weekend"],
        f"{CHECKPOINT_BASE_PATH}/gold/weekend_analysis"))
    
    print("✅ [GOLD] All analytical streams started. Monitoring...")
    
    # Await termination for ALL streams
    try:
        bronze_query.awaitTermination()
        # You technically don't need to await the gold queries here
        # because if the bronze query terminates, the app will exit.
        # But it's good practice for clarity.
        for query in gold_queries:
            query.awaitTermination()
    except KeyboardInterrupt:
        print("⏹️ Stopping all streams due to user interruption...")
        bronze_query.stop()
        for query in gold_queries:
            query.stop()
        print("✅ All streams stopped gracefully.")

if __name__ == "__main__":
    main()
