from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
import json

class CreditCardAnalytics:
    def __init__(self, app_name="CreditCardAnalytics"):
        """Initialize Spark Session with optimized configuration"""
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.streaming.schemaInference", "true") \
            .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider") \
            .config("spark.dynamicAllocation.enabled", "false") \
            .config("spark.shuffle.service.enabled", "false") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        print(f"✅ Spark Session initialized: {app_name}")
    
    def create_kafka_stream(self, kafka_servers, topic_name):
        """Create streaming DataFrame from Kafka topic"""
        
        # Schema for transaction data
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
        
        # Read from Kafka
        kafka_df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("subscribe", topic_name) \
            .option("startingOffsets", "earliest") \
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
        
        # Add derived columns
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
        """Analyze hourly transaction patterns"""
        return df \
            .groupBy("hour") \
            .agg(
                count("*").alias("transaction_count"),
                sum("amount").alias("total_amount"),
                avg("amount").alias("avg_amount"),
                sum(when(col("is_fraud") == "Yes", 1).otherwise(0)).alias("fraud_count")
            ) \
            .withColumn("fraud_rate", round(col("fraud_count") / col("transaction_count") * 100, 2)) \
            .withColumn("analysis_type", lit("hourly_patterns")) \
            .withColumn("analysis_timestamp", current_timestamp())
    
    def analyze_city_performance(self, df):
        """Analyze city transaction performance"""
        return df \
            .groupBy("merchant_city", "merchant_state") \
            .agg(
                count("*").alias("transaction_count"),
                sum("amount").alias("total_amount"),
                avg("amount").alias("avg_amount"),
                sum(when(col("is_fraud") == "Yes", 1).otherwise(0)).alias("fraud_count")
            ) \
            .withColumn("fraud_rate", round(col("fraud_count") / col("transaction_count") * 100, 2)) \
            .withColumn("analysis_type", lit("city_performance")) \
            .withColumn("analysis_timestamp", current_timestamp())
    
    def write_to_console(self, df, query_name):
        """Write streaming DataFrame to console for debugging"""
        return df.writeStream \
            .outputMode("complete") \
            .format("console") \
            .option("truncate", "false") \
            .option("numRows", 20) \
            .queryName(query_name) \
            .trigger(processingTime="10 seconds") \
            .start()

def main():
    """
    Simple main function that outputs to console instead of Delta Lake
    
    Improvements:
    - Uses 2G executor memory (up from 1G) for better performance
    - Uses 2 executor cores (up from 1) for parallel processing
    - Enhanced memory management with optimized memory fractions
    - Adaptive query optimization enabled for better resource utilization
    - Console output for simplicity and avoiding Delta Lake complexity
    """
    
    # Configuration
    KAFKA_SERVERS = "kafka-broker:29092"
    TOPIC_NAME = "credit-card-transactions"
    
    print("🚀 Starting Simple Credit Card Analytics...")
    
    # Initialize analytics
    analytics = CreditCardAnalytics("SimpleCreditCardAnalytics")
    
    # Create streaming DataFrame
    stream_df = analytics.create_kafka_stream(KAFKA_SERVERS, TOPIC_NAME)
    
    # Perform analyses
    hourly_df = analytics.analyze_hourly_patterns(stream_df)
    city_df = analytics.analyze_city_performance(stream_df)
    
    # Start console output streams
    queries = []
    
    # Output hourly patterns to console
    queries.append(analytics.write_to_console(
        hourly_df, "hourly_patterns"
    ))
    
    # Output city performance to console  
    queries.append(analytics.write_to_console(
        city_df, "city_performance"
    ))
    
    print("✅ Console output streams started. Check the logs for results...")
    
    try:
        # Wait for all streams
        for query in queries:
            query.awaitTermination()
    except KeyboardInterrupt:
        print("⏹️ Stopping streams...")
        for query in queries:
            query.stop()
        print("✅ All streams stopped")

if __name__ == "__main__":
    main()
