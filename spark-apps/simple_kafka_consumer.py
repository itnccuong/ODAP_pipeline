from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, concat_ws, split, when, 
    dayofweek, to_date, current_timestamp, lit
)
from pyspark.sql.types import *

def main():
    # Configuration
    KAFKA_SERVERS = "kafka-broker:29092"
    TOPIC_NAME = "credit-card-transactions"
    OUTPUT_PATH = "hdfs://namenode:9000/delta-lake/credit-card-analytics"
    CHECKPOINT_BASE_PATH = "hdfs://namenode:9000/checkpoints/credit-card-analytics"
    
    print("🚀 Starting Simple Kafka to Console Application...")
    
    spark = SparkSession.builder \
        .appName("SimpleKafkaConsumer") \
        .config("spark.executor.memory", "2g") \
        .config("spark.executor.cores", "2") \
        .config("spark.cores.max", "2") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print("✅ Spark Session initialized")
    
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
    
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
        .option("subscribe", TOPIC_NAME) \
        .option("startingOffsets", "latest") \
        .load()
    
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
    
    df = enriched_df
    
    print(f"✅ Kafka stream created for topic: {TOPIC_NAME} with enrichment transformations")
    
    csv_output_path = f"{OUTPUT_PATH}/simple_consumer/csv_output"
    csv_checkpoint_path = f"{CHECKPOINT_BASE_PATH}/simple_consumer/csv_checkpoint"
    
    # Write the stream to console
    console_query = df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()
    
    print("✅ Console stream started. Printing messages to console...")
    
    # Write the stream to CSV files in HDFS
    csv_query = df \
        .writeStream \
        .outputMode("append") \
        .format("csv") \
        .option("path", csv_output_path) \
        .option("checkpointLocation", csv_checkpoint_path) \
        .option("header", "true") \
        .trigger(processingTime="10 seconds") \
        .start()
    
    print(f"✅ CSV stream started. Writing data to: {csv_output_path}")
    
    # Wait for the streams to terminate
    try:
        print("⏵️ Streaming is active. Press Ctrl+C to stop...")
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("⏹️ Stopping streams due to user interruption...")
        console_query.stop()
        csv_query.stop()
        print("✅ All streams stopped gracefully.")

if __name__ == "__main__":
    main()
