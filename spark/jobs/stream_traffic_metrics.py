# stream_traffic_metrics.py
import os
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, to_json, struct, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# DB Connection Config
DB_URL = os.getenv("DB_URL", "jdbc:postgresql://postgres:5432/mobility_dw")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

# schemas
traffic_schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("speed", DoubleType(), True),
    StructField("vehicle_volume", IntegerType(), True),
    StructField("occupancy", DoubleType(), True),
    StructField("congestion_index", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

gps_schema = StructType([
    StructField("vehicle_id", StringType(), True),
    StructField("route_id", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("speed", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

def create_spark_session():
    return SparkSession.builder \
        .appName("SmartCity-Mobility-Streaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.postgresql:postgresql:42.6.0") \
        .getOrCreate()

def process_batch(batch_df, batch_id, stream_type):
    """Processes a micro-batch of streaming data."""
    if batch_df.isEmpty():
        return

    spark = batch_df.sparkSession

    # Load active dimensions for lookups
    dim_sensors = spark.read \
        .format("jdbc") \
        .option("url", DB_URL) \
        .option("dbtable", "dw.dim_sensors") \
        .option("user", DB_USER) \
        .option("password", DB_PASSWORD) \
        .load() \
        .filter(col("is_current") == True)

    dim_vehicles = spark.read \
        .format("jdbc") \
        .option("url", DB_URL) \
        .option("dbtable", "dw.dim_vehicles") \
        .option("user", DB_USER) \
        .option("password", DB_PASSWORD) \
        .load() \
        .filter(col("is_current") == True)

    if stream_type == "traffic":
        # 1. Parse JSON
        parsed_df = batch_df.select(
            from_json(col("value").cast("string"), traffic_schema).alias("data"),
            col("value").cast("string").alias("raw_payload")
        ).select("data.*", "raw_payload")

        # 2. Validation rule
        # A record is invalid if: sensor_id is missing, speed is negative or null, occupancy is null, or it fails json parsing
        is_invalid_expr = (
            col("sensor_id").isNull() | 
            col("speed").isNull() | 
            (col("speed") < 0.0) | 
            col("vehicle_volume").isNull() |
            col("occupancy").isNull()
        )

        invalid_df = parsed_df.filter(is_invalid_expr | col("sensor_id").contains("MALFORMED"))
        valid_df = parsed_df.filter(~(is_invalid_expr | col("sensor_id").contains("MALFORMED")))

        # Process Invalid (Send to DLQ)
        if not invalid_df.isEmpty():
            dlq_df = invalid_df.select(
                col("raw_payload").alias("payload"),
                lit("Validation Failed: Missing ID, invalid metrics, or parsing error").alias("validation_error"),
                lit("traffic-sensors-topic").alias("source_topic")
            )
            # Write to DLQ Postgres Table
            dlq_df.write \
                .format("jdbc") \
                .option("url", DB_URL) \
                .option("dbtable", "dw.dead_letter_queue") \
                .option("user", DB_USER) \
                .option("password", DB_PASSWORD) \
                .mode("append") \
                .save()

            # Write to Kafka DLQ Topic
            dlq_df.select(to_json(struct("*")).alias("value")).write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "kafka:29092") \
                .option("topic", "dead-letter-topic") \
                .save()

        # Process Valid
        if not valid_df.isEmpty():
            # Match sensor_key
            enriched_df = valid_df.join(
                dim_sensors,
                valid_df.sensor_id == dim_sensors.sensor_id,
                "inner"
            )
            
            # Map time and date keys
            # Format: timestamp represents '2026-07-17T10:34:02' -> date_key: 20260717, time_key: 1034
            enriched_df = enriched_df.withColumn("ts", col("timestamp").cast(TimestampType())) \
                .withColumn("date_key", (col("ts").cast("string").substr(1, 4) + 
                                         col("ts").cast("string").substr(6, 2) + 
                                         col("ts").cast("string").substr(9, 2)).cast(IntegerType())) \
                .withColumn("time_key", (col("ts").cast("string").substr(12, 2) + 
                                         col("ts").cast("string").substr(15, 2)).cast(IntegerType()))
            
            fact_df = enriched_df.select(
                col("sensor_key"),
                col("date_key"),
                col("time_key"),
                col("speed"),
                col("vehicle_volume"),
                col("occupancy"),
                col("congestion_index"),
                col("ts").alias("record_timestamp")
            )

            fact_df.write \
                .format("jdbc") \
                .option("url", DB_URL) \
                .option("dbtable", "dw.fact_traffic_readings") \
                .option("user", DB_USER) \
                .option("password", DB_PASSWORD) \
                .mode("append") \
                .save()

    elif stream_type == "gps":
        parsed_df = batch_df.select(
            from_json(col("value").cast("string"), gps_schema).alias("data"),
            col("value").cast("string").alias("raw_payload")
        ).select("data.*", "raw_payload")

        # Validation Rule
        is_invalid_expr = (
            col("vehicle_id").isNull() | 
            col("latitude").isNull() | 
            (col("latitude") < -90.0) | (col("latitude") > 90.0) |
            col("longitude").isNull() | 
            (col("longitude") < -180.0) | (col("longitude") > 180.0) |
            col("speed").isNull() | 
            (col("speed") < 0.0)
        )

        invalid_df = parsed_df.filter(is_invalid_expr)
        valid_df = parsed_df.filter(~is_invalid_expr)

        # Process Invalid
        if not invalid_df.isEmpty():
            dlq_df = invalid_df.select(
                col("raw_payload").alias("payload"),
                lit("Validation Failed: Invalid coords or speed metrics").alias("validation_error"),
                lit("gps-tracking-topic").alias("source_topic")
            )
            dlq_df.write \
                .format("jdbc") \
                .option("url", DB_URL) \
                .option("dbtable", "dw.dead_letter_queue") \
                .option("user", DB_USER) \
                .option("password", DB_PASSWORD) \
                .mode("append") \
                .save()

            dlq_df.select(to_json(struct("*")).alias("value")).write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "kafka:29092") \
                .option("topic", "dead-letter-topic") \
                .save()

        # Process Valid
        if not valid_df.isEmpty():
            enriched_df = valid_df.join(
                dim_vehicles,
                valid_df.vehicle_id == dim_vehicles.vehicle_id,
                "inner"
            )
            
            enriched_df = enriched_df.withColumn("ts", col("timestamp").cast(TimestampType())) \
                .withColumn("date_key", (col("ts").cast("string").substr(1, 4) + 
                                         col("ts").cast("string").substr(6, 2) + 
                                         col("ts").cast("string").substr(9, 2)).cast(IntegerType())) \
                .withColumn("time_key", (col("ts").cast("string").substr(12, 2) + 
                                         col("ts").cast("string").substr(15, 2)).cast(IntegerType()))

            fact_df = enriched_df.select(
                col("vehicle_key"),
                col("date_key"),
                col("time_key"),
                col("latitude"),
                col("longitude"),
                col("speed"),
                col("ts").alias("record_timestamp")
            )

            fact_df.write \
                .format("jdbc") \
                .option("url", DB_URL) \
                .option("dbtable", "dw.fact_gps_pings") \
                .option("user", DB_USER) \
                .option("password", DB_PASSWORD) \
                .mode("append") \
                .save()

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Starting Spark Structured Streaming pipelines from Kafka...")

    # Kafka Streams
    traffic_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "traffic-sensors-topic") \
        .option("startingOffsets", "latest") \
        .load()

    gps_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "gps-tracking-topic") \
        .option("startingOffsets", "latest") \
        .load()

    # Query writers using foreachBatch
    traffic_query = traffic_stream.writeStream \
        .foreachBatch(lambda df, epoch_id: process_batch(df, epoch_id, "traffic")) \
        .option("checkpointLocation", "/tmp/spark-checkpoints-traffic") \
        .start()

    gps_query = gps_stream.writeStream \
        .foreachBatch(lambda df, epoch_id: process_batch(df, epoch_id, "gps")) \
        .option("checkpointLocation", "/tmp/spark-checkpoints-gps") \
        .start()

    traffic_query.awaitTermination()
    gps_query.awaitTermination()

if __name__ == "__main__":
    main()
