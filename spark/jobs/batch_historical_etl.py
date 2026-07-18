# batch_historical_etl.py
import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

# DB Connection Config
DB_URL = os.getenv("DB_URL", "jdbc:postgresql://postgres:5432/mobility_dw")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

def create_spark_session():
    return SparkSession.builder \
        .appName("SmartCity-Mobility-Batch-ETL") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .getOrCreate()

def execute_jdbc_update(spark, query):
    """Executes an update query on the PostgreSQL warehouse using JVM JDBC driver directly."""
    try:
        jvm = spark._sc._gateway.jvm
        # Register driver
        jvm.java.lang.Class.forName("org.postgresql.Driver")
        conn = jvm.java.sql.DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)
        stmt = conn.createStatement()
        stmt.executeUpdate(query)
        stmt.close()
        conn.close()
    except Exception as e:
        print(f"Error running JDBC SQL update: {e}", file=sys.stderr)
        raise

def log_audit(spark, job_name, status, start_time, end_time, read_count, write_count, error_msg=None):
    """Inserts a pipeline run audit record into dw.etl_audit_log."""
    start_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
    end_str = end_time.strftime('%Y-%m-%d %H:%M:%S') if end_time else 'NULL'
    error_val = f"'{error_msg}'" if error_msg else "NULL"
    
    query = f"""
    INSERT INTO dw.etl_audit_log (job_name, status, start_time, end_time, records_read, records_written, error_message)
    VALUES ('{job_name}', '{status}', '{start_str}', {'NULL' if end_time is None else f"'{end_str}'"}, {read_count}, {write_count}, {error_val})
    """
    execute_jdbc_update(spark, query)

def run_scd2_sensors(spark, start_time):
    print("Running SCD Type 2 for Sensors Dimension...")
    
    # 1. Load operational sensors (Source)
    source_df = spark.read \
        .format("jdbc") \
        .option("url", DB_URL) \
        .option("dbtable", "oltp.sensors") \
        .option("user", DB_USER) \
        .option("password", DB_PASSWORD) \
        .load()

    # 2. Load target sensors dim (Active records only)
    target_df = spark.read \
        .format("jdbc") \
        .option("url", DB_URL) \
        .option("dbtable", "dw.dim_sensors") \
        .option("user", DB_USER) \
        .option("password", DB_PASSWORD) \
        .load() \
        .filter(col("is_current") == True)

    # 3. Detect Changes
    # Join on sensor_id to compare fields
    joined_df = source_df.join(
        target_df,
        source_df.sensor_id == target_df.sensor_id,
        "left"
    )

    # New records (sensor_id doesn't exist in dw.dim_sensors)
    new_records = joined_df.filter(target_df.sensor_id.isNull()).select(
        source_df.sensor_id,
        source_df.location_name,
        source_df.latitude,
        source_df.longitude,
        source_df.status
    )

    # Changed records (sensor_id exists, but status or location details changed)
    changed_records = joined_df.filter(
        target_df.sensor_id.isNotNull() & (
            (source_df.location_name != target_df.location_name) |
            (source_df.latitude != target_df.latitude) |
            (source_df.longitude != target_df.longitude) |
            (source_df.status != target_df.status)
        )
    ).select(
        target_df.sensor_key,
        source_df.sensor_id,
        source_df.location_name,
        source_df.latitude,
        source_df.longitude,
        source_df.status
    )

    records_read = source_df.count()
    records_written = 0

    # 4. Handle Changed Records: Expire and Insert
    if not changed_records.isEmpty():
        keys_to_expire = [row['sensor_key'] for row in changed_records.select("sensor_key").collect()]
        keys_str = ",".join([str(k) for k in keys_to_expire])
        
        # Expire old records
        expire_query = f"""
        UPDATE dw.dim_sensors 
        SET end_date = CURRENT_TIMESTAMP, is_current = FALSE 
        WHERE sensor_key IN ({keys_str})
        """
        execute_jdbc_update(spark, expire_query)
        print(f"Expired {len(keys_to_expire)} sensor records.")

        # Prepare new records for insertion
        to_insert_df = changed_records.select(
            col("sensor_id"),
            col("location_name"),
            col("latitude"),
            col("longitude"),
            col("status")
        ).withColumn("start_date", lit(start_time)) \
         .withColumn("is_current", lit(True))

        to_insert_df.write \
            .format("jdbc") \
            .option("url", DB_URL) \
            .option("dbtable", "dw.dim_sensors") \
            .option("user", DB_USER) \
            .option("password", DB_PASSWORD) \
            .mode("append") \
            .save()
        
        records_written += to_insert_df.count()

    # 5. Handle New Records: Insert
    if not new_records.isEmpty():
        to_insert_df = new_records.withColumn("start_date", lit(start_time)) \
                                  .withColumn("is_current", lit(True))

        to_insert_df.write \
            .format("jdbc") \
            .option("url", DB_URL) \
            .option("dbtable", "dw.dim_sensors") \
            .option("user", DB_USER) \
            .option("password", DB_PASSWORD) \
            .mode("append") \
            .save()

        records_written += to_insert_df.count()
        print(f"Inserted {to_insert_df.count()} new sensor records.")

    return records_read, records_written

def run_scd2_vehicles(spark, start_time):
    print("Running SCD Type 2 for Vehicles Dimension...")
    
    # 1. Load operational vehicles
    source_df = spark.read \
        .format("jdbc") \
        .option("url", DB_URL) \
        .option("dbtable", "oltp.vehicles") \
        .option("user", DB_USER) \
        .option("password", DB_PASSWORD) \
        .load()

    # 2. Load target vehicles dim (Active only)
    target_df = spark.read \
        .format("jdbc") \
        .option("url", DB_URL) \
        .option("dbtable", "dw.dim_vehicles") \
        .option("user", DB_USER) \
        .option("password", DB_PASSWORD) \
        .load() \
        .filter(col("is_current") == True)

    joined_df = source_df.join(
        target_df,
        source_df.vehicle_id == target_df.vehicle_id,
        "left"
    )

    new_records = joined_df.filter(target_df.vehicle_id.isNull()).select(
        source_df.vehicle_id,
        source_df.license_plate,
        source_df.vehicle_type,
        source_df.operator_name,
        source_df.assigned_route_id,
        source_df.status
    )

    changed_records = joined_df.filter(
        target_df.vehicle_id.isNotNull() & (
            (source_df.license_plate != target_df.license_plate) |
            (source_df.assigned_route_id != target_df.assigned_route_id) |
            (source_df.status != target_df.status)
        )
    ).select(
        target_df.vehicle_key,
        source_df.vehicle_id,
        source_df.license_plate,
        source_df.vehicle_type,
        source_df.operator_name,
        source_df.assigned_route_id,
        source_df.status
    )

    records_read = source_df.count()
    records_written = 0

    if not changed_records.isEmpty():
        keys_to_expire = [row['vehicle_key'] for row in changed_records.select("vehicle_key").collect()]
        keys_str = ",".join([str(k) for k in keys_to_expire])
        
        expire_query = f"""
        UPDATE dw.dim_vehicles 
        SET end_date = CURRENT_TIMESTAMP, is_current = FALSE 
        WHERE vehicle_key IN ({keys_str})
        """
        execute_jdbc_update(spark, expire_query)
        print(f"Expired {len(keys_to_expire)} vehicle records.")

        to_insert_df = changed_records.select(
            col("vehicle_id"),
            col("license_plate"),
            col("vehicle_type"),
            col("operator_name"),
            col("assigned_route_id"),
            col("status")
        ).withColumn("start_date", lit(start_time)) \
         .withColumn("is_current", lit(True))

        to_insert_df.write \
            .format("jdbc") \
            .option("url", DB_URL) \
            .option("dbtable", "dw.dim_vehicles") \
            .option("user", DB_USER) \
            .option("password", DB_PASSWORD) \
            .mode("append") \
            .save()
        
        records_written += to_insert_df.count()

    if not new_records.isEmpty():
        to_insert_df = new_records.withColumn("start_date", lit(start_time)) \
                                  .withColumn("is_current", lit(True))

        to_insert_df.write \
            .format("jdbc") \
            .option("url", DB_URL) \
            .option("dbtable", "dw.dim_vehicles") \
            .option("user", DB_USER) \
            .option("password", DB_PASSWORD) \
            .mode("append") \
            .save()

        records_written += to_insert_df.count()
        print(f"Inserted {to_insert_df.count()} new vehicle records.")

    return records_read, records_written

def main():
    spark = create_spark_session()
    start_time = datetime.now()
    
    # 1. Run Sensors Dimension ETL
    try:
        r_read, r_written = run_scd2_sensors(spark, start_time)
        log_audit(spark, "SCD2_Sensors", "SUCCESS", start_time, datetime.now(), r_read, r_written)
    except Exception as e:
        log_audit(spark, "SCD2_Sensors", "FAILED", start_time, datetime.now(), 0, 0, str(e))
        print(f"Sensors ETL failed: {e}", file=sys.stderr)
        
    # 2. Run Vehicles Dimension ETL
    try:
        r_read, r_written = run_scd2_vehicles(spark, start_time)
        log_audit(spark, "SCD2_Vehicles", "SUCCESS", start_time, datetime.now(), r_read, r_written)
    except Exception as e:
        log_audit(spark, "SCD2_Vehicles", "FAILED", start_time, datetime.now(), 0, 0, str(e))
        print(f"Vehicles ETL failed: {e}", file=sys.stderr)

    spark.stop()

if __name__ == "__main__":
    main()
