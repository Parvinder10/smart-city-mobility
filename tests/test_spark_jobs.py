# test_spark_jobs.py
import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

@pytest.fixture(scope="session")
def spark():
    """Provides a localized Spark session for unit testing."""
    return SparkSession.builder \
        .master("local[*]") \
        .appName("pyspark-unit-tests") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()

def run_sensor_cdc_logic(source_df, target_df):
    """Business logic isolated from JDBC writes for unit testing."""
    joined_df = source_df.join(
        target_df,
        source_df.sensor_id == target_df.sensor_id,
        "left"
    )

    new_records = joined_df.filter(target_df.sensor_id.isNull()).select(
        source_df.sensor_id,
        source_df.location_name,
        source_df.latitude,
        source_df.longitude,
        source_df.status
    )

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
    
    return new_records, changed_records

def test_scd2_sensor_processing(spark):
    # Mock operational sensors (Source)
    source_data = [
        ("SNS-001", "Broadway & 42nd St", 40.75797, -73.98554, "ACTIVE"), # Unchanged
        ("SNS-002", "5th Ave & 59th St", 40.76435, -73.97297, "MAINTENANCE"), # Changed status
        ("SNS-006", "8th Ave & 14th St", 40.73999, -74.00111, "ACTIVE") # New sensor
    ]
    source_df = spark.createDataFrame(
        source_data, 
        ["sensor_id", "location_name", "latitude", "longitude", "status"]
    )

    # Mock current active dimension state (Target)
    target_data = [
        (1, "SNS-001", "Broadway & 42nd St", 40.75797, -73.98554, "ACTIVE", datetime(2026, 1, 1), None, True),
        (2, "SNS-002", "5th Ave & 59th St", 40.76435, -73.97297, "ACTIVE", datetime(2026, 1, 1), None, True)
    ]
    target_df = spark.createDataFrame(
        target_data,
        ["sensor_key", "sensor_id", "location_name", "latitude", "longitude", "status", "start_date", "end_date", "is_current"]
    )

    new_df, changed_df = run_sensor_cdc_logic(source_df, target_df)

    # Validate new records
    new_rows = new_df.collect()
    assert len(new_rows) == 1
    assert new_rows[0]["sensor_id"] == "SNS-006"

    # Validate changed records
    changed_rows = changed_df.collect()
    assert len(changed_rows) == 1
    assert changed_rows[0]["sensor_key"] == 2
    assert changed_rows[0]["status"] == "MAINTENANCE"
