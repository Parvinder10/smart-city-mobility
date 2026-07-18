# test_validation.py
import pytest

def validate_traffic_record(record):
    """Python representation of Spark validation logic."""
    if not isinstance(record, dict):
        return False, "Malformed payload"
    
    required_fields = ["sensor_id", "speed", "vehicle_volume", "occupancy", "congestion_index", "timestamp"]
    for f in required_fields:
        if f not in record or record[f] is None:
            return False, f"Missing required field: {f}"
            
    if record["speed"] < 0:
        return False, "Speed cannot be negative"
        
    return True, ""

def validate_gps_record(record):
    """Python representation of GPS tracking validation logic."""
    if not isinstance(record, dict):
        return False, "Malformed payload"
        
    required_fields = ["vehicle_id", "latitude", "longitude", "speed", "timestamp"]
    for f in required_fields:
        if f not in record or record[f] is None:
            return False, f"Missing required field: {f}"
            
    if not (-90.0 <= record["latitude"] <= 90.0):
        return False, "Latitude out of bounds"
        
    if not (-180.0 <= record["longitude"] <= 180.0):
        return False, "Longitude out of bounds"
        
    if record["speed"] < 0:
        return False, "Speed cannot be negative"
        
    return True, ""

# Unit Tests
def test_valid_traffic_record():
    record = {
        "sensor_id": "SNS-001",
        "speed": 35.5,
        "vehicle_volume": 12,
        "occupancy": 0.15,
        "congestion_index": 0.2,
        "timestamp": "2026-07-17T10:30:00"
    }
    is_valid, err = validate_traffic_record(record)
    assert is_valid is True
    assert err == ""

def test_invalid_traffic_record_missing_id():
    record = {
        "speed": 35.5,
        "vehicle_volume": 12,
        "occupancy": 0.15,
        "congestion_index": 0.2,
        "timestamp": "2026-07-17T10:30:00"
    }
    is_valid, err = validate_traffic_record(record)
    assert is_valid is False
    assert "sensor_id" in err

def test_invalid_traffic_record_negative_speed():
    record = {
        "sensor_id": "SNS-001",
        "speed": -5.0,
        "vehicle_volume": 12,
        "occupancy": 0.15,
        "congestion_index": 0.2,
        "timestamp": "2026-07-17T10:30:00"
    }
    is_valid, err = validate_traffic_record(record)
    assert is_valid is False
    assert "Speed" in err

def test_valid_gps_record():
    record = {
        "vehicle_id": "VH-001",
        "latitude": 40.75797,
        "longitude": -73.98554,
        "speed": 15.0,
        "timestamp": "2026-07-17T10:30:00"
    }
    is_valid, err = validate_gps_record(record)
    assert is_valid is True

def test_invalid_gps_record_oob_latitude():
    record = {
        "vehicle_id": "VH-001",
        "latitude": 105.0,
        "longitude": -73.98554,
        "speed": 15.0,
        "timestamp": "2026-07-17T10:30:00"
    }
    is_valid, err = validate_gps_record(record)
    assert is_valid is False
    assert "Latitude" in err
