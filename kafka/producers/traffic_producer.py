# traffic_producer.py
import json
import time
import random
import logging
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = ["kafka:29092"]
TRAFFIC_TOPIC = "traffic-sensors-topic"
GPS_TOPIC = "gps-tracking-topic"

SENSORS = [
    {"id": "SNS-001", "name": "Broadway & 42nd St", "lat": 40.75797, "lon": -73.98554},
    {"id": "SNS-002", "name": "5th Ave & 59th St", "lat": 40.76435, "lon": -73.97297},
    {"id": "SNS-003", "name": "7th Ave & 34th St", "lat": 40.75056, "lon": -73.99124},
    {"id": "SNS-004", "name": "FDR Dr & E 96th St", "lat": 40.78166, "lon": -73.94052},
    {"id": "SNS-005", "name": "West Side Hwy & 14th St", "lat": 40.74244, "lon": -74.00938}
]

VEHICLES = [
    {"id": "VH-001", "route": "RT-101", "lat": 40.75797, "lon": -73.98554},
    {"id": "VH-002", "route": "RT-101", "lat": 40.76435, "lon": -73.97297},
    {"id": "VH-003", "route": "RT-102", "lat": 40.75056, "lon": -73.99124},
    {"id": "VH-004", "route": None, "lat": 40.78166, "lon": -73.94052},
    {"id": "VH-005", "route": None, "lat": 40.74244, "lon": -74.00938}
]

def get_kafka_producer(servers, retries=5, delay=5):
    """Attempts to connect to Kafka broker with retry logic."""
    for attempt in range(1, retries + 1):
        try:
            logger.info(f"Connecting to Kafka brokers (Attempt {attempt}/{retries})...")
            producer = KafkaProducer(
                bootstrap_servers=servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3
            )
            logger.info("Successfully connected to Kafka.")
            return producer
        except NoBrokersAvailable:
            if attempt == retries:
                logger.error("Failed to connect to Kafka after maximum retries.")
                raise
            logger.warning(f"Kafka not ready. Retrying in {delay}s...")
            time.sleep(delay)

def generate_traffic_data():
    """Generates sensor reading metrics with congestion calculation based on time of day."""
    sensor = random.choice(SENSORS)
    now = datetime.now()
    hour = now.hour
    
    # Calculate congestion based on typical rush hours (7-9 AM, 4-6 PM)
    is_peak = (7 <= hour <= 9) or (16 <= hour <= 18)
    
    if is_peak:
        speed = round(random.uniform(5.0, 20.0), 1)  # slow speeds
        volume = random.randint(30, 80)
        occupancy = round(random.uniform(0.4, 0.95), 2)
    else:
        speed = round(random.uniform(25.0, 55.0), 1)  # normal flow
        volume = random.randint(5, 30)
        occupancy = round(random.uniform(0.05, 0.35), 2)
        
    # Congestion Index: 1.0 is fully congested, 0.0 is empty
    congestion_index = round(max(0.0, 1.0 - (speed / 55.0)), 2)
    
    # Introduce faulty data injection for DLQ testing (2% probability)
    inject_fault = random.random() < 0.02
    if inject_fault:
        fault_type = random.choice(["negative_speed", "missing_sensor_id", "malformed_json"])
        if fault_type == "negative_speed":
            speed = -15.0
        elif fault_type == "missing_sensor_id":
            return {
                "location_name": sensor["name"],
                "speed": speed,
                "vehicle_volume": volume,
                "occupancy": occupancy,
                "congestion_index": congestion_index,
                "timestamp": now.isoformat()
            }
        elif fault_type == "malformed_json":
            # Return string directly to cause json parse failures
            return "MALFORMED_JSON_STRING_123"

    return {
        "sensor_id": sensor["id"],
        "speed": speed,
        "vehicle_volume": volume,
        "occupancy": occupancy,
        "congestion_index": congestion_index,
        "timestamp": now.isoformat()
    }

def generate_gps_data():
    """Generates a vehicle GPS coordinates update."""
    vehicle = random.choice(VEHICLES)
    now = datetime.now()
    
    # Simulate a small movement
    lat_drift = random.uniform(-0.002, 0.002)
    lon_drift = random.uniform(-0.002, 0.002)
    current_lat = vehicle["lat"] + lat_drift
    current_lon = vehicle["lon"] + lon_drift
    
    # Update state for continuity
    vehicle["lat"] = current_lat
    vehicle["lon"] = current_lon
    
    speed = round(random.uniform(0.0, 45.0), 1)
    
    # 2% probability of faulty GPS ping (e.g. invalid latitude/longitude)
    inject_fault = random.random() < 0.02
    if inject_fault:
        current_lat = 999.9  # Invalid lat

    return {
        "vehicle_id": vehicle["id"],
        "route_id": vehicle["route"],
        "latitude": current_lat,
        "longitude": current_lon,
        "speed": speed,
        "timestamp": now.isoformat()
    }

def main():
    producer = get_kafka_producer(KAFKA_BOOTSTRAP_SERVERS)
    
    logger.info("Starting real-time city traffic simulation...")
    while True:
        try:
            # 1. Send sensor reading
            traffic_payload = generate_traffic_data()
            if isinstance(traffic_payload, str): # malformed direct send
                producer.send(TRAFFIC_TOPIC, value=traffic_payload.encode('utf-8'))
            else:
                producer.send(TRAFFIC_TOPIC, value=traffic_payload)
            
            # 2. Send GPS reading
            gps_payload = generate_gps_data()
            producer.send(GPS_TOPIC, value=gps_payload)
            
            producer.flush()
            logger.debug("Successfully pushed telemetry records to Kafka.")
            
            # Rest between pings (simulating real telemetry cycles)
            time.sleep(random.uniform(1.0, 3.0))
        except Exception as e:
            logger.error(f"Error during simulation execution: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()
