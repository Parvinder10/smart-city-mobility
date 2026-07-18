# dlq_logger.py
import json
import logging
from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = ["kafka:29092"]
DLQ_TOPIC = "dead-letter-topic"

def main():
    logger.info("Initializing Dead-Letter Queue listener...")
    try:
        consumer = KafkaConsumer(
            DLQ_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: m.decode('utf-8', errors='ignore'),
            auto_offset_reset='earliest',
            group_id='dlq-monitoring-group'
        )
        logger.info(f"Subscribed to topic: {DLQ_TOPIC}. Awaiting messages...")
        for message in consumer:
            logger.warning(f"CRITICAL INVALID PAYLOAD RECEIVED in DLQ: {message.value}")
    except Exception as e:
        logger.error(f"Error executing DLQ logger: {e}")

if __name__ == "__main__":
    main()
