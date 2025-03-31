import os
import json
import logging
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("udaconnect-api")

# Kafka-Parameter
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "locations")


class KafkaService:

    @staticmethod
    def write_message(message):
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')  # JSON serialisieren
        )

        try:
            future = producer.send(KAFKA_TOPIC, value=message)
            future.get(timeout=10)  # Warten auf Bestätigung, Timeout nach 10s
            logger.info("Kafka message sent successfully.")
        except Exception as e:
            logger.error(f"Error whne sending Kafka message: {e}")
        finally:
            producer.flush()
            producer.close()
