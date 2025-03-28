import os
import json
import logging
from kafka import KafkaConsumer
from typing import Dict
from app.udaconnect.models import Location
from app.udaconnect.schemas import LocationSchema
from geoalchemy2.functions import ST_Point

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("udaconnect-api")

# Kafka-Parameter
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "locations")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "my-kafka-group")

def consume_messages():

    # Kafka-Consumer as background process
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="earliest",
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )

    logging.info(f"Kafka consumer started: Listening on {KAFKA_TOPIC}...")

    try:
        for msg in consumer:
            logging.info(f"Received message: {msg.value}")
            #LocationServiceKafka.create(msg.value)
    except Exception as e:
        logging.error(f"Error in Kafka consumer: {e}")
    finally:
        consumer.close()


class LocationServiceKafka:
    @staticmethod
    def create(location: Dict) -> Location:

        from app import create_app
        from app import db

        # Application Context setzen
        app = create_app()
        with app.app_context():
        
            validation_results: Dict = LocationSchema().validate(location)
            if validation_results:
                logger.warning(f"Unexpected data format in payload: {validation_results}")
                raise Exception(f"Invalid payload: {validation_results}")

            new_location = Location()
            new_location.person_id = location["person_id"]
            new_location.creation_time = location["creation_time"]
            new_location.coordinate = ST_Point(location["latitude"], location["longitude"])
            db.session.add(new_location)
            db.session.commit()

            return new_location
