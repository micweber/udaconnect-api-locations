import os
import logging
import json
from typing import Dict

from app import db
from app.udaconnect.models import Location
from app.udaconnect.schemas import LocationSchema
from geoalchemy2.functions import ST_AsText, ST_Point
from kafka import KafkaProducer


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("udaconnect-api")


class LocationService:
    @staticmethod
    def retrieve(location_id) -> Location:
        location, coord_text = (
            db.session.query(Location, Location.coordinate.ST_AsText())
            .filter(Location.id == location_id)
            .one()
        )

        # Rely on database to return text form of point to reduce overhead of conversion in app code
        location.wkt_shape = coord_text
        return location


    @staticmethod
    def create(location: Dict) -> Location:
        validation_results: Dict = LocationSchema().validate(location)
        if validation_results:
            logger.warning(f"Unexpected data format in payload: {validation_results}")
            raise Exception(f"Invalid payload: {validation_results}")

        KafkaService.write_message(location)

        new_location = Location()
        new_location.person_id = location["person_id"]
        new_location.creation_time = location["creation_time"]
        new_location.coordinate = ST_Point(location["latitude"], location["longitude"])

        return new_location


class KafkaService:

    # Kafka-Parameter
    KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "locations")
    KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "my-kafka-group")

    @staticmethod
    def write_message(message):
        producer = KafkaProducer(
                KafkaService.KAFKA_TOPIC,
                bootstrap_servers=KafkaService.KAFKA_BROKER,
                group_id=KafkaService.KAFKA_GROUP_ID,
                auto_offset_reset="earliest",
                value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Messages will be serializes as JSON
        )
    
        # senden message
        producer.send(KafkaService.KAFKA_TOPIC, message)
        producer.flush()
        producer.close()
