import json
import logging

from app.udaconnect.schemas import LocationSchema
from app.udaconnect.kafka_producer import KafkaService
from flask import request, Response
from flask_accepts import accepts, responds
from flask_restx import Namespace, Resource

DATE_FORMAT = "%Y-%m-%d"

api = Namespace("UdaConnect Locations", description="Provide geoData from mobile devices")  # noqa
logger = logging.getLogger("udaconnect-api")

@api.route("/locations")
class LocationResource(Resource):
    @accepts(schema=LocationSchema, api=api)
    @responds(schema=LocationSchema)
    def post(self) -> Response:
        location_data = request.get_json()  # JSON-Daten abrufen
        if not location_data:
            return Response(json.dumps({"error": "Invalid payload"}), status=400, mimetype="application/json")

        # Sende Daten an Kafka
        try:
            KafkaService.write_message(location_data)
            logger.info("Location data successfully sent to Kafka.")
        except Exception as e:
            logger.error(f"Failed to send location data to Kafka: {e}")
            return Response(json.dumps({"error": "Internal Server Error"}), status=500, mimetype="application/json")

        return Response(json.dumps({"message": "Location queued successfully"}), status=202, mimetype="application/json")