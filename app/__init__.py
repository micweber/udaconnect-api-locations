from flask import Flask, jsonify
from flask_cors import CORS
from flask_restx import Api
from flask_sqlalchemy import SQLAlchemy
import logging
import sys
import threading

db = SQLAlchemy()


def create_app(env=None):
    from app.config import config_by_name
    from app.routes import register_routes
    from app.udaconnect.kafka_consumer import consume_messages

    app = Flask(__name__)
    app.config.from_object(config_by_name[env or "test"])
    api = Api(app, title="UdaConnect API", version="0.1.0")

    CORS(app)  # Set CORS for development

    register_routes(api, app)
    db.init_app(app)

    @app.route("/health")
    def health():
        return jsonify("healthy")
    
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    logger = logging.getLogger(__name__)
    logger.info("#2 - Microservice Locations")
    
    kafka_thread = threading.Thread(target=consume_messages, daemon=True)
    kafka_thread.start()

    return app
