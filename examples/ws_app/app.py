import atexit
import os

from flask import Flask
from werkzeug.exceptions import HTTPException

from flask_mqtt.config import MQTTConfig
from flask_mqtt.flaskmqtt import FlaskMQTT

from ..json_provider import JSONProvider
from .dependencies import WS_SUBSCRIBERS_KEY
from .mqtt_ws_client import DynamicMQTTClient
from .router import mqtt_router, sock

# NOTE: Websocket support is provided by `flask-sock`,
# run it with a server that supports it, like the werkzeug dev server:
# `flask --app examples.ws_app.app run --port 8000`

# Run MQTT broker in background for tests with:
# `docker run -d --name mosquitto -p 9001:9001 -p 1883:1883 eclipse-mosquitto:1.6.15`
TEST_BROKER_HOST = os.getenv("TEST_BROKER_HOST", default="localhost")


def create_app():
    """Example Flask app with dynamic MQTT client."""
    flask_mqtt = FlaskMQTT(config=MQTTConfig(host=TEST_BROKER_HOST))

    ws_subscribers = DynamicMQTTClient(flask_mqtt)

    app = Flask(__name__)
    app.json = JSONProvider(app)

    @app.errorhandler(HTTPException)
    def _http_error(exc: HTTPException):
        """Unknown paths and wrong methods answer JSON, as they always did."""
        return {"detail": exc.name}, exc.code

    @flask_mqtt.on_message()
    async def _process_message(_client, topic, payload, qos, properties):
        """
        Common method to dispatch all received MQTT messages.

        If there are multiple subscriptions that match the same topic of the
        received message, this method will be called multiple times.

        Example:
             * Client is subscribed to 'test/#' and 'test/hello/+'
             * Broker receives a message with topic 'test/hello/there'
             * This method is called 2 times with the same topic and payload,
               **one for each topic match** in the client subscriptions.
        """
        num_clients_send_to = ws_subscribers.send_mqtt_msg(flask_mqtt, topic, payload.decode())
        app.logger.info(
            "Received message: %s '%s' QoS=%s properties=%s. Broadcasted to %d ws clients",
            topic,
            payload.decode(),
            qos,
            properties,
            num_clients_send_to,
        )
        return num_clients_send_to

    app.register_blueprint(mqtt_router)
    sock.init_app(app)

    app.extensions[WS_SUBSCRIBERS_KEY] = ws_subscribers
    # connects the MQTT client once every handler is registered
    flask_mqtt.init_app(app)
    atexit.register(ws_subscribers.close)

    return app


application = create_app()
