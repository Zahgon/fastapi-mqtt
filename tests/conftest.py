import atexit
import logging
import os
from collections import defaultdict
from typing import Any

import pytest
from flask import Flask
from gmqtt import Client as MQTTClient

from flask_mqtt.config import MQTTConfig
from flask_mqtt.flaskmqtt import FlaskMQTT

# Run MQTT broker in background for tests with:
# `docker run -d -p 9001:9001 -p 1883:1883 eclipse-mosquitto:1.6.15`
# set `TEST_BROKER_HOST=localhost` to use against a local broker
TEST_BROKER_HOST = os.getenv("TEST_BROKER_HOST", default="test.mosquitto.org")
TEST_BROKER_USER = "testuser" if TEST_BROKER_HOST != "test.mosquitto.org" else None
TEST_BROKER_PWD = "secret" if TEST_BROKER_HOST != "test.mosquitto.org" else None


@pytest.fixture
def test_app():  # noqa: C901
    """Fixture with example Flask app for tests."""
    mqtt_config = MQTTConfig(
        host=TEST_BROKER_HOST,
        will_message_topic="/WILL",
        will_message_payload="MQTT Connection is dead!",
        will_delay_interval=2,
        username=TEST_BROKER_USER,
        password=TEST_BROKER_PWD,
    )
    flask_mqtt = FlaskMQTT(config=mqtt_config)
    received_msgs: dict[str, int] = defaultdict(int)
    processed_msgs: dict[str, int] = defaultdict(int)

    app = Flask(__name__)

    @flask_mqtt.on_connect()
    def _connect(client: MQTTClient, flags: int, rc: int, properties: Any):
        client.subscribe("flask-mqtt")  # subscribing mqtt topic
        logging.info("Connected: %s %s %s %s", client, flags, rc, properties)

    @flask_mqtt.subscribe("$share/test/mqtt/+/temperature", "mqtt/+/humidity")
    async def _decorated_subscription(
        client: MQTTClient, topic: str, payload: bytes, qos: int, properties: Any
    ):
        """Subscription handler for temperature and humidity topics."""
        processed_msgs[topic] += 1
        logging.info(
            "temperature/humidity: %s %s %s %s",
            topic,
            payload.decode(),
            qos,
            properties,
        )
        return 0

    @flask_mqtt.subscribe("mqtt/+/humidity", qos=2)
    async def _second_subscription(
        client: MQTTClient, topic: str, payload: bytes, qos: int, properties: Any
    ):
        """
        Second subscription handler for humidity topic.

        Both handlers should be called when receiving mqtt/+/humidity messages.
        """
        processed_msgs[topic] += 1
        logging.info(
            "humidity: %s %s %s %s",
            topic,
            payload.decode(),
            qos,
            properties,
        )
        return 0

    @flask_mqtt.on_message()
    async def _process_message(
        client: MQTTClient, topic: str, payload: bytes, qos: int, properties: Any
    ):
        """Universal handler for all messages received."""
        received_msgs[topic] += 1
        logging.info(
            "Received message: %s %s %s %s",
            topic,
            payload.decode(),
            qos,
            properties,
        )
        return 0

    @flask_mqtt.on_disconnect()
    def _disconnect(client: MQTTClient, packet, exc=None):
        logging.info("Disconnected")

    @flask_mqtt.on_subscribe()
    def _subscribe(client: MQTTClient, mid: int, qos: int, properties: Any):
        logging.info("subscribed %s %s %s %s", client, mid, qos, properties)

    @app.get("/test-status")
    def _get_status():
        return {
            "received_msgs": received_msgs,
            "processed_msgs": processed_msgs,
            "num_subscriptions": len(flask_mqtt.subscriptions),
        }

    @app.post("/test-publish")
    def _pub_msg():
        flask_mqtt.publish("flask-mqtt", "Hello from Flask")
        flask_mqtt.publish("mqtt/test/temperature", "27ºC")
        flask_mqtt.publish("mqtt/test/humidity", "0%")
        return {"result": True, "message": "Published"}

    @app.post("/test-unsubscribe")
    def _unsub():
        flask_mqtt.unsubscribe("flask-mqtt")
        flask_mqtt.unsubscribe("$share/test/mqtt/+/temperature")
        return {"result": True, "message": "Unsubscribed"}

    @app.post("/test-reset")
    def _reset_msgs():
        flask_mqtt.publish("flask-mqtt")
        flask_mqtt.publish("mqtt/test/humidity")
        flask_mqtt.publish("mqtt/test/temperature")
        return {"result": True, "message": "Cleaned"}

    # connects the MQTT client and registers it in `app.extensions`
    flask_mqtt.init_app(app)
    logging.info("connection done, starting flask app now")
    try:
        yield app
    finally:
        atexit.unregister(flask_mqtt.mqtt_shutdown)
        flask_mqtt.mqtt_shutdown()


@pytest.fixture
def app_client(test_app):
    """Flask test client with example app."""
    with test_app.test_client() as tc:
        yield tc
