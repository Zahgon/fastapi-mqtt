from typing import Any

from flask import Flask
from gmqtt import Client as MQTTClient
from werkzeug.exceptions import HTTPException

from flask_mqtt import FlaskMQTT, MQTTConfig

from .json_provider import JSONProvider

mqtt_config = MQTTConfig()

flask_mqtt = FlaskMQTT(config=mqtt_config)

app = Flask(__name__)
app.json = JSONProvider(app)


@app.errorhandler(HTTPException)
def http_error(exc: HTTPException):
    """Unknown paths and wrong methods answer JSON, as they always did."""
    return {"detail": exc.name}, exc.code


@flask_mqtt.on_connect()
def connect(client: MQTTClient, flags: int, rc: int, properties: Any):
    client.subscribe("/mqtt")  # subscribing mqtt topic
    print("Connected: ", client, flags, rc, properties)


@flask_mqtt.subscribe("mqtt/+/temperature", "mqtt/+/humidity", qos=1)
async def home_message(client: MQTTClient, topic: str, payload: bytes, qos: int, properties: Any):
    print("temperature/humidity: ", topic, payload.decode(), qos, properties)


@flask_mqtt.on_message()
async def message(client: MQTTClient, topic: str, payload: bytes, qos: int, properties: Any):
    print("Received message: ", topic, payload.decode(), qos, properties)


@flask_mqtt.subscribe("my/mqtt/topic/#", qos=2)
async def message_to_topic_with_high_qos(
    client: MQTTClient, topic: str, payload: bytes, qos: int, properties: Any
):
    print(
        "Received message to specific topic and QoS=2: ", topic, payload.decode(), qos, properties
    )


@flask_mqtt.on_disconnect()
def disconnect(client: MQTTClient, packet, exc=None):
    print("Disconnected")


@flask_mqtt.on_subscribe()
def subscribe(client: MQTTClient, mid: int, qos: int, properties: Any):
    print("subscribed", client, mid, qos, properties)


@app.get("/test")
def func():
    flask_mqtt.publish("/mqtt", "Hello from Fastapi")  # publishing mqtt topic
    return {"result": True, "message": "Published"}


# Connect once every handler is registered, so the subscriptions above
# are sent to the broker as soon as the connection is established.
flask_mqtt.init_app(app)
