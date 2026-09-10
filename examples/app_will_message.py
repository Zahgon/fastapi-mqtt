from flask import Flask
from werkzeug.exceptions import HTTPException

from flask_mqtt.config import MQTTConfig
from flask_mqtt.flaskmqtt import FlaskMQTT

from .json_provider import JSONProvider

mqtt_config = MQTTConfig(
    will_message_topic="/WILL",
    will_message_payload="MQTT Connection is dead!",
    will_delay_interval=2,
)

flask_mqtt = FlaskMQTT(config=mqtt_config)

app = Flask(__name__)
app.json = JSONProvider(app)


@app.errorhandler(HTTPException)
def http_error(exc: HTTPException):
    """Unknown paths and wrong methods answer JSON, as they always did."""
    return {"detail": exc.name}, exc.code


@flask_mqtt.on_connect()
def connect(client, flags, rc, properties):
    flask_mqtt.client.subscribe("/WILL")  # /WILL will trigger after disconnect
    flask_mqtt.client.subscribe("/mqtt")
    print("Connected: ", client, flags, rc, properties)


@flask_mqtt.on_message()
async def message(client, topic, payload, qos, properties):
    print("Received message: ", topic, payload.decode(), qos, properties)

    return 0


@flask_mqtt.on_disconnect()
def disconnect(client, packet, exc=None):
    print("Disconnected")


@flask_mqtt.on_subscribe()
def subscribe(client, mid, qos, properties):
    print("subscribed", client, mid, qos, properties)


@app.get("/")
def func():
    # publishing mqtt topic
    flask_mqtt.publish("/mqtt", "Hello from Fastapi")
    return {"result": True, "message": "Published"}


# Connect once every handler is registered.
flask_mqtt.init_app(app)
