# flask-mqtt

MQTT is a lightweight publish/subscribe messaging protocol designed for M2M (machine to machine) telemetry in low bandwidth environments.
Flask-mqtt is the client for working with MQTT.

For more information about MQTT, please refer to here: [MQTT](MQTT.md)

Flask-mqtt wraps around [gmqtt](https://github.com/wialon/gmqtt) module. Gmqtt Python async client for MQTT client implementation.
Module has support of MQTT version 5.0 protocol

[![MIT licensed](https://img.shields.io/github/license/sabuhish/flask-mqtt)](https://raw.githubusercontent.com/sabuhish/flask-mqtt/master/LICENSE)
[![GitHub stars](https://img.shields.io/github/stars/sabuhish/flask-mqtt.svg)](https://github.com/sabuhish/flask-mqtt/stargazers)
[![GitHub forks](https://img.shields.io/github/forks/sabuhish/flask-mqtt.svg)](https://github.com/sabuhish/flask-mqtt/network)
[![GitHub issues](https://img.shields.io/github/issues-raw/sabuhish/flask-mqtt)](https://github.com/sabuhish/flask-mqtt/issues)
[![Downloads](https://pepy.tech/badge/flask-mqtt)](https://pepy.tech/project/flask-mqtt)

---

## **Documentation**: [Flask-MQTT](https://sabuhish.github.io/flask-mqtt/)

The key feature are:

MQTT specification avaliable with help decarator methods using callbacks:

- on_connect()
- on_disconnect()
- on_subscribe()
- on_message()
- subscribe(topic)

- MQTT Settings available with `pydantic` class
- Authentication to broker with credentials
- unsubscribe certain topics and publish to certain topics

### 🔨 Installation

```sh
pip install flask-mqtt
```

### 🕹 Guide

```python
from typing import Any

from flask import Flask
from gmqtt import Client as MQTTClient

from flask_mqtt import FlaskMQTT, MQTTConfig

mqtt_config = MQTTConfig()
flask_mqtt = FlaskMQTT(config=mqtt_config)

app = Flask(__name__)


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
```

Publish method:

```python
def func():
    flask_mqtt.publish("/mqtt", "Hello from Fastapi")  # publishing mqtt topic
    return {"result": True, "message": "Published"}
```

Subscribe method:

```python
@flask_mqtt.on_connect()
def connect(client, flags, rc, properties):
    client.subscribe("/mqtt")  # subscribing mqtt topic
    print("Connected: ", client, flags, rc, properties)
```

Changing connection params

```python
mqtt_config = MQTTConfig(
    host="mqtt.mosquito.org",
    port=1883,
    keepalive=60,
    username="username",
    password="strong_password",
)
flask_mqtt = FlaskMQTT(config=mqtt_config)
```

### ✅ Testing

- Clone the repository and install it with [`poetry`](https://python-poetry.org).
- Run tests with `pytest`, using an external MQTT broker to connect (defaults to 'test.mosquitto.org').
- Explore the Flask app **examples** and run them with the `flask` CLI

```sh
# (opc) Run a local mosquitto MQTT broker with docker
docker run -d --name mosquitto -p 9001:9001 -p 1883:1883 eclipse-mosquitto:1.6.15
# Set host for test broker when running pytest
TEST_BROKER_HOST=localhost pytest
# Run the example apps against local broker, with the flask dev server
TEST_BROKER_HOST=localhost flask --app examples.app run --port 8000
TEST_BROKER_HOST=localhost flask --app examples.ws_app.app run --port 8000
```

# Contributing

Fell free to open issue and send pull request.

Thanks To [Contributors](https://github.com/sabuhish/flask-mqtt/graphs/contributors).
Contributions of any kind are welcome!

Before you start please read [CONTRIBUTING](https://github.com/sabuhish/flask-mqtt/blob/master/CONTRIBUTING.md)
