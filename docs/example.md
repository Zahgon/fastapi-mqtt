### Full example

```python
from flask import Flask
from flask_mqtt.config import MQTTConfig
from flask_mqtt.flaskmqtt import FlaskMQTT

flask_mqtt = FlaskMQTT(config=MQTTConfig())

app = Flask(__name__)

@flask_mqtt.on_connect()
def connect(client, flags, rc, properties):
    flask_mqtt.client.subscribe("/mqtt") #subscribing mqtt topic
    print("Connected: ", client, flags, rc, properties)

@flask_mqtt.on_message()
async def message(client, topic, payload, qos, properties):
    print("Received message: ",topic, payload.decode(), qos, properties)
    return 0

@flask_mqtt.subscribe("my/mqtt/topic/#")
async def message_to_topic(client, topic, payload, qos, properties):
    print("Received message to specific topic: ", topic, payload.decode(), qos, properties)

@flask_mqtt.on_disconnect()
def disconnect(client, packet, exc=None):
    print("Disconnected")

@flask_mqtt.on_subscribe()
def subscribe(client, mid, qos, properties):
    print("subscribed", client, mid, qos, properties)


@app.get("/")
def func():
    flask_mqtt.publish("/mqtt", "Hello from Fastapi") #publishing mqtt topic

    return {"result": True,"message":"Published" }


# connects the MQTT client once every handler is registered
flask_mqtt.init_app(app)
```

### More complex examples

Visit the [examples](https://github.com/sabuhish/flask-mqtt/tree/master/examples) folder for more code examples,
including a full Flask app organized in multiple files (splitting dependencies, routes, app creation) implementing a **dynamic MQTT client** through a WebSocket connection served by `flask-sock`.
