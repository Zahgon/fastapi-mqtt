### 🕹 Guide

After installing the module you setup your `Flask` app:

The main classes are `FlaskMQTT` and `MQTTConfig`

```python
from flask import Flask
from flask_mqtt import FlaskMQTT, MQTTConfig

app = Flask(__name__)

mqtt_config = MQTTConfig()

mqtt = FlaskMQTT(
    config=mqtt_config
)

# bind the client to the app, once every handler is registered
mqtt.init_app(app)

```

### `MQTTConfig` class

the class has the following attributes

- host: To connect MQTT broker, defaults to localhost
- port: To connect MQTT broker, defaults to 1883

- ssl: an SSL/TLS transport is created (by default a plain TCP transport is created)
  In case of an ssl.SSLContext object, context is used to create the transport.
  Alternately passing bool value, will set a default context,
  which is calls ssl.create_default_context().

- keepalive: Maximum period in seconds between communications with the broker.
  This controls the rate at which the client will send ping messages.
  Defaults to 60 seconds.

- username: username for authentication, defaults to None
- password: password for authentication, defaults to None

- version: MQTT broker version to use, defaults to MQTTv50.
  According to gmqtt.Client if the broker does not support 5.0 protocol version,
  responds with proper CONNACK reason code,
  the client will downgrade to 3.1 and reconnect automatically.

- reconnect_retries && reconnect_delay: Connected MQTT client always tries to reconnect,
  in case of lost connections. The number of reconnect attempts is unlimited.
  For changing this behavior give reconnect_retries and reconnect_delay values.
  For more info: # https://github.com/wialon/gmqtt#reconnects

The last three parameters are used after the client disconnects abnormally

- will_message_topic: Topic of the payload
- will_message_payload: The payload
- will_delay_interval: Delay interval

### `FlaskMQTT` client

сlient sets connection parameters before connecting and manipulating the MQTT service.
The object holds session information necessary to connect the MQTT broker.

### `FlaskMQTT` params

client has the following parameters. The class object holds session information necessary to connect the MQTT broker.

- config: MQTTConfig config object

- client_id: Should be a unique identifier for connection to the MQTT broker.

- clean_session: Enables broker to establish a persistent session.
  In a persistent session clean_session = False.
  The broker stores all subscriptions for the client.
  If the session is not persistent (clean_session = True).
  The broker does not store anything for the client and \
   purges all information from any previous persistent session.
  The client_id identifies the session.

- optimistic_acknowledgement

- mqtt_logger: Optional `logging.Logger` to use. When it is not given and the client
  is bound with `init_app`, the Flask `app.logger` is used.

- operation_timeout: Seconds a synchronous call blocks waiting for the MQTT event loop.

### Connecting the client

Flask is a synchronous WSGI framework, while `gmqtt` is asyncio based, so `FlaskMQTT`
runs its own asyncio event loop in a background daemon thread. Calls made from a view,
like `publish` or `unsubscribe`, are marshalled onto that loop.

`init_app(app)` registers the client in `app.extensions["mqtt"]`, connects it and
disconnects it when the interpreter exits. It has to be called **after** all the
`subscribe` / `on_message` handlers are registered, since the subscriptions are sent
to the broker as soon as the connection is established.

`mqtt_startup()` and `mqtt_shutdown()` are also available to drive the connection
lifecycle manually.
