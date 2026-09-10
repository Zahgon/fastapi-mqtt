# Flask-MQTT

## Extension

MQTT is a lightweight publish/subscribe messaging protocol designed for M2M (machine to machine) telemetry in low bandwidth environments.
Flask-mqtt is the client for working with MQTT.

For more information about MQTT, please refer to here: [MQTT](mqtt.md)

Flask-mqtt wraps around [gmqtt](https://github.com/wialon/gmqtt) module. Gmqtt Python async client for MQTT client implementation.
The module has the support of MQTT version 5.0 protocol

## Badges

[![MIT licensed](https://img.shields.io/github/license/sabuhish/flask-mqtt)](https://raw.githubusercontent.com/sabuhish/flask-mqtt/master/LICENSE)
[![GitHub stars](https://img.shields.io/github/stars/sabuhish/flask-mqtt.svg)](https://github.com/sabuhish/flask-mqtt/stargazers)
[![GitHub forks](https://img.shields.io/github/forks/sabuhish/flask-mqtt.svg)](https://github.com/sabuhish/flask-mqtt/network)
[![GitHub issues](https://img.shields.io/github/issues-raw/sabuhish/flask-mqtt)](https://github.com/sabuhish/flask-mqtt/issues)
[![Downloads](https://pepy.tech/badge/flask-mqtt)](https://pepy.tech/project/flask-mqtt)

## Available Features

MQTT specification avaliable with help decarator methods using callbacks:

- `on_connect() `
- `on_disconnect() `
- `on_subscribe() `
- `on_message() `

MQTT Settings available with `pydantic` class:

- `Authentication` to broker with credentials
- `unsubscribe` certain topics and `publish` to certain topics
