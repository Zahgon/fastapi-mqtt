from flask import current_app

from .mqtt_ws_client import DynamicMQTTClient

#: Key used to register the websocket subscribers manager on the Flask app.
WS_SUBSCRIBERS_KEY = "ws_subscribers"


def get_ws_subscribers() -> DynamicMQTTClient:
    """Access the websocket subscribers manager stored in the current Flask app."""
    return current_app.extensions[WS_SUBSCRIBERS_KEY]
