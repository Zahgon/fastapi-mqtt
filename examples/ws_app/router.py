from threading import Event, Thread

from flask import Blueprint, current_app, Response
from flask_sock import Sock
from simple_websocket import ConnectionClosed

from .dependencies import get_ws_subscribers

mqtt_router = Blueprint("mqtt", __name__)
sock = Sock()

_HTML_WS_MQTT_CLIENT = """<!DOCTYPE html>
<html>
    <head>
        <title>MQTT WebSocket Client</title>
    </head>
    <body>
        <h1>WebSocket Dynamic MQTT client</h1>
        <form action="" onsubmit="sendMessage(event)">
            <input type="text" id="topicText" autocomplete="off"/>
            <button>Subscribe to topic</button>
        </form>
        <ul id='messages'>
        </ul>
        <script>
            var ws = new WebSocket(`ws://localhost:8000/ws-mqtt-client`);
            ws.onmessage = function(event) {
                var messages = document.getElementById('messages');
                var message = document.createElement('li');
                var content = document.createTextNode(event.data);
                message.appendChild(content);
                messages.appendChild(message);
            };
            function sendMessage(event) {
                var input = document.getElementById("topicText");
                ws.send(input.value);
                input.value = '';
                event.preventDefault();
            }
        </script>
    </body>
</html>
"""


@mqtt_router.get("/home")
def _ws_demo_page():
    """Show basic web with websocket connection to subscribe to MQTT topics."""
    return Response(_HTML_WS_MQTT_CLIENT, mimetype="text/html")


@mqtt_router.get("/ws-subscriptions")
def _get_current_clients_subscriptions():
    """Return JSON with current state of WS clients."""
    ws_subscribers = get_ws_subscribers()
    return {
        "topic_subscriptions": list(ws_subscribers.topic_subscriptions.keys()),
        "clients_by_topic": {
            key: len(queues) for key, queues in ws_subscribers.topic_subscriptions.items()
        },
    }


@sock.route("/ws-mqtt-client", bp=mqtt_router)
def websocket_endpoint(ws):
    ws_subscribers = get_ws_subscribers()
    logger = current_app.logger
    logger.info("WS connected")

    def _send_received_msgs(topic: str, stop: Event) -> None:
        try:
            for msg in ws_subscribers.subscribe(topic, stop=stop):
                ws.send(msg)
        except ConnectionClosed:
            # the socket is gone: the subscription ends with this undeliverable message
            return

    try:
        data_topic = ws.receive()
        logger.warning("WS MQTT subscription to '%s'", data_topic)

        ws.send(f"You subscribed to: {data_topic}")
        while True:
            # in a separate thread, push the received MQTT messages to the client
            stop_event = Event()
            Thread(
                target=_send_received_msgs,
                args=(data_topic, stop_event),
                daemon=True,
            ).start()
            # while waiting on the WS socket for a new MQTT subscription
            data_topic = ws.receive()
            stop_event.set()
            logger.warning("WS MQTT subscription change to '%s'", data_topic)
            ws.send(f"You subscribed to: {data_topic}")
    except ConnectionClosed:
        # As before, the subscriber of a closed socket is not stopped here: its MQTT
        # subscription stays registered until the next message for it finds the socket
        # closed.
        logger.info("Closed tab")
