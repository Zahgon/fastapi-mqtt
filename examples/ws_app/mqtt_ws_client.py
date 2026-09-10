import logging
from contextlib import contextmanager
from queue import Empty, Queue
from threading import Event
from typing import cast, Generator, Iterator, Optional

from gmqtt import Subscription

from flask_mqtt.flaskmqtt import FlaskMQTT

logger = logging.getLogger(__name__)

#: How often a waiting subscriber checks whether it has been stopped.
_POLL_INTERVAL = 0.5


class DynamicMQTTClient:
    """
    Wrapper around FlaskMQTT manager to dynamically subscribe to MQTT topics

    Supporting multiple persistent connections (like websockets or SSE),
    so the MQTT client is subscribed to a certain topic only when at least
    one client is listening to MQTT messages on it.

    If multiple clients _subscribe_ to the same topic,
    the subscription is shared and all clients are notified on new messages
    that match the subscribed topic.

    The queues are `queue.Queue` instead of `asyncio.Queue`, because they are
    filled from the MQTT event loop thread and drained from the WSGI worker
    threads serving the websocket connections.

    Inpired by the `MultisubscriberQueue` in
    https://github.com/smithk86/asyncio-multisubscriber-queue
    """

    mqtt: FlaskMQTT
    topic_subscriptions: dict[str, set[Queue[str]]]
    _close_sentinel = cast(str, object())

    __slots__ = ("mqtt", "topic_subscriptions")

    def __init__(self, mqtt: FlaskMQTT) -> None:
        self.mqtt = mqtt
        self.topic_subscriptions = {}

    def subscribe(
        self,
        topic: str,
        qos: int = 0,
        no_local: bool = False,
        retain_as_published: bool = False,
        retain_handling_options: int = 0,
        stop: Optional[Event] = None,
    ) -> Iterator[str]:
        """
        Generator to subscribe to MQTT topic and receive messages.

        `stop` is an optional `threading.Event` used to interrupt a subscriber
        that is blocked waiting for the next message.
        """
        with self.queue(topic, qos, no_local, retain_as_published, retain_handling_options) as q:
            while stop is None or not stop.is_set():
                try:
                    _data: str = q.get(timeout=_POLL_INTERVAL)
                except Empty:
                    continue
                if _data is self._close_sentinel:
                    break
                yield _data

    @contextmanager
    def queue(
        self,
        topic: str,
        qos: int,
        no_local: bool,
        retain_as_published: bool,
        retain_handling_options: int,
    ) -> Generator[Queue[str], None, None]:
        """Context helper which manages the lifecycle of the Queue."""
        _queue: Queue[str] = Queue()
        if topic in self.topic_subscriptions:
            self.topic_subscriptions[topic].add(_queue)
        else:
            self.topic_subscriptions[topic] = {_queue}
            subscription = Subscription(
                topic,
                qos,
                no_local,
                retain_as_published,
                retain_handling_options,
            )
            logger.warning("Subscribing to %s -> %s", subscription.topic, subscription)
            self.mqtt.run_on_mqtt_loop(self.mqtt.client.subscribe, subscription)
        try:
            yield _queue
        finally:
            self.topic_subscriptions[topic].remove(_queue)
            if not self.topic_subscriptions[topic]:
                self.topic_subscriptions.pop(topic)
                logger.warning("UnSubscribing from %s", topic)
                self.mqtt.unsubscribe(topic)

    def send_mqtt_msg(self, mqtt: FlaskMQTT, topic: str, msg_payload: str) -> int:
        """Put data on all the Queues matching the topic."""
        num_sent = 0
        for topic_listen, queues in self.topic_subscriptions.items():
            if mqtt.match(topic, topic_listen):
                for _queue in queues:
                    _queue.put(f"Msg received in topic '{topic}':\n{msg_payload}")
                    num_sent += 1
        return num_sent

    def close(self) -> None:
        """Put the close sentinel on all the Queues to signal session end."""
        for queues in self.topic_subscriptions.values():
            for _queue in queues:
                _queue.put(self._close_sentinel)
