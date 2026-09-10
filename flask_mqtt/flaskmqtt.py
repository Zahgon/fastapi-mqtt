import asyncio
import atexit
import logging
import signal
import threading
import uuid
from itertools import zip_longest
from threading import Event, Thread
from typing import Any, Callable, Dict, List, Optional, Tuple, TYPE_CHECKING

from gmqtt import Client as MQTTClient
from gmqtt import Message, Subscription
from gmqtt.mqtt.constants import MQTTv50

from .config import MQTTConfig
from .handlers import (
    MQTTConnectionHandler,
    MQTTDisconnectHandler,
    MQTTHandlers,
    MQTTMessageHandler,
    MQTTSubscriptionHandler,
)

if TYPE_CHECKING:  # pragma: no cover
    from flask import Flask

log_info = logging.getLogger("flask_mqtt")

#: Seconds a synchronous call may wait for the MQTT event loop to answer.
DEFAULT_OPERATION_TIMEOUT = 30.0


async def _call_on_loop(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Trivial coroutine used to run a blocking-free gmqtt call inside the MQTT loop."""
    return func(*args, **kwargs)


def _exit_on_sigterm() -> None:
    """
    Make SIGTERM exit through the interpreter's normal shutdown, so `atexit` runs.

    ASGI servers turned SIGTERM into their lifespan shutdown. WSGI servers such as the
    Werkzeug development server leave it at its default disposition, which kills the
    process outright: `mqtt_shutdown` never runs, and the broker publishes the last-will
    message on every normal stop. Only installed from the main thread, and only when
    nothing else (e.g. gunicorn) handles SIGTERM already.
    """
    if threading.current_thread() is not threading.main_thread():
        return
    if signal.getsignal(signal.SIGTERM) is not signal.SIG_DFL:
        return

    def _terminate(_signum: int, _frame: Any) -> None:
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, _terminate)


class FlaskMQTT:
    """
    FlaskMQTT client sets connection parameters before connecting and manipulating the MQTT service.
    The object holds session information necessary to connect the MQTT broker.

    Flask is a synchronous WSGI framework while `gmqtt` is asyncio based, so this
    extension owns a private asyncio event loop running in a background daemon
    thread. Every MQTT operation issued from a Flask view (which runs in a WSGI
    worker thread) is marshalled onto that loop in a thread-safe way.

    config: MQTTConfig config object

    client_id: Should be a unique identifier for connection to the MQTT broker.

    clean_session: Enables broker to establish a persistent session.
                            In a persistent session clean_session = False.
                            The broker stores all subscriptions for the client.
                            If the session is not persistent (clean_session = True).
                            The broker does not store anything for the client and \
                            purges all information from any previous persistent session.
                            The client_id  identifies the session.

    optimistic_acknowledgement:  #TODO more info needed

    mqtt_logger: Optional logging.Logger to use. When it is not given and the client
        is bound to an application with `init_app`, the Flask `app.logger` is used.

    operation_timeout: Seconds a synchronous call blocks waiting for the MQTT loop.
    """

    def __init__(
        self,
        config: MQTTConfig,
        *,
        client_id: Optional[str] = None,
        clean_session: bool = True,
        optimistic_acknowledgement: bool = True,
        mqtt_logger: Optional[logging.Logger] = None,
        operation_timeout: float = DEFAULT_OPERATION_TIMEOUT,
        **kwargs: Any,
    ) -> None:
        if not client_id:
            client_id = uuid.uuid4().hex

        self._next_loop: Optional[asyncio.AbstractEventLoop] = None
        self.client: MQTTClient = self._build_client(client_id, **kwargs)
        self.config: MQTTConfig = config

        self.client._clean_session = clean_session
        self.client._username = config.username
        self.client._password = config.password
        self.client._host = config.host
        self.client._port = config.port
        self.client._keepalive = config.keepalive
        self.client._ssl = config.ssl
        self.client.optimistic_acknowledgement = optimistic_acknowledgement
        self.client._connect_properties = kwargs
        self.client.on_message = self.__on_message
        self.client.on_connect = self.__on_connect
        self.subscriptions: Dict[str, Tuple[Subscription, List[MQTTMessageHandler]]] = {}
        self._mqtt_logger = mqtt_logger
        self._logger = mqtt_logger or log_info
        self._operation_timeout = operation_timeout
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._loop_thread: Optional[Thread] = None
        self.mqtt_handlers = MQTTHandlers(self.client, self._logger)

        if (
            self.config.will_message_topic
            and self.config.will_message_payload
            and self.config.will_delay_interval
        ):
            self.client._will_message = Message(
                self.config.will_message_topic,
                self.config.will_message_payload,
                will_delay_interval=self.config.will_delay_interval,
            )
            self._logger.debug(
                "WILL MESSAGE INITIALIZED: "
                "topic -> %s\n payload -> %s\n will_delay_interval -> %s",
                self.config.will_message_topic,
                self.config.will_message_payload,
                self.config.will_delay_interval,
            )

    @staticmethod
    def match(topic: str, template: str) -> bool:
        """
        Defined match topics

        topic: topic name
        template: template topic name that contains wildcards
        """
        if str(template).startswith("$share/"):
            template = template.split("/", 2)[2]

        topic_parts = topic.split("/")
        template_parts = template.split("/")

        for topic_part, part in zip_longest(topic_parts, template_parts):
            if part == "#" and not str(topic_part).startswith("$"):
                return True
            elif (topic_part is None or part not in {"+", topic_part}) or (
                part == "+" and topic_part.startswith("$")
            ):
                return False
            continue

        return len(template_parts) == len(topic_parts)

    async def connection(self) -> None:
        if self.client._username:
            self.client.set_auth_credentials(self.client._username, self.client._password)
            self._logger.debug("user is authenticated")

        await self.__set_connetion_config()

        version = self.config.version or MQTTv50
        self._logger.info("Used broker version is %s", version)

        await self.client.connect(
            self.client._host,
            self.client._port,
            self.client._ssl,
            self.client._keepalive,
            version,
        )
        self._logger.debug("Connected to broker")

    async def __set_connetion_config(self) -> None:
        """
        The connected MQTT clients will always try to reconnect in case of lost connections.
        The number of reconnect attempts is unlimited.
        For changing this behavior, set reconnect_retries and reconnect_delay with its values.
        For more info: https://github.com/wialon/gmqtt#reconnects
        """
        self.client.set_config(
            {
                "reconnect_retries": self.config.reconnect_retries,
                "reconnect_delay": self.config.reconnect_delay,
            }
        )

    def __on_connect(self, client: MQTTClient, flags: int, rc: int, properties: Any) -> None:
        """
        Generic on connecting handler, it would call user handler if defined.
        Will perform subscription for given topics.
        It cannot be done earlier, since subscription relies on connection.
        """
        if self.mqtt_handlers.user_connect_handler is not None:
            self.mqtt_handlers.user_connect_handler(client, flags, rc, properties)

        for topic in self.subscriptions:
            self._logger.debug("Subscribing for %s", topic)
            self.client.subscribe(self.subscriptions[topic][0])

    async def __on_message(
        self, client: MQTTClient, topic: str, payload: bytes, qos: int, properties: Any
    ) -> Any:
        """
        Generic on message handler, it will call user handler if defined.
        This will invoke per topic handlers that are subscribed for
        """
        gather = []
        if self.mqtt_handlers.user_message_handler is not None:
            self._logger.debug("Calling user_message_handler")
            gather.append(
                self.mqtt_handlers.user_message_handler(client, topic, payload, qos, properties)
            )

        for topic_template in self.subscriptions:
            if self.match(topic, topic_template):
                self._logger.debug("Calling specific handler for topic %s", topic)
                for handler in self.subscriptions[topic_template][1]:
                    gather.append(handler(client, topic, payload, qos, properties))

        return await asyncio.gather(*gather)

    def _build_client(self, client_id: str, **kwargs: Any) -> MQTTClient:
        """
        Build the gmqtt client on the event loop `mqtt_startup()` is going to run.

        gmqtt schedules its QoS 1/2 re-delivery task on the current event loop as soon
        as the client is built. Built from plain synchronous code there is no such loop:
        the task would be bound to a loop that never runs, so unacknowledged messages
        would never be re-delivered (and Python 3.14+ refuses to build the client at all).
        """
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()

            async def _build() -> MQTTClient:
                return MQTTClient(client_id, **kwargs)

            client = loop.run_until_complete(_build())
            self._next_loop = loop
            return client
        # Built from inside a running loop: gmqtt binds its task to that loop.
        return MQTTClient(client_id, **kwargs)

    def _start_loop(self) -> asyncio.AbstractEventLoop:
        """Spawn the daemon thread that runs the private asyncio loop used by gmqtt."""
        # The first start runs the loop the client was built on (see `_build_client`).
        loop, self._next_loop = self._next_loop, None
        if loop is None or loop.is_closed():
            loop = asyncio.new_event_loop()
        running = Event()

        def _run_loop() -> None:
            asyncio.set_event_loop(loop)
            loop.call_soon(running.set)
            loop.run_forever()

        thread = Thread(target=_run_loop, name="flask-mqtt", daemon=True)
        thread.start()
        if not running.wait(self._operation_timeout):  # pragma: no cover
            raise RuntimeError("MQTT event loop did not start")

        self._loop = loop
        self._loop_thread = thread
        self._logger.debug("MQTT event loop started")
        return loop

    def _stop_loop(self) -> None:
        """Cancel anything left pending, stop the loop and join its thread."""
        loop, thread = self._loop, self._loop_thread
        self._loop, self._loop_thread = None, None
        if loop is None or thread is None:  # pragma: no cover
            return

        def _cancel_pending() -> None:
            for task in asyncio.all_tasks(loop):
                task.cancel()
            loop.stop()

        loop.call_soon_threadsafe(_cancel_pending)
        thread.join(self._operation_timeout)
        loop.close()
        self._logger.debug("MQTT event loop stopped")

    def run_on_mqtt_loop(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """
        Run a synchronous gmqtt call inside the MQTT event loop and return its result.

        gmqtt writes to an asyncio transport and schedules tasks, so its methods
        must not be called from a WSGI worker thread directly. Use this helper to
        reach `self.client` from a Flask view, e.g.::

            mqtt.run_on_mqtt_loop(mqtt.client.subscribe, Subscription("some/topic"))

        When it is called from the MQTT loop itself, e.g. inside a message handler,
        `func` is invoked inline instead, since waiting on the loop from the loop
        would deadlock.

        Before `mqtt_startup()` (or after `mqtt_shutdown()`) no loop runs, so `func`
        is called directly, as it always was: e.g. publishing before connecting
        fails inside gmqtt itself, which has no connection yet.
        """
        loop = self._loop
        if loop is None or not loop.is_running():
            return func(*args, **kwargs)
        try:
            current_loop: Optional[asyncio.AbstractEventLoop] = asyncio.get_running_loop()
        except RuntimeError:
            current_loop = None
        if current_loop is loop:
            # Already inside the MQTT loop, e.g. called from a message handler.
            return func(*args, **kwargs)

        future = asyncio.run_coroutine_threadsafe(_call_on_loop(func, *args, **kwargs), loop)
        return future.result(self._operation_timeout)

    def publish(
        self,
        message_or_topic: str,
        payload: Any = None,
        qos: int = 0,
        retain: bool = False,
        **kwargs,
    ) -> None:
        """
        Defined to publish payload MQTT server

        message_or_topic: topic name

        payload: message payload

        qos: Quality of Assurance

        retain:
        """
        return self.run_on_mqtt_loop(
            self.client.publish, message_or_topic, payload=payload, qos=qos, retain=retain, **kwargs
        )

    def unsubscribe(self, topic: str, **kwargs):
        """
        Defined to unsubscribe topic

        topic: topic name
        """
        self._logger.debug("unsubscribe")
        if topic in self.subscriptions:
            del self.subscriptions[topic]

        return self.run_on_mqtt_loop(self.client.unsubscribe, topic, **kwargs)

    def mqtt_startup(self) -> None:
        """Start the MQTT event loop and connect the client."""
        if self._loop is not None and self._loop.is_running():
            self._logger.debug("MQTT client is already started")
            return

        loop = self._start_loop()
        try:
            asyncio.run_coroutine_threadsafe(self.connection(), loop).result(
                self._operation_timeout
            )
        except BaseException:
            self._stop_loop()
            raise

    def mqtt_shutdown(self) -> None:
        """Disconnect the MQTT client and stop its event loop."""
        loop = self._loop
        if loop is None or not loop.is_running():
            self._logger.debug("MQTT client is already stopped")
            return

        try:
            asyncio.run_coroutine_threadsafe(self.client.disconnect(), loop).result(
                self._operation_timeout
            )
        finally:
            self._stop_loop()

    def init_app(self, app: "Flask") -> None:
        """
        Bind the MQTT client to a Flask application.

        The client is registered in `app.extensions["mqtt"]`, connected right away
        and disconnected when the interpreter exits. It has to be called once every
        `subscribe` / `on_message` handler is registered, since the subscriptions
        are sent to the broker as soon as the connection is established.
        """
        app.extensions["mqtt"] = self
        if self._mqtt_logger is None:
            self._logger = app.logger
            self.mqtt_handlers._logger = app.logger

        self.mqtt_startup()
        atexit.register(self.mqtt_shutdown)
        _exit_on_sigterm()

    def subscribe(
        self,
        *topics,
        qos: int = 0,
        no_local: bool = False,
        retain_as_published: bool = False,
        retain_handling_options: int = 0,
        subscription_identifier: Any = None,
    ) -> Callable[..., Any]:
        """
        Decorator method used to subscribe for specific topics.
        """

        def subscribe_handler(handler: MQTTMessageHandler) -> MQTTMessageHandler:
            self._logger.debug("Subscribe for topics: %s", topics)
            for topic in topics:
                if topic not in self.subscriptions:
                    subscription = Subscription(
                        topic,
                        qos,
                        no_local,
                        retain_as_published,
                        retain_handling_options,
                        subscription_identifier,
                    )
                    self.subscriptions[topic] = (subscription, [handler])
                else:
                    # Use the most restrictive field of the same subscription
                    old_subscription = self.subscriptions[topic][0]
                    new_subscription = Subscription(
                        topic,
                        max(qos, old_subscription.qos),
                        no_local or old_subscription.no_local,
                        retain_as_published or old_subscription.retain_as_published,
                        max(
                            retain_handling_options,
                            old_subscription.retain_handling_options,
                        ),
                        old_subscription.subscription_identifier or subscription_identifier,
                    )
                    self.subscriptions[topic] = (
                        new_subscription,
                        self.subscriptions[topic][1],
                    )
                    self.subscriptions[topic][1].append(handler)
            return handler

        return subscribe_handler

    def on_connect(self) -> Callable[..., Any]:
        """
        Decorator method used to handle the connection to MQTT.
        """

        def connect_handler(handler: MQTTConnectionHandler) -> MQTTConnectionHandler:
            self._logger.debug("handler accepted")
            return self.mqtt_handlers.on_connect(handler)

        return connect_handler

    def on_message(self) -> Callable[..., Any]:
        """
        The decorator method is used to subscribe to messages from all topics.
        """

        def message_handler(handler: MQTTMessageHandler) -> MQTTMessageHandler:
            self._logger.debug("on_message handler accepted")
            return self.mqtt_handlers.on_message(handler)

        return message_handler

    def on_disconnect(self) -> Callable[..., Any]:
        """
        The Decorator method used wrap disconnect callback.
        """

        def disconnect_handler(handler: MQTTDisconnectHandler) -> MQTTDisconnectHandler:
            self._logger.debug("on_disconnect handler accepted")
            return self.mqtt_handlers.on_disconnect(handler)

        return disconnect_handler

    def on_subscribe(self) -> Callable[..., Any]:
        """
        Decorator method is used to obtain subscribed topics and properties.
        """

        def subscribe_handler(handler: MQTTSubscriptionHandler) -> MQTTSubscriptionHandler:
            self._logger.debug("on_subscribe handler accepted")
            return self.mqtt_handlers.on_subscribe(handler)

        return subscribe_handler
