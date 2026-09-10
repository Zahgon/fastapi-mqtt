"""
Tests for the synchronous client lifecycle.

Flask is a synchronous WSGI framework, so `FlaskMQTT` drives `gmqtt` from a
private asyncio loop running in a background thread. These tests cover that
threading model: the loop is started/stopped on demand and MQTT calls issued
from a worker thread -- or from inside an MQTT handler -- are marshalled safely.
"""

import threading
import time
from collections import defaultdict
from typing import Any

import pytest
from flask import Flask

from flask_mqtt.config import MQTTConfig
from flask_mqtt.flaskmqtt import FlaskMQTT

from .conftest import TEST_BROKER_HOST, TEST_BROKER_PWD, TEST_BROKER_USER


def _make_client(**overrides: Any) -> FlaskMQTT:
    config = MQTTConfig(
        host=TEST_BROKER_HOST,
        username=TEST_BROKER_USER,
        password=TEST_BROKER_PWD,
        **overrides,
    )
    return FlaskMQTT(config=config, operation_timeout=15.0)


def _mqtt_threads() -> list[threading.Thread]:
    return [t for t in threading.enumerate() if t.name == "flask-mqtt"]


def test_publish_before_startup_raises():
    """Before connecting, gmqtt has no connection: calls fail exactly as they always did."""
    flask_mqtt = _make_client()

    with pytest.raises(AttributeError, match="'NoneType' object has no attribute 'publish'"):
        flask_mqtt.publish("flask-mqtt", "no loop yet")

    with pytest.raises(AttributeError, match="'NoneType' object has no attribute 'unsubscribe'"):
        flask_mqtt.unsubscribe("flask-mqtt")


def test_startup_and_shutdown_are_idempotent():
    """`mqtt_startup` / `mqtt_shutdown` can be called repeatedly without side effects."""
    threads_before = len(_mqtt_threads())
    flask_mqtt = _make_client()

    flask_mqtt.mqtt_startup()
    loop = flask_mqtt._loop
    assert loop is not None and loop.is_running()
    assert len(_mqtt_threads()) == threads_before + 1

    # second call is a no-op and keeps the very same loop
    flask_mqtt.mqtt_startup()
    assert flask_mqtt._loop is loop
    assert len(_mqtt_threads()) == threads_before + 1

    flask_mqtt.mqtt_shutdown()
    assert flask_mqtt._loop is None
    # second call is a no-op
    flask_mqtt.mqtt_shutdown()
    assert flask_mqtt._loop is None

    deadline = time.monotonic() + 5
    while len(_mqtt_threads()) > threads_before and time.monotonic() < deadline:
        time.sleep(0.01)
    assert len(_mqtt_threads()) == threads_before


def test_failed_connection_releases_the_loop_thread():
    """A broker that refuses the connection must not leak the background loop."""
    threads_before = len(_mqtt_threads())
    # port 1 is not served by any broker
    flask_mqtt = _make_client(port=1, reconnect_retries=0, reconnect_delay=1)

    with pytest.raises(OSError):
        flask_mqtt.mqtt_startup()

    assert flask_mqtt._loop is None
    deadline = time.monotonic() + 5
    while len(_mqtt_threads()) > threads_before and time.monotonic() < deadline:
        time.sleep(0.01)
    assert len(_mqtt_threads()) == threads_before


def test_publish_from_within_a_message_handler():
    """
    Publishing from an MQTT handler runs on the loop thread itself.

    It must be executed inline instead of being scheduled and waited on,
    which would deadlock the loop.
    """
    flask_mqtt = _make_client()
    seen: dict[str, int] = defaultdict(int)

    app = Flask(__name__)

    @flask_mqtt.subscribe("lifecycle/echo/in")
    async def _echo(client, topic: str, payload: bytes, qos: int, properties: Any):
        seen[topic] += 1
        # re-publish from inside the MQTT event loop
        flask_mqtt.publish("lifecycle/echo/out", payload.decode())
        return 0

    @flask_mqtt.subscribe("lifecycle/echo/out")
    async def _echoed(client, topic: str, payload: bytes, qos: int, properties: Any):
        seen[topic] += 1
        return 0

    @app.post("/emit")
    def _emit():
        # published from the WSGI worker thread
        flask_mqtt.publish("lifecycle/echo/in", "ping")
        return {"result": True}

    flask_mqtt.init_app(app)
    try:
        assert app.extensions["mqtt"] is flask_mqtt
        with app.test_client() as client:
            assert client.post("/emit").status_code == 200

        deadline = time.monotonic() + 5
        while seen["lifecycle/echo/out"] < 1 and time.monotonic() < deadline:
            time.sleep(0.01)

        assert seen["lifecycle/echo/in"] == 1
        assert seen["lifecycle/echo/out"] == 1
    finally:
        import atexit

        atexit.unregister(flask_mqtt.mqtt_shutdown)
        flask_mqtt.mqtt_shutdown()


def test_qos_redelivery_task_runs_on_the_mqtt_loop():
    """
    gmqtt's QoS 1/2 re-delivery task must live on the loop that runs the connection.

    The client is built from synchronous code (no event loop in this thread), yet the
    task gmqtt schedules at construction time must run once the client is started.
    """
    built: list = []
    # a plain thread has no event loop at all, like a WSGI worker or Python 3.14's main thread
    worker = threading.Thread(target=lambda: built.append(_make_client()))
    worker.start()
    worker.join()
    flask_mqtt = built[0]

    flask_mqtt.mqtt_startup()
    try:
        resend_task = flask_mqtt.client._resend_task
        assert resend_task.get_loop() is flask_mqtt._loop
        assert flask_mqtt._loop.is_running()
        assert not resend_task.done()
    finally:
        flask_mqtt.mqtt_shutdown()


def test_init_app_makes_sigterm_run_the_shutdown(monkeypatch):
    """SIGTERM must leave through `atexit` (clean DISCONNECT), not kill the process."""
    import signal

    installed: dict = {}
    monkeypatch.setattr(signal, "getsignal", lambda signum: signal.SIG_DFL)
    monkeypatch.setattr(
        signal, "signal", lambda signum, handler: installed.update({signum: handler})
    )
    flask_mqtt = _make_client()

    flask_mqtt.init_app(Flask(__name__))
    try:
        with pytest.raises(SystemExit):
            installed[signal.SIGTERM](signal.SIGTERM, None)
    finally:
        import atexit

        atexit.unregister(flask_mqtt.mqtt_shutdown)
        flask_mqtt.mqtt_shutdown()


def test_init_app_keeps_an_existing_sigterm_handler(monkeypatch):
    """A server that already handles SIGTERM (e.g. gunicorn) keeps its handler."""
    import signal

    installed: dict = {}
    monkeypatch.setattr(signal, "getsignal", lambda signum: lambda *_: None)
    monkeypatch.setattr(
        signal, "signal", lambda signum, handler: installed.update({signum: handler})
    )
    flask_mqtt = _make_client()

    flask_mqtt.init_app(Flask(__name__))
    try:
        assert installed == {}
    finally:
        import atexit

        atexit.unregister(flask_mqtt.mqtt_shutdown)
        flask_mqtt.mqtt_shutdown()
