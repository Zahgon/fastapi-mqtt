import time

from flask.testing import FlaskClient


def test_example_app(app_client: FlaskClient):
    response = app_client.post("/test-publish")
    assert response.status_code == 200
    assert response.get_json() == {"result": True, "message": "Published"}

    # wait a bit for the published messages to be received and processed
    time.sleep(0.1)

    # check status of received and processed msgs
    response = app_client.get("/test-status")
    assert response.status_code == 200
    assert response.get_json() == {
        "received_msgs": {"flask-mqtt": 1, "mqtt/test/humidity": 1, "mqtt/test/temperature": 1},
        "processed_msgs": {"mqtt/test/temperature": 1, "mqtt/test/humidity": 2},
        "num_subscriptions": 2,
    }

    response = app_client.post("/test-unsubscribe")
    assert response.status_code == 200
    assert response.get_json() == {"result": True, "message": "Unsubscribed"}

    time.sleep(0.1)

    response = app_client.post("/test-publish")
    assert response.status_code == 200
    assert response.get_json() == {"result": True, "message": "Published"}

    time.sleep(0.1)

    response = app_client.get("/test-status")
    assert response.status_code == 200
    assert response.get_json() == {
        "received_msgs": {"flask-mqtt": 1, "mqtt/test/humidity": 2, "mqtt/test/temperature": 1},
        "processed_msgs": {"mqtt/test/temperature": 1, "mqtt/test/humidity": 4},
        "num_subscriptions": 1,
    }

    response = app_client.post("/test-reset")
    assert response.status_code == 200
    assert response.get_json() == {"result": True, "message": "Cleaned"}

    time.sleep(0.1)

    response = app_client.get("/test-status")
    assert response.status_code == 200
    assert response.get_json() == {
        "received_msgs": {"flask-mqtt": 1, "mqtt/test/humidity": 3, "mqtt/test/temperature": 1},
        "processed_msgs": {"mqtt/test/temperature": 1, "mqtt/test/humidity": 6},
        "num_subscriptions": 1,
    }
