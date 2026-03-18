"""Pytest configuration and shared fixtures."""

import json
import pytest
from unittest.mock import Mock


@pytest.fixture
def mqtt_message_dict():
    """Sample MQTT message payload as dict."""
    return {"temp": 25.5, "humidity": 60, "location": "room1"}


@pytest.fixture
def mqtt_message_bytes(mqtt_message_dict):
    """Sample MQTT message payload as bytes."""
    return json.dumps(mqtt_message_dict).encode("utf-8")


@pytest.fixture
def valid_mqtt_topic():
    """Valid MQTT topic."""
    return "telemetry/device123/data"


@pytest.fixture
def valid_device_serial():
    """Valid device serial number."""
    return "SN-123456"


@pytest.fixture
def mock_kafka_producer():
    """Mock Kafka producer."""
    producer = Mock()
    producer.produce = Mock()
    producer.flush = Mock(return_value=0)
    producer.close = Mock()
    return producer


@pytest.fixture
def mock_mqtt_client():
    """Mock MQTT client."""
    client = Mock()
    client.connect = Mock()
    client.subscribe = Mock()
    client.loop_forever = Mock()
    client.loop_stop = Mock()
    client.disconnect = Mock()
    client.is_connected = Mock(return_value=True)
    return client
