"""Pytest configuration and shared fixtures."""

import sys
import json
import pytest
from unittest.mock import Mock, MagicMock

# Mock external dependencies that aren't installed in test environment
paho_mock = MagicMock()
paho_mqtt_mock = MagicMock()
paho_mqtt_enums_mock = MagicMock()
paho_mqtt_client_mock = MagicMock()

# Set up the module hierarchy
paho_mqtt_mock.enums = paho_mqtt_enums_mock
paho_mqtt_mock.client = paho_mqtt_client_mock
paho_mock.mqtt = paho_mqtt_mock

sys.modules["IoTKafka"] = MagicMock()
sys.modules["paho"] = paho_mock
sys.modules["paho.mqtt"] = paho_mqtt_mock
sys.modules["paho.mqtt.enums"] = paho_mqtt_enums_mock
sys.modules["paho.mqtt.client"] = paho_mqtt_client_mock
sys.modules["confluent_kafka"] = MagicMock()


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
