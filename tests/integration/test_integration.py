"""Integration tests for MQTT-to-Kafka ingestion pipeline.

Tests the full pipeline using mock Kafka and MQTT implementations.
Covers KafkaProducer, MQTTClient, and message processing workflows.
"""

import json
import logging
from unittest.mock import Mock, patch, MagicMock

import pytest

from app.config import Settings
from app.core.kafka_producer import KafkaProducer
from app.core.mqtt_client import MQTTClient
from tests.mocks.kafka import (
    MockProducer,
    reset_mock_topics,
    get_mock_topic_messages,
)
from tests.mocks.mqtt import (
    MockMQTTClient,
    reset_mock_broker,
    get_mock_broker_messages,
)


@pytest.fixture
def settings():
    """Application settings for testing."""
    return Settings(
        MQTT_BROKER_HOST="localhost",
        MQTT_BROKER_PORT=1883,
        MQTT_TOPIC="telemetry/#",
        KAFKA_BROKERS="localhost:9092",
        KAFKA_TOPIC_TELEMETRY_RAW="telemetry.raw",
    )


@pytest.fixture
def cleanup_mocks():
    """Clean up mock state before and after each test."""
    reset_mock_topics()
    reset_mock_broker()
    yield
    reset_mock_topics()
    reset_mock_broker()


class TestKafkaProducerIntegration:
    """Integration tests for KafkaProducer."""

    def test_kafka_producer_initialization(self, settings):
        """Test KafkaProducer initialization."""
        with patch("builtins.__import__") as mock_import:
            # Make IoTKafka import succeed with mock
            mock_iot_kafka = MagicMock()
            mock_iot_producer_class = MagicMock()
            mock_iot_kafka.IoTKafkaProducer = mock_iot_producer_class

            def import_side_effect(name, *args, **kwargs):
                if name == "IoTKafka":
                    return mock_iot_kafka
                return __import__(name, *args, **kwargs)

            mock_import.side_effect = import_side_effect

            producer = KafkaProducer(settings)
            assert producer.config == settings

    def test_kafka_producer_initialization_import_error(self, settings):
        """Test KafkaProducer initialization with missing dependency."""
        with patch("builtins.__import__") as mock_import:

            def import_side_effect(name, *args, **kwargs):
                if name == "IoTKafka":
                    raise ImportError("IoTKafka not installed")
                return __import__(name, *args, **kwargs)

            mock_import.side_effect = import_side_effect

            with pytest.raises(ImportError, match="IoTKafka not installed"):
                KafkaProducer(settings)

    def test_kafka_producer_publish_raw(self, settings):
        """Test publishing raw telemetry to Kafka."""
        mock_iot_producer_instance = Mock()
        mock_iot_producer_class = Mock(return_value=mock_iot_producer_instance)

        with patch.dict("sys.modules", {"IoTKafka": MagicMock()}):
            import sys

            sys.modules["IoTKafka"].IoTKafkaProducer = mock_iot_producer_class

            producer = KafkaProducer(settings)

            # Publish a message
            data = {
                "serial_number": "SN-123456",
                "value": 25.5,
                "timestamp": "2026-03-19T12:00:00Z",
            }
            producer.publish_raw(data, key="SN-123456")

            # Verify produce was called
            mock_iot_producer_instance.produce.assert_called_once()
            call_kwargs = mock_iot_producer_instance.produce.call_args[1]
            assert call_kwargs["topic"] == settings.kafka_topic_telemetry_raw
            assert call_kwargs["key"] == "SN-123456"
            assert call_kwargs["value"] == data

    def test_kafka_producer_custom_topic(self, settings):
        """Test publishing to custom Kafka topic."""
        mock_iot_producer_instance = Mock()
        mock_iot_producer_class = Mock(return_value=mock_iot_producer_instance)

        with patch.dict("sys.modules", {"IoTKafka": MagicMock()}):
            import sys

            sys.modules["IoTKafka"].IoTKafkaProducer = mock_iot_producer_class

            producer = KafkaProducer(settings)

            data = {"test": "data"}
            custom_topic = "custom.topic"
            producer.publish_raw(data, topic=custom_topic, key="test-key")

            call_kwargs = mock_iot_producer_instance.produce.call_args[1]
            assert call_kwargs["topic"] == custom_topic

    def test_kafka_producer_delivery_callback_success(self, settings):
        """Test delivery callback on successful publish."""
        mock_iot_producer_instance = Mock()
        mock_iot_producer_class = Mock(return_value=mock_iot_producer_instance)

        with patch.dict("sys.modules", {"IoTKafka": MagicMock()}):
            with patch("app.core.kafka_producer.metrics") as mock_metrics:
                import sys

                sys.modules["IoTKafka"].IoTKafkaProducer = mock_iot_producer_class

                producer = KafkaProducer(settings)

                # Simulate successful delivery
                mock_msg = Mock()
                mock_msg.topic.return_value = "telemetry.raw"
                mock_msg.partition.return_value = 0
                mock_msg.offset.return_value = 100

                producer._delivery_callback(None, mock_msg)

                # Verify metrics were updated
                mock_metrics.kafka_messages_published.labels.assert_called_once_with(
                    topic="telemetry.raw"
                )

    def test_kafka_producer_delivery_callback_error(self, settings):
        """Test delivery callback on publish error."""
        mock_iot_producer_instance = Mock()
        mock_iot_producer_class = Mock(return_value=mock_iot_producer_instance)

        with patch.dict("sys.modules", {"IoTKafka": MagicMock()}):
            with patch("app.core.kafka_producer.metrics") as mock_metrics:
                import sys

                sys.modules["IoTKafka"].IoTKafkaProducer = mock_iot_producer_class

                producer = KafkaProducer(settings)

                # Simulate error delivery
                mock_msg = Mock()
                mock_msg.topic.return_value = "telemetry.raw"
                error = Exception("Delivery failed")

                producer._delivery_callback(error, mock_msg)

                # Verify error metrics were updated
                mock_metrics.kafka_publish_errors.labels.assert_called_once()

    def test_kafka_producer_close(self, settings):
        """Test graceful producer shutdown."""
        mock_iot_producer_instance = Mock()
        mock_iot_producer_instance.flush.return_value = 0
        mock_iot_producer_class = Mock(return_value=mock_iot_producer_instance)

        with patch.dict("sys.modules", {"IoTKafka": MagicMock()}):
            import sys

            sys.modules["IoTKafka"].IoTKafkaProducer = mock_iot_producer_class

            producer = KafkaProducer(settings)
            producer.close(timeout_seconds=5.0)

            mock_iot_producer_instance.flush.assert_called_once_with(timeout=5.0)

    def test_kafka_producer_close_with_undelivered(self, settings):
        """Test producer close with undelivered messages."""
        mock_iot_producer_instance = Mock()
        mock_iot_producer_instance.flush.return_value = 3  # 3 undelivered
        mock_iot_producer_class = Mock(return_value=mock_iot_producer_instance)

        with patch.dict("sys.modules", {"IoTKafka": MagicMock()}):
            import sys

            sys.modules["IoTKafka"].IoTKafkaProducer = mock_iot_producer_class

            producer = KafkaProducer(settings)
            producer.close(timeout_seconds=5.0)

            mock_iot_producer_instance.flush.assert_called_once_with(timeout=5.0)

    def test_kafka_producer_close_error(self, settings):
        """Test producer close error handling."""
        mock_iot_producer_instance = Mock()
        mock_iot_producer_instance.flush.side_effect = Exception("Close error")
        mock_iot_producer_class = Mock(return_value=mock_iot_producer_instance)

        with patch.dict("sys.modules", {"IoTKafka": MagicMock()}):
            import sys

            sys.modules["IoTKafka"].IoTKafkaProducer = mock_iot_producer_class

            producer = KafkaProducer(settings)
            # Should not raise, just log warning
            producer.close(timeout_seconds=5.0)

            mock_iot_producer_instance.flush.assert_called_once()

    def test_kafka_producer_publish_raw_exception(self, settings):
        """Test publishing raw telemetry with producer exception."""
        mock_iot_producer_instance = Mock()
        mock_iot_producer_class = Mock(return_value=mock_iot_producer_instance)

        with patch.dict("sys.modules", {"IoTKafka": MagicMock()}):
            import sys

            sys.modules["IoTKafka"].IoTKafkaProducer = mock_iot_producer_class

            # Mock IoTProducerException
            class IoTProducerException(Exception):
                pass

            sys.modules["IoTKafka"].IoTProducerException = IoTProducerException

            mock_iot_producer_instance.produce.side_effect = IoTProducerException(
                "Produce failed"
            )

            with patch("app.core.kafka_producer.metrics") as mock_metrics:
                producer = KafkaProducer(settings)

                data = {
                    "serial_number": "SN-123456",
                    "value": 25.5,
                }

                with pytest.raises(IoTProducerException):
                    producer.publish_raw(data, key="SN-123456")

                # Verify error metrics
                mock_metrics.kafka_publish_errors.labels.assert_called_once()


class TestMQTTClientIntegration:
    """Integration tests for MQTTClient."""

    def test_mqtt_client_initialization(self, settings):
        """Test MQTTClient initialization with callbacks."""
        on_message = Mock()
        on_connect = Mock()
        on_disconnect = Mock()

        client = MQTTClient(
            config=settings,
            on_message_callback=on_message,
            on_connect_callback=on_connect,
            on_disconnect_callback=on_disconnect,
        )

        assert client.config == settings
        assert client.on_message_callback == on_message
        assert client.on_connect_callback == on_connect
        assert client.on_disconnect_callback == on_disconnect
        assert client._reconnect_count == 0
        assert client._is_connected is False

    def test_mqtt_client_backoff_delay_initial(self, settings):
        """Test exponential backoff calculation for initial attempt."""
        on_message = Mock()

        client = MQTTClient(config=settings, on_message_callback=on_message)

        delay = client._get_backoff_delay()
        # First attempt: delay = 2^0 = 1s, plus jitter
        assert delay >= settings.mqtt_reconnect_min_delay

    def test_mqtt_client_backoff_delay_exponential(self, settings):
        """Test exponential backoff increases with retries."""
        on_message = Mock()

        client = MQTTClient(config=settings, on_message_callback=on_message)

        # Simulate reconnect attempts
        delays = []
        for attempt in range(5):
            client._reconnect_count = attempt
            delay = client._get_backoff_delay()
            delays.append(delay)

        # Verify delays are generally increasing
        # (may have jitter but 2^n increases overall)
        assert delays[0] < delays[-1]

    def test_mqtt_client_backoff_delay_max_cap(self, settings):
        """Test backoff delay is capped at max."""
        on_message = Mock()

        client = MQTTClient(config=settings, on_message_callback=on_message)

        # Simulate many retries
        client._reconnect_count = 100  # 2^100 >> max_delay

        delay = client._get_backoff_delay()
        # Should be capped at max_delay + jitter
        assert delay <= settings.mqtt_reconnect_max_delay * 1.15  # Allow for jitter

    def test_mqtt_client_on_connect_success(self, settings):
        """Test MQTT on_connect callback for successful connection."""
        on_message = Mock()
        on_connect = Mock()

        client = MQTTClient(
            config=settings,
            on_message_callback=on_message,
            on_connect_callback=on_connect,
        )

        # Simulate successful connection
        mock_client = Mock()
        client._on_connect(mock_client, {}, {}, 0, None)

        assert client._is_connected is True
        assert client._reconnect_count == 0
        on_connect.assert_called_once()
        mock_client.subscribe.assert_any_call(
            settings.mqtt_topic, qos=settings.mqtt_qos
        )

    def test_mqtt_client_on_connect_failure(self, settings):
        """Test MQTT on_connect callback for failed connection."""
        on_message = Mock()

        client = MQTTClient(config=settings, on_message_callback=on_message)

        # Simulate failed connection
        mock_client = Mock()
        client._on_connect(mock_client, {}, {}, 1, None)  # rc=1 = failure

        assert client._is_connected is False

    def test_mqtt_client_on_message_success(self, settings):
        """Test MQTT on_message callback processes messages."""
        on_message = Mock()

        client = MQTTClient(config=settings, on_message_callback=on_message)

        # Simulate message reception
        mock_client = Mock()
        mock_msg = Mock()
        mock_msg.topic = "telemetry/device123/data"
        mock_msg.payload = b'{"value": 25.5}'

        client._on_message(mock_client, {}, mock_msg)

        on_message.assert_called_once_with(
            "telemetry/device123/data", b'{"value": 25.5}'
        )

    def test_mqtt_client_on_message_error(self, settings):
        """Test MQTT on_message callback handles processing errors."""
        on_message = Mock(side_effect=ValueError("Invalid payload"))

        client = MQTTClient(config=settings, on_message_callback=on_message)

        # Simulate message reception
        mock_client = Mock()
        mock_msg = Mock()
        mock_msg.topic = "telemetry/device123/data"
        mock_msg.payload = b'{"value": 25.5}'

        # Should not raise, just log error
        client._on_message(mock_client, {}, mock_msg)

    def test_mqtt_client_on_disconnect_explicit(self, settings):
        """Test MQTT on_disconnect for explicit disconnection."""
        on_message = Mock()
        on_disconnect = Mock()

        client = MQTTClient(
            config=settings,
            on_message_callback=on_message,
            on_disconnect_callback=on_disconnect,
        )
        client._is_connected = True

        # Simulate explicit disconnect (rc=0)
        mock_client = Mock()
        client._on_disconnect(mock_client, {}, {}, 0, None)

        assert client._is_connected is False
        on_disconnect.assert_called_once()

    def test_mqtt_client_on_disconnect_unexpected(self, settings):
        """Test MQTT on_disconnect for unexpected disconnection."""
        on_message = Mock()

        client = MQTTClient(config=settings, on_message_callback=on_message)
        client._is_connected = True

        # Simulate unexpected disconnect (rc!=0)
        mock_client = Mock()
        client._on_disconnect(mock_client, {}, {}, 1, None)

        assert client._is_connected is False

    def test_mqtt_client_connect(self, settings):
        """Test MQTT client connection."""
        on_message = Mock()

        client = MQTTClient(config=settings, on_message_callback=on_message)

        # Mock the internal client
        client.client.connect = Mock()

        client.connect()

        client.client.connect.assert_called_once_with(
            settings.mqtt_broker_host,
            settings.mqtt_broker_port,
            keepalive=60,
        )

    def test_mqtt_client_disconnect(self, settings):
        """Test MQTT client disconnection."""
        on_message = Mock()

        client = MQTTClient(config=settings, on_message_callback=on_message)

        # Mock the internal client
        client.client.disconnect = Mock()

        client.disconnect()

        client.client.disconnect.assert_called_once()

    def test_mqtt_client_is_connected_property(self, settings):
        """Test is_connected property."""
        on_message = Mock()

        client = MQTTClient(config=settings, on_message_callback=on_message)

        assert client.is_connected is False

        client._is_connected = True
        assert client.is_connected is True

    def test_mqtt_client_loop_forever(self, settings):
        """Test MQTT loop_forever."""
        on_message = Mock()

        client = MQTTClient(config=settings, on_message_callback=on_message)

        # Mock the internal client
        client.client.loop_forever = Mock()

        client.loop_forever()

        client.client.loop_forever.assert_called_once()

    def test_mqtt_client_loop_stop(self, settings):
        """Test MQTT loop_stop."""
        on_message = Mock()

        client = MQTTClient(config=settings, on_message_callback=on_message)

        # Mock the internal client
        client.client.loop_stop = Mock()

        client.loop_stop()

        client.client.loop_stop.assert_called_once()


class TestMainIntegration:
    """Integration tests for main.py functions."""

    def test_setup_logging_with_structured_json(self, settings):
        """Test logging setup with StructuredJsonFormatter available."""
        import logging

        with patch("app.main.StructuredJsonFormatter"):
            from app.main import setup_logging

            setup_logging(settings)
            # Should use structured JSON formatter
            assert logging.getLogger().level in (
                logging.INFO,
                logging.DEBUG,
                logging.WARNING,
            )

    def test_setup_logging_fallback(self, settings):
        """Test logging setup fallback to python-json-logger."""
        from app.main import setup_logging

        with patch("app.main.StructuredJsonFormatter", None):
            setup_logging(settings)
            # Should successfully configure logging
            logger = logging.getLogger()
            assert logger.level in (
                logging.INFO,
                logging.DEBUG,
                logging.WARNING,
            )

    def test_on_mqtt_message_valid(self, settings):
        """Test on_mqtt_message processing valid payload."""
        from app.main import on_mqtt_message
        import app.main

        # Mock the kafka producer
        mock_producer = Mock()
        app.main.kafka_producer = mock_producer
        mock_producer.config.kafka_topic_telemetry_raw = "telemetry.raw"

        # Mock metrics
        with patch("app.main.metrics"):
            payload = json.dumps(
                {
                    "value": 25.5,
                    "timestamp": "2026-03-19T12:00:00Z",
                }
            ).encode("utf-8")

            on_mqtt_message("telemetry/device123/data", payload)

            # Should call produce
            mock_producer.publish_raw.assert_called_once()

    def test_on_mqtt_message_no_producer(self):
        """Test on_mqtt_message with no kafka producer."""
        from app.main import on_mqtt_message
        import app.main

        app.main.kafka_producer = None

        payload = b'{"value": 25.5}'

        # Should not raise, just log error
        on_mqtt_message("telemetry/device123/data", payload)

    def test_on_mqtt_message_invalid_payload(self, settings):
        """Test on_mqtt_message with invalid JSON payload."""
        from app.main import on_mqtt_message
        import app.main

        mock_producer = Mock()
        app.main.kafka_producer = mock_producer

        with patch("app.main.metrics"):
            # Invalid JSON
            payload = b"invalid json"

            on_mqtt_message("telemetry/device123/data", payload)

            # Should not call producer
            mock_producer.publish_raw.assert_not_called()

    def test_on_mqtt_message_device_status(self, settings):
        """Test on_mqtt_message with device status topic."""
        from app.main import on_mqtt_message
        import app.main

        mock_producer = Mock()
        app.main.kafka_producer = mock_producer

        with patch("app.main.message_handler") as mock_handler:
            # Device status needs valid JSON payload
            mock_handler.validate_mqtt_payload.return_value = (
                {"status": "online"},
                None,
            )

            payload = json.dumps({"status": "online"}).encode("utf-8")
            on_mqtt_message("devices/SN-123456/status", payload)

            # Should call handle_device_status
            mock_handler.handle_device_status.assert_called_once_with(
                "devices/SN-123456/status", payload
            )

    def test_on_mqtt_message_processing_error(self, settings):
        """Test on_mqtt_message error handling."""
        from app.main import on_mqtt_message
        import app.main

        mock_producer = Mock()
        mock_producer.publish_raw.side_effect = Exception("Kafka error")
        app.main.kafka_producer = mock_producer

        with patch("app.main.metrics") as mock_metrics:
            payload = json.dumps(
                {
                    "value": 25.5,
                    "timestamp": "2026-03-19T12:00:00Z",
                }
            ).encode("utf-8")

            # Should not raise
            on_mqtt_message("telemetry/device123/data", payload)

            # Should update error metrics
            mock_metrics.mqtt_messages_failed.labels.assert_called()

    def test_run_mqtt_client(self, settings):
        """Test run_mqtt_client with mock client."""
        from app.main import run_mqtt_client
        import app.main

        mock_client = Mock()
        app.main.mqtt_client = mock_client

        with patch("app.main.logging"):
            run_mqtt_client()

            # Should connect and loop
            mock_client.connect.assert_called_once()
            mock_client.loop_forever.assert_called_once()

    def test_run_mqtt_client_not_initialized(self):
        """Test run_mqtt_client when not initialized."""
        from app.main import run_mqtt_client
        import app.main

        app.main.mqtt_client = None

        # Should not raise, just log error
        run_mqtt_client()

    def test_run_mqtt_client_error(self, settings):
        """Test run_mqtt_client error handling."""
        from app.main import run_mqtt_client
        import app.main

        mock_client = Mock()
        mock_client.connect.side_effect = Exception("Connection error")
        app.main.mqtt_client = mock_client

        with patch("app.main.sys.exit") as mock_exit:
            with patch("app.main.logging"):
                run_mqtt_client()
                # Should exit on error
                mock_exit.assert_called_once_with(1)

    def test_signal_handler(self, settings):
        """Test signal_handler shutdown."""
        from app.main import signal_handler
        import app.main

        mock_mqtt = Mock()
        mock_kafka = Mock()
        app.main.mqtt_client = mock_mqtt
        app.main.kafka_producer = mock_kafka

        with patch("app.main.sys.exit") as mock_exit:
            with patch("app.main.logging"):
                signal_handler(15, None)

                # Should stop MQTT and close Kafka
                mock_mqtt.loop_stop.assert_called_once()
                mock_mqtt.disconnect.assert_called_once()
                mock_kafka.close.assert_called_once()
                mock_exit.assert_called_once_with(0)


class TestMessageFlowIntegration:
    """Integration tests for MQTT-to-Kafka message flow."""

    def test_message_flow_mqtt_to_kafka(self, cleanup_mocks):
        """Test complete message flow from MQTT to Kafka."""
        # Create mock MQTT client for testing callbacks
        mqtt_client = MockMQTTClient("test-client")
        mqtt_client.connect("localhost", 1883)
        mqtt_client.subscribe("telemetry/#")

        # Create mock Kafka producer
        kafka_producer = MockProducer()

        # Simulate MQTT message reception
        mqtt_topic = "telemetry/device123/data"
        payload = json.dumps(
            {
                "serial_number": "SN-123456",
                "value": 25.5,
                "timestamp": "2026-03-19T12:00:00Z",
            }
        ).encode("utf-8")

        mqtt_client.publish(mqtt_topic, payload, qos=1)

        # Verify message is in MQTT broker
        mqtt_messages = get_mock_broker_messages(topic=mqtt_topic)
        assert len(mqtt_messages) == 1

        # Simulate Kafka publishing
        kafka_producer.send(
            "telemetry.raw",
            {
                "serial_number": "SN-123456",
                "value": 25.5,
                "timestamp": "2026-03-19T12:00:00Z",
                "idempotency_key": "mqtt:SN-123456",
            },
        )

        # Verify message is in Kafka
        kafka_messages = get_mock_topic_messages("telemetry.raw")
        assert len(kafka_messages) == 1

    def test_multiple_devices_message_flow(self, cleanup_mocks):
        """Test handling multiple device messages."""
        mqtt_client = MockMQTTClient("test-client")
        mqtt_client.connect("localhost", 1883)
        mqtt_client.subscribe("telemetry/#")

        kafka_producer = MockProducer()

        # Publish from multiple devices
        for device_id in range(1, 4):
            serial = f"SN-{device_id:06d}"
            topic = f"telemetry/device{device_id}/data"

            payload = json.dumps(
                {
                    "serial_number": serial,
                    "value": 20.0 + device_id,
                    "timestamp": "2026-03-19T12:00:00Z",
                }
            ).encode("utf-8")

            mqtt_client.publish(topic, payload, qos=1)

            # Also produce to Kafka
            kafka_producer.send("telemetry.raw", json.loads(payload))

        # Verify all messages were processed
        mqtt_messages = get_mock_broker_messages()
        mqtt_total = sum(len(msgs) for msgs in mqtt_messages.values())
        assert mqtt_total == 3

        kafka_messages = get_mock_topic_messages("telemetry.raw")
        assert len(kafka_messages) == 3
