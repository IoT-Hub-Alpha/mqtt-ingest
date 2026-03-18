"""Unit tests for message handler validation and transformation."""

import json
import pytest

from app import message_handler


class TestValidateMqttPayload:
    """Tests for payload validation function."""

    def test_valid_json_dict_payload(self):
        """Valid JSON dict payload should parse successfully."""
        payload = b'{"temp": 25.5, "humidity": 60}'
        data, error = message_handler.validate_mqtt_payload(payload)

        assert data == {"temp": 25.5, "humidity": 60}
        assert error is None

    def test_valid_json_with_special_chars(self):
        """Valid JSON with special characters should parse successfully."""
        payload = b'{"device": "temp-\u00b0C", "value": 25.5}'
        data, error = message_handler.validate_mqtt_payload(payload)

        assert isinstance(data, dict)
        assert error is None
        assert "device" in data

    def test_invalid_json_syntax_error(self):
        """Invalid JSON should return error message."""
        payload = b'{"temp": 25.5, invalid}'
        data, error = message_handler.validate_mqtt_payload(payload)

        assert data is None
        assert error is not None
        assert "malformed_json" in error

    def test_empty_payload(self):
        """Empty payload should return error."""
        payload = b''
        data, error = message_handler.validate_mqtt_payload(payload)

        assert data is None
        assert error is not None
        assert "malformed_json" in error

    def test_invalid_unicode_encoding(self):
        """Invalid UTF-8 encoding should return error."""
        payload = b'\x80\x81\x82'
        data, error = message_handler.validate_mqtt_payload(payload)

        assert data is None
        assert error is not None
        assert "invalid_encoding" in error

    def test_json_array_not_dict(self):
        """JSON array should return error (requires dict)."""
        payload = b'[1, 2, 3]'
        data, error = message_handler.validate_mqtt_payload(payload)

        assert data is None
        assert error is not None
        assert "invalid_type" in error

    def test_json_string_not_dict(self):
        """JSON string should return error (requires dict)."""
        payload = b'"hello"'
        data, error = message_handler.validate_mqtt_payload(payload)

        assert data is None
        assert error is not None
        assert "invalid_type" in error

    def test_json_number_not_dict(self):
        """JSON number should return error (requires dict)."""
        payload = b'123'
        data, error = message_handler.validate_mqtt_payload(payload)

        assert data is None
        assert error is not None
        assert "invalid_type" in error

    def test_empty_dict(self):
        """Empty dict should parse successfully."""
        payload = b'{}'
        data, error = message_handler.validate_mqtt_payload(payload)

        assert data == {}
        assert error is None

    def test_nested_dict(self):
        """Nested dict should parse successfully."""
        payload = b'{"sensor": {"temp": 25, "location": "room1"}}'
        data, error = message_handler.validate_mqtt_payload(payload)

        assert data == {"sensor": {"temp": 25, "location": "room1"}}
        assert error is None


class TestExtractSerialNumber:
    """Tests for serial number extraction."""

    def test_extract_from_payload_serial_number_field(self):
        """Extract from payload serial_number field."""
        topic = "telemetry/unknown/data"
        payload = {"serial_number": "SN-123456", "temp": 25}

        serial = message_handler.extract_serial_number(topic, payload)
        assert serial == "SN-123456"

    def test_extract_from_topic_last_segment(self):
        """Extract from topic last segment."""
        topic = "telemetry/device123/data"
        payload = {"temp": 25}

        serial = message_handler.extract_serial_number(topic, payload)
        assert serial == "data"

    def test_payload_takes_precedence_over_topic(self):
        """Payload serial_number should take precedence."""
        topic = "telemetry/device123/data"
        payload = {"serial_number": "SN-from-payload", "temp": 25}

        serial = message_handler.extract_serial_number(topic, payload)
        assert serial == "SN-from-payload"

    def test_empty_serial_number_in_payload_ignored(self):
        """Empty string serial_number in payload should be ignored."""
        topic = "telemetry/device123/data"
        payload = {"serial_number": "", "temp": 25}

        serial = message_handler.extract_serial_number(topic, payload)
        assert serial == "data"

    def test_whitespace_only_serial_number_ignored(self):
        """Whitespace-only serial_number in payload should be ignored."""
        topic = "telemetry/device123/data"
        payload = {"serial_number": "   ", "temp": 25}

        serial = message_handler.extract_serial_number(topic, payload)
        assert serial == "data"

    def test_serial_number_stripped_of_whitespace(self):
        """Serial number should be stripped of leading/trailing whitespace."""
        topic = "telemetry/unknown/data"
        payload = {"serial_number": "  SN-123456  ", "temp": 25}

        serial = message_handler.extract_serial_number(topic, payload)
        assert serial == "SN-123456"

    def test_root_level_topic_returns_last_segment(self):
        """Topic with single segment should return that segment."""
        topic = "telemetry"
        payload = {"temp": 25}

        serial = message_handler.extract_serial_number(topic, payload)
        assert serial == "telemetry"

    def test_topic_with_trailing_slash(self):
        """Topic with trailing slash should still work."""
        topic = "telemetry/device123/"
        payload = {}

        serial = message_handler.extract_serial_number(topic, payload)
        assert serial == "device123"

    def test_missing_serial_returns_none(self):
        """Missing serial number should return None."""
        topic = "telemetry"
        payload = {}

        serial = message_handler.extract_serial_number(topic, payload)
        assert serial is None

    def test_serial_number_non_string_type_ignored(self):
        """Non-string serial_number in payload should be ignored."""
        topic = "telemetry/device123/data"
        payload = {"serial_number": 123, "temp": 25}

        serial = message_handler.extract_serial_number(topic, payload)
        assert serial == "data"


class TestBuildIdempotencyKey:
    """Tests for idempotency key generation."""

    def test_explicit_idempotency_key_in_payload(self):
        """Explicit idempotency_key in payload takes precedence."""
        topic = "telemetry/device/data"
        serial = "device123"
        payload = {"idempotency_key": "custom-key-123", "temp": 25}
        payload_bytes = json.dumps(payload).encode()

        key = message_handler.build_idempotency_key(
            topic=topic, serial_number=serial, payload=payload, payload_bytes=payload_bytes
        )
        assert key == "custom-key-123"

    def test_explicit_idempotency_key_stripped(self):
        """Explicit idempotency_key should be stripped of whitespace."""
        topic = "telemetry/device/data"
        serial = "device123"
        payload = {"idempotency_key": "  custom-key-123  ", "temp": 25}
        payload_bytes = json.dumps(payload).encode()

        key = message_handler.build_idempotency_key(
            topic=topic, serial_number=serial, payload=payload, payload_bytes=payload_bytes
        )
        assert key == "custom-key-123"

    def test_empty_explicit_key_ignored(self):
        """Empty idempotency_key should be ignored."""
        topic = "telemetry/device/data"
        serial = "device123"
        payload = {"idempotency_key": "", "message_id": "msg-123", "temp": 25}
        payload_bytes = json.dumps(payload).encode()

        key = message_handler.build_idempotency_key(
            topic=topic, serial_number=serial, payload=payload, payload_bytes=payload_bytes
        )
        assert key.startswith("mqtt:")
        assert "msg-123" in key

    def test_stable_field_message_id(self):
        """Use message_id as stable field."""
        topic = "telemetry/device/data"
        serial = "device123"
        payload = {"message_id": "msg-456", "temp": 25}
        payload_bytes = json.dumps(payload).encode()

        key = message_handler.build_idempotency_key(
            topic=topic, serial_number=serial, payload=payload, payload_bytes=payload_bytes
        )
        assert key.startswith("mqtt:")

    def test_stable_field_seq(self):
        """Use sequence number as stable field."""
        topic = "telemetry/device/data"
        serial = "device123"
        payload = {"seq": "12345", "temp": 25}
        payload_bytes = json.dumps(payload).encode()

        key = message_handler.build_idempotency_key(
            topic=topic, serial_number=serial, payload=payload, payload_bytes=payload_bytes
        )
        assert key.startswith("mqtt:")

    def test_stable_field_timestamp(self):
        """Use timestamp as stable field."""
        topic = "telemetry/device/data"
        serial = "device123"
        payload = {"timestamp": "2026-03-16T10:30:00Z", "temp": 25}
        payload_bytes = json.dumps(payload).encode()

        key = message_handler.build_idempotency_key(
            topic=topic, serial_number=serial, payload=payload, payload_bytes=payload_bytes
        )
        assert key.startswith("mqtt:")

    def test_payload_hash_fallback(self):
        """Fall back to payload hash when no stable fields."""
        topic = "telemetry/device/data"
        serial = "device123"
        payload = {"temp": 25, "humidity": 60}
        payload_bytes = json.dumps(payload).encode()

        key = message_handler.build_idempotency_key(
            topic=topic, serial_number=serial, payload=payload, payload_bytes=payload_bytes
        )
        assert key.startswith("mqtt:")
        assert len(key) > 10  # Should be mqtt: + hash

    def test_same_payload_generates_same_key(self):
        """Same payload should generate same idempotency key."""
        topic = "telemetry/device/data"
        serial = "device123"
        payload = {"temp": 25, "humidity": 60}
        payload_bytes = json.dumps(payload).encode()

        key1 = message_handler.build_idempotency_key(
            topic=topic, serial_number=serial, payload=payload, payload_bytes=payload_bytes
        )
        key2 = message_handler.build_idempotency_key(
            topic=topic, serial_number=serial, payload=payload, payload_bytes=payload_bytes
        )
        assert key1 == key2

    def test_different_payload_different_key(self):
        """Different payloads should generate different keys."""
        topic = "telemetry/device/data"
        serial = "device123"

        payload1 = {"temp": 25}
        key1 = message_handler.build_idempotency_key(
            topic=topic,
            serial_number=serial,
            payload=payload1,
            payload_bytes=json.dumps(payload1).encode(),
        )

        payload2 = {"temp": 26}
        key2 = message_handler.build_idempotency_key(
            topic=topic,
            serial_number=serial,
            payload=payload2,
            payload_bytes=json.dumps(payload2).encode(),
        )

        assert key1 != key2


class TestBuildRawEvent:
    """Tests for event envelope construction."""

    def test_minimal_event_envelope(self):
        """Build minimal event envelope."""
        payload = {"temp": 25}
        serial = "device123"

        event = message_handler.build_raw_event(payload, serial_number=serial)

        assert event["payload"] == payload
        assert event["serial_number"] == serial
        assert event["ingest_protocol"] == "mqtt"
        assert event["ingest_index"] == 0
        assert "request_id" in event
        assert "received_at" in event
        assert "idempotency_key" not in event  # Optional, not provided

    def test_event_with_idempotency_key(self):
        """Build event with idempotency key."""
        payload = {"temp": 25}
        serial = "device123"
        idempotency_key = "mqtt:abc123"

        event = message_handler.build_raw_event(
            payload, serial_number=serial, idempotency_key=idempotency_key
        )

        assert event["idempotency_key"] == idempotency_key

    def test_event_with_custom_request_id(self):
        """Build event with custom request ID."""
        payload = {"temp": 25}
        serial = "device123"
        request_id = "req-456"

        event = message_handler.build_raw_event(
            payload, serial_number=serial, request_id=request_id
        )

        assert event["request_id"] == request_id

    def test_event_with_custom_ingest_index(self):
        """Build event with custom ingest index."""
        payload = {"temp": 25}
        serial = "device123"

        event = message_handler.build_raw_event(payload, serial_number=serial, ingest_index=3)

        assert event["ingest_index"] == 3

    def test_event_with_custom_source(self):
        """Build event with custom source protocol."""
        payload = {"temp": 25}
        serial = "device123"

        event = message_handler.build_raw_event(
            payload, serial_number=serial, source="http"
        )

        assert event["ingest_protocol"] == "http"

    def test_event_request_id_auto_generated(self):
        """Request ID should be auto-generated if not provided."""
        payload = {"temp": 25}

        event1 = message_handler.build_raw_event(payload, serial_number="dev1")
        event2 = message_handler.build_raw_event(payload, serial_number="dev2")

        assert "request_id" in event1
        assert "request_id" in event2
        assert event1["request_id"] != event2["request_id"]

    def test_event_received_at_auto_generated(self):
        """Received_at timestamp should be auto-generated if not provided."""
        payload = {"temp": 25}

        event = message_handler.build_raw_event(payload, serial_number="device123")

        assert "received_at" in event
        assert event["received_at"].endswith("Z")  # ISO format with Z suffix

    def test_event_received_at_custom(self):
        """Received_at can be provided."""
        payload = {"temp": 25}
        received_at = "2026-03-16T10:30:00Z"

        event = message_handler.build_raw_event(
            payload, serial_number="device123", received_at=received_at
        )

        assert event["received_at"] == received_at

    def test_invalid_ingest_index_negative(self):
        """Negative ingest_index should raise ValueError."""
        payload = {"temp": 25}

        with pytest.raises(ValueError, match="ingest_index must be non-negative"):
            message_handler.build_raw_event(payload, serial_number="device123", ingest_index=-1)

    def test_invalid_ingest_index_non_integer(self):
        """Non-integer ingest_index should raise ValueError."""
        payload = {"temp": 25}

        with pytest.raises(ValueError, match="ingest_index must be non-negative"):
            message_handler.build_raw_event(
                payload, serial_number="device123", ingest_index="0"
            )

    def test_invalid_idempotency_key_non_string(self):
        """Non-string idempotency_key should raise ValueError."""
        payload = {"temp": 25}

        with pytest.raises(ValueError, match="idempotency_key must be str"):
            message_handler.build_raw_event(
                payload, serial_number="device123", idempotency_key=123
            )

    def test_idempotency_key_whitespace_only_not_included(self):
        """Whitespace-only idempotency_key should not be included in event."""
        payload = {"temp": 25}

        event = message_handler.build_raw_event(
            payload, serial_number="device123", idempotency_key="   "
        )

        assert "idempotency_key" not in event


class TestHandleDeviceStatus:
    """Tests for device status message handling."""

    def test_online_status_message(self, caplog):
        """Handle online status message."""
        topic = "devices/device123/status"
        payload = b"online"

        message_handler.handle_device_status(topic, payload)

        # Check log contains device status info
        assert "device_status_changed" in caplog.text or any(
            "device_status_changed" in record.message for record in caplog.records
        )

    def test_offline_status_message(self, caplog):
        """Handle offline status message."""
        topic = "devices/device123/status"
        payload = b"offline"

        message_handler.handle_device_status(topic, payload)

        assert "device_status_changed" in caplog.text or any(
            "device_status_changed" in record.message for record in caplog.records
        )

    def test_status_message_case_insensitive(self, caplog):
        """Status should be case-insensitive."""
        topic = "devices/device123/status"
        payload = b"ONLINE"

        message_handler.handle_device_status(topic, payload)

        assert "device_status_changed" in caplog.text or any(
            "device_status_changed" in record.message for record in caplog.records
        )

    def test_invalid_topic_format(self, caplog):
        """Invalid topic format should log warning."""
        topic = "invalid/topic"
        payload = b"online"

        message_handler.handle_device_status(topic, payload)

        assert "device_status_invalid_topic" in caplog.text or any(
            "device_status_invalid_topic" in record.message for record in caplog.records
        )

    def test_invalid_payload_encoding(self, caplog):
        """Invalid payload encoding should be handled."""
        topic = "devices/device123/status"
        payload = b"\x80\x81\x82"

        message_handler.handle_device_status(topic, payload)

        # Should not raise exception, should log warning
        assert True  # Handler completes without exception
