"""Core MQTT and Kafka infrastructure."""

from .kafka_producer import KafkaProducer
from .message_handler import (
    build_idempotency_key,
    build_raw_event,
    extract_serial_number,
    handle_device_status,
    validate_mqtt_payload,
)
from .mqtt_client import MQTTClient

__all__ = [
    "MQTTClient",
    "KafkaProducer",
    "extract_serial_number",
    "build_idempotency_key",
    "build_raw_event",
    "validate_mqtt_payload",
    "handle_device_status",
]
