"""Prometheus metrics for MQTT ingestion microservice."""

from prometheus_client import Counter, Gauge, Histogram

# MQTT Metrics
mqtt_messages_received = Counter(
    "mqtt_messages_received_total",
    "Total MQTT messages received",
    ["topic"],
)

mqtt_messages_processed = Counter(
    "mqtt_messages_processed_total",
    "Total MQTT messages successfully processed",
    ["topic"],
)

mqtt_messages_failed = Counter(
    "mqtt_messages_failed_total",
    "Total MQTT messages that failed processing",
    ["topic", "reason"],
)

mqtt_connection_status = Gauge(
    "mqtt_connection_status",
    "MQTT broker connection status (1=connected, 0=disconnected)",
)

# Kafka Metrics
kafka_messages_published = Counter(
    "kafka_messages_published_total",
    "Total messages published to Kafka",
    ["topic"],
)

kafka_publish_errors = Counter(
    "kafka_publish_errors_total",
    "Total errors publishing to Kafka",
    ["topic", "error_type"],
)

kafka_publish_latency = Histogram(
    "kafka_publish_latency_seconds",
    "Latency of Kafka publish operations",
    ["topic"],
    buckets=(0.01, 0.05, 0.1, 0.5, 1.0, 5.0),
)

# Processing Metrics
processing_duration = Histogram(
    "mqtt_message_processing_duration_seconds",
    "Time spent processing MQTT message",
    buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5),
)

idempotency_key_cache_size = Gauge(
    "idempotency_key_cache_size",
    "Size of idempotency key cache",
)

# Device Status Metrics
device_status_changes = Counter(
    "device_status_changes_total",
    "Total device status changes (online/offline)",
    ["status"],
)
