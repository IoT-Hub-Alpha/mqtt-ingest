# MQTT Ingestion Service - Architecture

This document describes the design, architecture, and key implementation decisions for the mqtt-ingest microservice.

## Table of Contents

1. [Overview](#overview)
2. [Design Goals](#design-goals)
3. [System Architecture](#system-architecture)
4. [Message Flow](#message-flow)
5. [Key Components](#key-components)
6. [Idempotency Strategy](#idempotency-strategy)
7. [Error Handling](#error-handling)
8. [Logging & Observability](#logging--observability)
9. [Deployment Considerations](#deployment-considerations)

## Overview

**mqtt-ingest** is a dedicated microservice responsible for consuming telemetry from MQTT brokers and producing them to Kafka. It exists as a separate service from the monolith to:

- Isolate MQTT connection management from backend services
- Decouple MQTT protocol handling from business logic
- Enable independent scaling of MQTT ingestion
- Provide clean, idempotent Kafka messages for downstream processors

## Design Goals

1. **Reliability**: Never lose messages; handle MQTT disconnects and Kafka failures gracefully
2. **Idempotency**: Enable duplicate-free processing even with retries
3. **Observability**: Complete audit trail of every message through structured logging
4. **Simplicity**: Single responsibility - ingest, validate, publish
5. **Scalability**: Run multiple instances without coordination
6. **Performance**: Sub-second latency from MQTT to Kafka

## System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    MQTT Broker                              │
└────────────────────────┬────────────────────────────────────┘
                         │
                         │ MQTT Subscribe
                         │ telemetry/#
                         │
┌────────────────────────▼────────────────────────────────────┐
│                  mqtt-ingest Service                        │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  MQTT Client Thread (paho-mqtt)                      │  │
│  │  ├─ Connect with QoS=1                               │  │
│  │  ├─ Reconnect with exponential backoff (max 60s)    │  │
│  │  ├─ Receive messages via callback                    │  │
│  │  └─ Log: mqtt_message_received (DEBUG)              │  │
│  └──────────────┬───────────────────────────────────────┘  │
│                 │                                           │
│  ┌──────────────▼───────────────────────────────────────┐  │
│  │  Message Validation & Normalization                 │  │
│  │  ├─ Parse JSON (dict, list, string, bytes)          │  │
│  │  ├─ Extract serial_number (from topic or payload)   │  │
│  │  ├─ Generate idempotency_key (topic+sn+hash)        │  │
│  │  ├─ Build event envelope (add metadata)             │  │
│  │  └─ Log: warnings for validation failures (INFO)    │  │
│  └──────────────┬───────────────────────────────────────┘  │
│                 │                                           │
│  ┌──────────────▼───────────────────────────────────────┐  │
│  │  Kafka Producer (with retries)                       │  │
│  │  ├─ Publish to telemetry.raw (3 retry attempts)     │  │
│  │  ├─ Key: serial_number (partition routing)           │  │
│  │  ├─ Value: Full event envelope with idempotency_key │  │
│  │  └─ Log: kafka_message_enqueued (INFO)              │  │
│  └──────────────┬───────────────────────────────────────┘  │
│                 │                                           │
└─────────────────┼───────────────────────────────────────────┘
                  │
                  │ Kafka Produce
                  │ telemetry.raw topic
                  │
┌─────────────────▼───────────────────────────────────────────┐
│               Kafka Topic: telemetry.raw                    │
│    (Messages deduplicated by idempotency_key)              │
└─────────────────────────────────────────────────────────────┘
                  │
                  │ Kafka Consume
                  │
┌─────────────────▼───────────────────────────────────────────┐
│  Downstream Processors (rules, analytics, etc.)            │
└─────────────────────────────────────────────────────────────┘
```

## Message Flow

### 1. MQTT Message Reception

**Trigger**: MQTT broker publishes to subscribed topic

**Example**:
```
Topic: telemetry/device123/temperature
Payload: {"temp": 25.5, "humidity": 60}
```

**Processing**:
- MQTT client callback invoked
- Message logged at DEBUG level
- Payload size recorded in metrics

**Log**:
```json
{
  "message": "mqtt_message_received",
  "topic": "telemetry/device123/temperature",
  "payload_size": 38
}
```

---

### 2. Payload Validation

**Responsibility**: Ensure payload is valid and parseable

**Validation Rules**:
- Must be valid JSON (if text-based)
- If binary, must be decodable as JSON
- Can be dict, list, string, or bytes

**Failure Handling**:
- Invalid payloads are logged as warnings
- Message is discarded (not published to Kafka)
- `mqtt_messages_failed` counter incremented with reason

**Log (on failure)**:
```json
{
  "level": "WARNING",
  "message": "mqtt_payload_validation_failed",
  "topic": "telemetry/device123/data",
  "error": "Invalid JSON: Expecting value at line 1"
}
```

---

### 3. Serial Number Extraction

**Responsibility**: Identify the device that sent the message

**Extraction Priority**:
1. Topic path pattern: `devices/{serial}/status` → extract `{serial}`
2. Payload field: Look for `serial_number` or `serial` field in data
3. Fallback: Log warning, increment failure counter

**Why It Matters**:
- Serial number used as Kafka message key
- Enables topic partitioning by device
- Allows per-device ordering in Kafka

**Example Extractions**:

```
Topic: telemetry/device123/data
Payload: {"temp": 25.5}
→ serial_number = "device123" (from topic path)

---

Topic: telemetry/unknown/data
Payload: {"serial_number": "SN-987654", "temp": 25.5}
→ serial_number = "SN-987654" (from payload)
```

**Log (on failure)**:
```json
{
  "level": "WARNING",
  "message": "mqtt_missing_serial_number",
  "topic": "telemetry/unknown/data"
}
```

---

### 4. Event Envelope Construction

**Responsibility**: Wrap raw MQTT data with metadata for Kafka

**Envelope Structure**:
```json
{
  "data": {/* original MQTT payload */},
  "serial_number": "device123",
  "timestamp": "2026-03-16T10:30:00.123Z",
  "source_topic": "telemetry/device123/data",
  "idempotency_key": "telemetry/device123/data:device123:a1b2c3d4",
  "ingest_index": 0
}
```

**Idempotency Key Generation**:
```python
idempotency_key = f"{topic}:{serial_number}:{payload_hash}"
```

- **topic**: Source MQTT topic
- **serial_number**: Extracted device identifier
- **payload_hash**: Hash of serialized payload (ensures uniqueness)

**Purpose**: Allows Kafka consumers to detect and skip duplicate messages if mqtt-ingest retries a publish after temporary Kafka failure.

---

### 5. Kafka Publishing

**Responsibility**: Publish event to Kafka with retry logic

**Configuration**:
- **Topic**: `telemetry.raw` (configurable via `KAFKA_TOPIC_TELEMETRY_RAW`)
- **Key**: `serial_number` (enables per-device ordering)
- **Value**: Full event envelope (JSON serialized)
- **Retries**: 3 attempts via kafka-consumer-producer-lib

**Publishing Process**:
1. Call `producer.produce(topic, key, value, attempts=3)`
2. Library handles JSON serialization and retry logic
3. Async delivery confirmation via callback

**Success Log**:
```json
{
  "level": "INFO",
  "message": "kafka_message_enqueued",
  "topic": "telemetry.raw",
  "key": "device123",
  "idempotency_key": "telemetry/device123/data:device123:a1b2c3d4"
}
```

**Failure Log** (after 3 retries):
```json
{
  "level": "ERROR",
  "message": "kafka_publish_failed",
  "topic": "telemetry.raw",
  "error": "BrokerNotAvailable: Broker is not available",
  "attempts": 3
}
```

---

## Key Components

### MQTT Client (`app/mqtt_client.py`)

**Responsibility**: Manage MQTT connection lifecycle

**Key Features**:
- Non-blocking connection using paho-mqtt's `loop_forever()`
- Automatic reconnection with exponential backoff
- Reconnect delay: 2^n seconds, capped at 60 seconds
- Subscribes to configurable topic filter (default: `telemetry/#`)
- QoS = 1 (at-least-once delivery)

**Reconnection Algorithm**:
```python
reconnect_delay = min(2 ** attempt_count, 60)  # exponential backoff
```

**Events Logged**:
- MQTT connection established
- MQTT disconnected (with reason code)
- Reconnection attempts
- Message reception

**Error Handling**:
- On connection failure: Logs and retries automatically
- On unexpected disconnect: Logs reason and starts reconnect loop
- On thread exit: Graceful shutdown in lifespan shutdown handler

---

### Message Handler (`app/message_handler.py`)

**Responsibility**: Validate, normalize, and enrich MQTT messages

**Key Functions**:
- `validate_mqtt_payload()`: Parse and validate JSON
- `extract_serial_number()`: Identify device
- `build_idempotency_key()`: Create deduplication key
- `build_raw_event()`: Construct event envelope
- `handle_device_status()`: Process LWT (Last Will Testament) messages

**Validation Strategy**:
- Accept dict, list, string, or bytes
- Attempt JSON parsing with error handling
- Return (data, error) tuple for caller to handle

**Serial Number Extraction Strategy**:
- Check topic path for `devices/{serial}/status` pattern
- If not found, check payload for `serial_number` or `serial` fields
- Return None if not found (caller logs and increments failure counter)

---

### Kafka Producer (`app/kafka_producer.py`)

**Responsibility**: Publish to Kafka with metrics tracking

**Key Features**:
- Wrapper around `IoTKafkaProducer` from kafka-consumer-producer-lib
- Metrics collection for delivery confirmations
- Retry logic handled by underlying library (3 attempts)
- Graceful shutdown with message flush

**Delivery Callback**:
- Invoked after Kafka delivers or fails the message
- On success: Logs partition/offset, increments success counter
- On failure: Logs error type, increments failure counter

**Exception Handling**:
- `IoTProducerException`: Caught after 3 retries, logged and re-raised
- Service does not continue on Kafka failure (fail-fast approach)

---

### HTTP Server (`app/main.py`)

**Responsibility**: Provide health/readiness probes and metrics

**Endpoints**:
- `GET /`: Service info (name, version, status)
- `GET /health`: Liveness probe (returns 200 if running)
- `GET /ready`: Readiness probe (checks MQTT and Kafka)
- `GET /metrics`: Prometheus metrics

**Lifespan Management**:
- **Startup**: Initialize MQTT client, Kafka producer, start MQTT thread
- **Shutdown**: Stop MQTT loop, close Kafka producer, log completion

---

### Prometheus Metrics (`app/metrics.py`)

**Key Metrics**:

| Metric | Type | Labels | Purpose |
|--------|------|--------|---------|
| `mqtt_messages_received` | Counter | `topic` | Total MQTT messages |
| `mqtt_messages_failed` | Counter | `topic`, `reason` | Failed MQTT messages |
| `kafka_messages_published` | Counter | `topic` | Successfully published to Kafka |
| `kafka_publish_errors` | Counter | `topic`, `error_type` | Kafka publish failures |
| `processing_duration` | Histogram | | Time from MQTT to Kafka publish |

---

## Idempotency Strategy

### Problem

MQTT provides at-least-once delivery, and Kafka producer uses retries. This means:
- MQTT might send the same message twice
- Kafka producer might retry after a timeout (even if message was delivered)

Result: Duplicate messages can reach Kafka.

### Solution

**Idempotency Key in Event Envelope**:

```python
idempotency_key = build_idempotency_key(
    topic=topic,
    serial_number=serial_number,
    payload=data,
    payload_bytes=payload
)
```

- Generated for every MQTT message
- Included in event envelope published to Kafka
- Unique per (topic, serial_number, payload) combination

### Kafka Consumer Deduplication

Downstream consumers should implement idempotency:

```python
# In Kafka consumer
seen_idempotency_keys = set()

for msg in consumer:
    event = json.loads(msg.value())
    idempotency_key = event["idempotency_key"]

    if idempotency_key in seen_idempotency_keys:
        logger.debug("Duplicate message, skipping")
        continue

    seen_idempotency_keys.add(idempotency_key)
    # Process message
```

---

## Error Handling

### MQTT Connection Failures

**Scenario**: MQTT broker unreachable

**Handling**:
1. MQTT client logs connection error
2. Automatic reconnection with exponential backoff
3. Service continues running (HTTP endpoints available)
4. No messages published until connection restored

**Recovery**: When broker comes back online, reconnection succeeds and ingestion resumes

---

### Invalid MQTT Payload

**Scenario**: Message doesn't parse as JSON

**Handling**:
1. Validation fails, returns (None, error_message)
2. Log warning with topic and error
3. Increment `mqtt_messages_failed` counter
4. Message discarded (not published to Kafka)

**No Recovery**: Invalid messages are silently dropped

---

### Missing Serial Number

**Scenario**: Can't extract device identifier

**Handling**:
1. `extract_serial_number()` returns None
2. Log warning with topic
3. Increment `mqtt_messages_failed` counter
4. Message discarded (not published to Kafka)

**Context**: May indicate malformed topic or payload schema

---

### Kafka Publish Failure

**Scenario**: Kafka broker unreachable after 3 retries

**Handling**:
1. Library retries 3 times internally
2. Raises `IoTProducerException` after retries exhausted
3. Exception caught, logged with `attempts: 3`
4. Exception re-raised to stop service

**Design Decision**: Fail-fast approach
- Ensures no messages are silently lost
- Operator notified immediately
- Service stops (Kubernetes will restart)
- When kafka comes back, service resumes from next MQTT message

**No Buffering**: mqtt-ingest does not buffer messages locally
- MQTT broker is the buffer (persists unacknowledged messages)
- When service recovers, MQTT broker re-sends buffered messages

---

## Logging & Observability

### Structured JSON Logging

All logs are structured JSON via `iot_logging` library:

```json
{
  "timestamp": "2026-03-16T10:30:00.123Z",
  "level": "INFO",
  "logger": "app.main",
  "message": "kafka_message_enqueued",
  "topic": "telemetry.raw",
  "key": "device123",
  "idempotency_key": "telemetry/device123/data:device123:a1b2c3d4"
}
```

### Log Levels

- **DEBUG**: Low-volume events (MQTT messages, queue operations)
- **INFO**: Important milestones (service start, Kafka publish)
- **WARNING**: Recoverable issues (validation failures, missing fields)
- **ERROR**: Failures requiring attention (connection lost, publish failed)

### Log Audit Trail

Following an MQTT message from ingestion to Kafka:

1. **MQTT Reception** (DEBUG):
   ```
   mqtt_message_received, topic=telemetry/device123/data
   ```

2. **Validation** (DEBUG if success, WARNING if failed):
   ```
   mqtt_payload_validation_failed, error=Invalid JSON
   ```

3. **Serial Extraction** (WARNING if failed):
   ```
   mqtt_missing_serial_number, topic=telemetry/device123/data
   ```

4. **Kafka Publish** (INFO if success, ERROR if failed):
   ```
   kafka_message_enqueued, key=device123, idempotency_key=...
   ```

---

## Deployment Considerations

### Single Replica vs Multiple Replicas

**Single Replica**:
- ✅ Simple deployment
- ❌ No redundancy (downtime = message loss in flight)
- ❌ MQTT broker must buffer messages

**Multiple Replicas**:
- ✅ Redundancy - if one instance fails, another picks up
- ✅ MQTT broker continues accepting messages
- ⚠️ Must handle parallel message processing carefully

**MQTT Constraint**: Only one client can have same `client_id` connected at a time

**Solution**: Each replica uses unique `client_id`:
```python
client_id = f"mqtt-ingest-{pod_name}"  # K8s pod name, or hostname
```

### Scaling Strategy

**Vertical Scaling**: Increase MQTT and Kafka broker capacity (not mqtt-ingest)

**Horizontal Scaling**: Run multiple mqtt-ingest instances:
1. MQTT broker distributes subscribed topics across clients
2. Each client publishes same-keyed messages to same Kafka partition
3. Kafka ensures ordering within partition
4. Downstream consumers read from same topic

### Kafka Partitioning

**Current Strategy**: Key = `serial_number`
- All messages from device123 → same partition
- Preserves per-device ordering in Kafka
- Enables exactly-once processing per device

**Alternative**: Key = None (round-robin)
- Distributes load evenly
- Loses ordering guarantee
- Use if downstream doesn't require ordering

---

## Future Enhancements

### 1. Metrics Export to Backend

Currently: Prometheus scrapes `/metrics`

Future: Send metrics to backend database for long-term storage

```python
# Pseudo-code
@scheduler.every(1).minutes.do
def export_metrics():
    metrics_dict = metrics.export()
    send_to_backend(metrics_dict)
```

### 2. Device Registry Integration

Validate serial numbers against backend device registry before publishing:

```python
if not device_registry.exists(serial_number):
    logger.warning("Device not registered")
    increment_counter("mqtt_messages_dropped_unregistered")
    return
```

### 3. Rate Limiting

Add per-device rate limiting to prevent floods:

```python
rate_limiter.check(serial_number, limit=100_messages_per_minute)
```

### 4. Message Transformation

Add pluggable transformers for message normalization:

```python
# Example: Celsius to Fahrenheit
transformers = [
    TemperatureTransformer(),
    UnitNormalizer(),
]
for transformer in transformers:
    data = transformer.apply(data)
```

---

## Testing Strategy

### Unit Tests

- **MQTT Client**: Mocked paho-mqtt, test reconnect logic
- **Message Handler**: Validation, extraction, envelope building
- **Kafka Producer**: Mocked IoTKafkaProducer, test callbacks
- **Endpoints**: Test /health, /ready, /metrics responses

### Integration Tests

- **End-to-End**: Real MQTT broker, real Kafka, test full flow
- **Failure Scenarios**: Broker unavailable, invalid payloads

### Test Coverage

Target: **80%+ code coverage** (configured in pyproject.toml)

Commands:
```bash
pytest --cov=app --cov-report=html
open htmlcov/index.html
```

---

## References

- [MQTT Specification](http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/mqtt-v3.1.1.html)
- [Kafka Protocol](https://kafka.apache.org/protocol.html)
- [Paho MQTT Python Client](https://github.com/eclipse/paho.mqtt.python)
- [kafka-consumer-producer-lib](https://github.com/IoT-Hub-Alpha/kafka-consumer-producer-lib)
- [iot-logging Library](https://github.com/IoT-Hub-Alpha/logging-lib)
