# MQTT Ingestion Microservice

Independent MQTT to Kafka ingestion microservice for IoT Hub Alpha. Consumes telemetry messages from MQTT brokers and publishes them to Kafka for downstream processing.

## Overview

**mqtt-ingest** is a lightweight, production-ready microservice that:

- Subscribes to MQTT topics and receives telemetry messages
- Validates and normalizes MQTT payloads
- Extracts device identifiers from topic/payload
- Publishes to Kafka with idempotent message keys
- Provides HTTP health/readiness endpoints
- Exposes Prometheus metrics for monitoring
- Uses structured JSON logging for observability

## Architecture

```
MQTT Broker → [MQTT Client Thread] → [Message Handler] → [Kafka Producer] → Kafka Topic
                                           ↓
                                    [Validation]
                                    [Serial Number Extraction]
                                    [Idempotency Key Generation]
```

### Key Components

- **MQTT Client** (`app/mqtt_client.py`): Async MQTT connection with reconnect logic
- **Message Handler** (`app/message_handler.py`): Payload validation, normalization, serial extraction
- **Kafka Producer** (`app/kafka_producer.py`): Publishes to Kafka with retry logic (3 attempts)
- **Metrics** (`app/metrics.py`): Prometheus metrics for messages, errors, processing time
- **Health Checks** (`app/health.py`): Liveness and readiness probes

## Quick Start

### Local Development

```bash
# Install dependencies
pip install -e .

# Install test dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Start service (requires MQTT broker and Kafka running)
python -m app.main
```

### Docker

```bash
# Build image
docker build -t mqtt-ingest:latest .

# Run container
docker run -p 8000:8000 -p 9103:9103 \
  -e MQTT_BROKER_HOST=mosquitto \
  -e KAFKA_BROKERS=kafka:9092 \
  mqtt-ingest:latest
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| **MQTT Configuration** | | |
| `MQTT_BROKER_HOST` | `mosquitto` | MQTT broker hostname |
| `MQTT_BROKER_PORT` | `1883` | MQTT broker port |
| `MQTT_TOPIC` | `telemetry/#` | Topic filter to subscribe to (supports wildcards) |
| `MQTT_QOS` | `1` | MQTT Quality of Service (0, 1, or 2) |
| `MQTT_CONNECT_TIMEOUT` | `10` | Timeout for initial MQTT connection (seconds) |
| `MQTT_RECONNECT_MIN_DELAY` | `1.0` | Minimum delay for reconnection (seconds) |
| `MQTT_RECONNECT_MAX_DELAY` | `60.0` | Maximum delay for reconnection (seconds) |
| **Kafka Configuration** | | |
| `KAFKA_BROKERS` | `kafka:9092` | Comma-separated list of Kafka brokers |
| `KAFKA_TOPIC_TELEMETRY_RAW` | `telemetry.raw` | Kafka topic for raw telemetry |
| **HTTP Server** | | |
| `HTTP_HOST` | `0.0.0.0` | HTTP server bind address |
| `HTTP_PORT` | `8000` | HTTP server port |
| `METRICS_PORT` | `9103` | Prometheus metrics port |
| **Logging & Application** | | |
| `LOG_LEVEL` | `INFO` | Logging level (DEBUG, INFO, WARNING, ERROR) |
| `LOG_FORMAT` | `json` | Log format (json or standard) |
| `SERVICE_NAME` | `mqtt-ingest` | Service name for logging |
| `ENVIRONMENT` | `development` | Environment (development, staging, production) |

## API Endpoints

### Health & Status

- `GET /` - Service info (name, version, status)
- `GET /health` - Health check (returns 200 if healthy)
- `GET /ready` - Readiness probe (checks MQTT connection, Kafka availability)
- `GET /metrics` - Prometheus metrics endpoint

## Message Flow

### 1. MQTT Message Reception
```
Topic: telemetry/device1/data
Payload: {"temp": 25.5, "humidity": 60}
↓
logger.debug("mqtt_message_received", extra={"topic": "...", "payload_size": ...})
```

### 2. Payload Validation
Validates JSON structure and required fields:
- Must be valid JSON (dict, list, string, or bytes)
- Logs warnings for invalid payloads
- Increments `mqtt_messages_failed` metric

### 3. Serial Number Extraction
Extracts device identifier from:
- Topic path: `devices/{serial}/status` → `serial`
- Payload field: `data["serial_number"]`
- Logs warning if missing, increments `mqtt_messages_failed`

### 4. Idempotency Key Generation
Creates unique key from: `topic + serial_number + payload_hash`
- Enables Kafka deduplication
- Prevents duplicate processing

### 5. Kafka Publishing
```
Publishes to telemetry.raw topic with:
- Key: serial_number (for routing)
- Value: Full event envelope with metadata
- Retries: 3 attempts via kafka-consumer-producer-lib
↓
logger.info("kafka_message_enqueued", extra={
    "topic": "telemetry.raw",
    "key": "device1",
    "idempotency_key": "..."
})
```

## Logging

All logs are structured JSON with automatic context injection via `iot-logging-lib`:

### Log Levels

- **DEBUG**: Low-level events (raw MQTT messages, message enqueued)
- **INFO**: Important milestones (service start, Kafka messages published)
- **WARNING**: Recoverable issues (validation failures, reconnect attempts)
- **ERROR**: Failures requiring attention (MQTT errors, Kafka publish failures)

### Example Logs

**MQTT Message Received:**
```json
{
  "timestamp": "2026-03-16T10:30:00.123Z",
  "level": "DEBUG",
  "logger": "app.mqtt_client",
  "message": "mqtt_message_received",
  "topic": "telemetry/device1/data",
  "payload_size": 45
}
```

**Kafka Message Enqueued:**
```json
{
  "timestamp": "2026-03-16T10:30:00.130Z",
  "level": "INFO",
  "logger": "app.main",
  "message": "kafka_message_enqueued",
  "topic": "telemetry.raw",
  "key": "device1",
  "idempotency_key": "telemetry/device1/data:device1:a1b2c3d4"
}
```

**Kafka Publish Failed:**
```json
{
  "timestamp": "2026-03-16T10:30:00.140Z",
  "level": "ERROR",
  "logger": "app.kafka_producer",
  "message": "kafka_publish_failed",
  "topic": "telemetry.raw",
  "error": "KafkaError: Connection refused",
  "attempts": 3
}
```

## Metrics

Prometheus metrics exposed at `/metrics`:

### Counters

- `mqtt_messages_received_total` - Total MQTT messages received (per topic)
- `mqtt_messages_failed_total` - Failed MQTT messages (per topic, reason)
- `kafka_messages_published_total` - Messages successfully published to Kafka
- `kafka_publish_errors_total` - Kafka publish failures (per topic, error_type)

### Histograms

- `mqtt_message_processing_duration_seconds` - Processing time from MQTT to Kafka publish

### Examples

```bash
# Get all metrics
curl http://localhost:9103/metrics

# Filter by metric name
curl http://localhost:9103/metrics | grep mqtt_

# Monitor in real-time (requires watch command)
watch -n 1 'curl -s http://localhost:9103/metrics | grep kafka_messages'
```

## Development

### Project Structure

```
mqtt-ingest/
├── app/
│   ├── main.py                 # FastAPI app, lifespan, MQTT client startup
│   ├── config.py               # Settings and environment variables
│   ├── mqtt_client.py          # MQTT connection and message callbacks
│   ├── kafka_producer.py       # Kafka publishing wrapper
│   ├── message_handler.py      # Payload validation and normalization
│   ├── metrics.py              # Prometheus metric definitions
│   └── health.py               # Health check endpoints
├── tests/
│   ├── unit/                   # Unit tests
│   ├── integration/            # Integration tests
│   └── conftest.py             # Pytest fixtures
├── pyproject.toml              # Project metadata and dependencies
├── README.md                   # This file
└── ARCHITECTURE.md             # Detailed architecture and design decisions
```

### Running Tests

```bash
# All tests with coverage
pytest

# Specific test file
pytest tests/unit/test_message_handler.py

# With verbose output
pytest -v

# Stop on first failure
pytest -x

# Coverage report (HTML)
open htmlcov/index.html
```

### Code Quality

```bash
# Format code
black app tests

# Lint
flake8 app tests

# Type checking
mypy app
```

## Deployment

### Docker Compose

Example `docker-compose.yml`:

```yaml
services:
  mqtt-ingest:
    build: .
    ports:
      - "8000:8000"      # HTTP
      - "9103:9103"      # Prometheus metrics
    environment:
      MQTT_BROKER_HOST: mosquitto
      KAFKA_BROKERS: kafka:9092
      LOG_LEVEL: INFO
      ENVIRONMENT: production
    depends_on:
      - mosquitto
      - kafka
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 5s
```

### Kubernetes

Example resources:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: mqtt-ingest-config
data:
  MQTT_BROKER_HOST: mosquitto
  KAFKA_BROKERS: kafka-0.kafka-headless:9092
  LOG_LEVEL: INFO

---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mqtt-ingest
spec:
  replicas: 2
  template:
    spec:
      containers:
      - name: mqtt-ingest
        image: mqtt-ingest:latest
        ports:
        - name: http
          containerPort: 8000
        - name: metrics
          containerPort: 9103
        envFrom:
        - configMapRef:
            name: mqtt-ingest-config
        livenessProbe:
          httpGet:
            path: /health
            port: http
          initialDelaySeconds: 10
          periodSeconds: 30
        readinessProbe:
          httpGet:
            path: /ready
            port: http
          initialDelaySeconds: 5
          periodSeconds: 10
```

## Troubleshooting

### MQTT Connection Issues

```bash
# Check MQTT broker is reachable
mosquitto_pub -h mosquitto -t "test" -m "hello"

# Monitor MQTT traffic
docker logs mqtt-ingest | grep mqtt
```

### Kafka Connection Issues

```bash
# Check Kafka broker is reachable
kafka-broker-api-versions.sh --bootstrap-server kafka:9092

# Monitor Kafka topics
kafka-console-consumer.sh --bootstrap-server kafka:9092 \
  --topic telemetry.raw --from-beginning
```

### High Message Processing Time

Check metrics:
```bash
curl http://localhost:9103/metrics | grep processing_duration
```

Possible causes:
- Slow MQTT broker (network latency)
- Slow Kafka broker (publish latency)
- Large message payloads
- Overloaded system

## Dependencies

- **FastAPI** - HTTP framework
- **pydantic** - Configuration and validation
- **paho-mqtt** - MQTT client library
- **prometheus-client** - Metrics collection
- **iot-logging** - Structured logging (git submodule)
- **kafka-consumer-producer-lib** - Kafka client (git dependency)

## License

MIT

## Support

For issues and questions:
- Check logs: `docker logs mqtt-ingest`
- Review metrics: `http://localhost:9103/metrics`
- Check health: `curl http://localhost:8000/health`
- Open an issue on GitHub
