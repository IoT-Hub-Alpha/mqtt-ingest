"""MQTT Ingestion Microservice - Main FastAPI Application."""

import asyncio
import logging
import signal
import sys
import threading
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import Depends, FastAPI, Request
from prometheus_client import make_asgi_app, start_http_server

from . import message_handler, metrics
from .config import Settings, get_settings
from .health import router as health_router
from .kafka_producer import KafkaProducer
from .mqtt_client import MQTTClient

try:
    from iot_logging import (
        FastAPIRequestContextMiddleware,
        StructuredJsonFormatter,
        context,
    )
except ImportError:
    FastAPIRequestContextMiddleware = None  # type: ignore
    StructuredJsonFormatter = None  # type: ignore
    context = None  # type: ignore

logger = logging.getLogger(__name__)

# Suppress verbose Kafka client logs
logging.getLogger('confluent_kafka').setLevel(logging.WARNING)

# Global instances
mqtt_client: Optional[MQTTClient] = None
kafka_producer: Optional[KafkaProducer] = None
mqtt_thread: Optional[threading.Thread] = None


def setup_logging(config: Settings) -> None:
    """Configure JSON logging using logging-lib."""
    root_logger = logging.getLogger()
    root_logger.setLevel(config.log_level)

    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    console_handler = logging.StreamHandler(sys.stdout)

    # Use StructuredJsonFormatter from logging-lib if available
    if StructuredJsonFormatter:
        formatter = StructuredJsonFormatter()
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)
        logger.info("logging_setup_complete", extra={"format": "structured_json"})
    else:
        # Fallback: use python-json-logger if available
        try:
            from pythonjsonlogger import jsonlogger  # type: ignore

            if config.log_format == "json":
                formatter = jsonlogger.JsonFormatter(
                    fmt="%(asctime)s %(levelname)s %(name)s %(message)s"
                )
            else:
                formatter = logging.Formatter(
                    fmt="%(asctime)s %(levelname)s %(name)s: %(message)s"
                )

            console_handler.setFormatter(formatter)
            root_logger.addHandler(console_handler)

        except ImportError:
            logging.basicConfig(
                level=config.log_level,
                format="%(asctime)s %(levelname)s %(name)s: %(message)s",
            )
            logger.warning("logging_lib not available", extra={"fallback": "standard"})


def on_mqtt_message(topic: str, payload: bytes) -> None:
    """Handle MQTT message and publish to Kafka."""
    import time
    from . import metrics

    if not kafka_producer:
        logger.error("kafka_producer_not_initialized")
        return

    start_time = time.time()

    try:
        # Validate and parse payload
        data, error = message_handler.validate_mqtt_payload(payload)
        if error:
            logger.warning(
                "mqtt_payload_validation_failed",
                extra={"topic": topic, "error": error},
            )
            metrics.mqtt_messages_failed.labels(
                topic=topic,
                reason=error.split(":")[0],
            ).inc()
            return

        # Handle device status messages separately
        if topic.startswith("devices/") and topic.endswith("/status"):
            message_handler.handle_device_status(topic, payload)
            return

        # Extract serial number
        serial_number = message_handler.extract_serial_number(topic, data)
        if not serial_number:
            logger.warning(
                "mqtt_missing_serial_number",
                extra={"topic": topic},
            )
            metrics.mqtt_messages_failed.labels(
                topic=topic,
                reason="missing_serial_number",
            ).inc()
            return

        # Add serial_number to data if not present
        if "serial_number" not in data:
            data["serial_number"] = serial_number

        # Build idempotency key
        idempotency_key = message_handler.build_idempotency_key(
            topic=topic,
            serial_number=serial_number,
            payload=data,
            payload_bytes=payload,
        )

        # Build canonical event envelope
        event = message_handler.build_raw_event(
            data,
            serial_number=serial_number,
            request_id=None,
            idempotency_key=idempotency_key,
            ingest_index=0,
        )

        # Publish to Kafka
        kafka_producer.publish_raw(event, key=serial_number)

        # Log published event (INFO level for visibility)
        logger.info(
            "kafka_message_enqueued",
            extra={
                "topic": kafka_producer.config.kafka_topic_telemetry_raw,
                "key": serial_number,
                "idempotency_key": idempotency_key,
            },
        )

        # Record metrics
        elapsed = time.time() - start_time
        metrics.processing_duration.observe(elapsed)

    except Exception as exc:
        logger.error(
            "mqtt_message_processing_error",
            extra={
                "topic": topic,
                "error": str(exc),
            },
        )
        metrics.mqtt_messages_failed.labels(
            topic=topic,
            reason="processing_error",
        ).inc()


def run_mqtt_client() -> None:
    """Run MQTT client in background thread."""
    try:
        if not mqtt_client:
            logger.error("mqtt_client_not_initialized")
            return

        logger.info("mqtt_client_starting")
        mqtt_client.connect()
        mqtt_client.loop_forever()

    except Exception as exc:
        logger.error(
            "mqtt_client_error",
            extra={"error": str(exc)},
        )
        sys.exit(1)


def signal_handler(signum, frame):
    """Handle shutdown signals (SIGINT, SIGTERM)."""
    logger.info(
        "shutdown_signal_received",
        extra={"signal": signum},
    )

    # Stop MQTT loop
    if mqtt_client:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()

    # Close Kafka producer
    if kafka_producer:
        kafka_producer.close()

    logger.info("shutdown_complete")
    sys.exit(0)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handle FastAPI lifespan (startup/shutdown)."""
    global mqtt_client, kafka_producer, mqtt_thread

    config = get_settings()

    # Startup
    logger.info("application_starting")

    try:
        # Initialize Kafka producer
        kafka_producer = KafkaProducer(config)

        # Initialize MQTT client
        mqtt_client = MQTTClient(
            config=config,
            on_message_callback=on_mqtt_message,
        )

        # Start MQTT client in background thread
        mqtt_thread = threading.Thread(target=run_mqtt_client, daemon=True)
        mqtt_thread.start()

        # Start Prometheus metrics server
        try:
            start_http_server(config.metrics_port)
            logger.info(
                "metrics_server_started",
                extra={"port": config.metrics_port},
            )
        except OSError as exc:
            logger.warning(
                "metrics_server_start_error",
                extra={"port": config.metrics_port, "error": str(exc)},
            )

        logger.info("application_started")

    except Exception as exc:
        logger.error(
            "application_startup_error",
            extra={"error": str(exc)},
        )
        raise

    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    yield

    # Shutdown
    logger.info("application_shutting_down")
    if mqtt_client:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
    if kafka_producer:
        kafka_producer.close()
    logger.info("application_shutdown_complete")


def create_app() -> FastAPI:
    """Create and configure FastAPI application."""
    config = get_settings()

    # Setup logging
    setup_logging(config)

    # Create FastAPI app
    app = FastAPI(
        title="MQTT Ingestion Service",
        description="Independent MQTT to Kafka ingestion microservice",
        version="0.1.0",
        lifespan=lifespan,
    )

    # Add request context middleware from logging-lib
    if FastAPIRequestContextMiddleware:
        app.add_middleware(FastAPIRequestContextMiddleware)

    # Include health check routes
    app.include_router(health_router)

    # Mount Prometheus metrics
    metrics_app = make_asgi_app()
    app.mount("/metrics", metrics_app)

    # Dependency injection for MQTT client
    def get_mqtt_client() -> MQTTClient:
        if not mqtt_client:
            raise RuntimeError("MQTT client not initialized")
        return mqtt_client

    app.dependency_overrides[MQTTClient] = get_mqtt_client

    @app.get("/")
    async def root() -> dict:
        """Root endpoint."""
        return {
            "service": config.service_name,
            "version": "0.1.0",
            "status": "running",
        }

    return app


# Create app instance
app = create_app()


def main() -> None:
    """Main entry point for CLI."""
    import uvicorn

    config = get_settings()
    setup_logging(config)

    logger.info(
        "starting_server",
        extra={
            "host": config.http_host,
            "port": config.http_port,
        },
    )

    uvicorn.run(
        "app.main:app",
        host=config.http_host,
        port=config.http_port,
        log_level=config.log_level.lower(),
        access_log=False,
    )


if __name__ == "__main__":
    main()
