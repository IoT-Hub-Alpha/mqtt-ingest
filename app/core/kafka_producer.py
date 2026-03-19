"""Kafka producer wrapper for telemetry ingestion.

Thin wrapper around IoT-Hub-Alpha kafka-consumer-producer-lib that adds metrics tracking.
Library handles: type conversion (dict→bytes), retries, topic validation.
Wrapper adds: Prometheus metrics on delivery confirmation.
"""

import logging
import os
import sys
from contextlib import contextmanager
from typing import Any, Optional

from app.config import Settings
from app.services import metrics

logger = logging.getLogger(__name__)


@contextmanager
def _suppress_stderr():
    """Context manager to suppress stderr output (for librdkafka bootstrap errors)."""
    save_stderr = sys.stderr
    sys.stderr = open(os.devnull, "w")
    try:
        yield
    finally:
        sys.stderr.close()
        sys.stderr = save_stderr


class KafkaProducer:
    """Metrics-tracking wrapper for IoTKafkaProducer from kafka-consumer-producer-lib."""

    def __init__(self, config: Settings):
        """
        Initialize Kafka producer.

        Library auto-configured from env vars (KAFKA_*).

        Args:
            config: Application settings

        Raises:
            ImportError: If kafka-consumer-producer-lib not installed
        """
        self.config = config

        try:
            from IoTKafka import IoTKafkaProducer  # type: ignore

            # Suppress librdkafka bootstrap stderr messages
            with _suppress_stderr():
                self.producer = IoTKafkaProducer()

            logger.info(
                "kafka_producer_initialized",
                extra={
                    "brokers": self.config.kafka_brokers,
                    "topic_telemetry_raw": self.config.kafka_topic_telemetry_raw,
                },
            )
        except ImportError as exc:
            logger.error(
                "kafka_producer_not_available",
                extra={
                    "error": str(exc),
                    "install": "pip install git+https://github.com/IoT-Hub-Alpha/kafka-consumer-producer-lib.git@dev",
                },
            )
            raise

    def _delivery_callback(self, err: Optional[Exception], msg: Any) -> None:
        """Handle async delivery confirmation - track metrics only."""
        if err:
            logger.error(
                "kafka_delivery_failed",
                extra={
                    "error": str(err),
                    "topic": msg.topic() if msg else "unknown",
                },
            )
            metrics.kafka_publish_errors.labels(
                topic=msg.topic() if msg else "unknown",
                error_type=type(err).__name__,
            ).inc()
        else:
            logger.debug(
                "kafka_message_delivered",
                extra={
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                },
            )
            metrics.kafka_messages_published.labels(topic=msg.topic()).inc()

    def publish_raw(
        self,
        data: dict[str, Any],
        *,
        topic: Optional[str] = None,
        key: Optional[str] = None,
    ) -> None:
        """
        Publish raw telemetry event to Kafka.

        Library handles: type conversion (dict→JSON→bytes), retries, topic validation.

        Args:
            data: Event payload (dict, list, str, or bytes)
            topic: Target topic (defaults to telemetry.raw)
            key: Partition key for routing (serial_number recommended)

        Raises:
            IoTProducerException: If publish fails after retries
        """
        from IoTKafka import IoTProducerException  # type: ignore

        target_topic = topic or self.config.kafka_topic_telemetry_raw

        try:
            # Library handles: type conversion, retries, validation
            # We only add metrics callback
            self.producer.produce(
                topic=target_topic,
                key=key,
                value=data,
                on_delivery=self._delivery_callback,
                attempts=3,
            )

            logger.debug(
                "kafka_message_enqueued",
                extra={"topic": target_topic, "key": key},
            )

        except IoTProducerException as exc:
            logger.error(
                "kafka_publish_failed",
                extra={
                    "topic": target_topic,
                    "error": str(exc),
                    "attempts": 3,
                },
            )
            metrics.kafka_publish_errors.labels(
                topic=target_topic,
                error_type="IoTProducerException",
            ).inc()
            raise

    def close(self, timeout_seconds: float = 10.0) -> None:
        """
        Close producer gracefully.

        Flushes pending messages to Kafka.

        Args:
            timeout_seconds: Max wait time for delivery
        """
        try:
            logger.info(
                "kafka_producer_closing",
                extra={"timeout_seconds": timeout_seconds},
            )
            undelivered = self.producer.flush(timeout=timeout_seconds)

            if undelivered:
                logger.warning(
                    "kafka_undelivered_messages",
                    extra={"count": undelivered},
                )
            else:
                logger.info("kafka_producer_closed")

        except Exception as exc:
            logger.warning(
                "kafka_close_error",
                extra={"error": str(exc)},
            )
