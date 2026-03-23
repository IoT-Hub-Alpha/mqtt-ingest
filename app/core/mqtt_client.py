"""MQTT client wrapper with reconnection logic."""

import logging
import random
from typing import Callable, Optional

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion

from app.config import Settings
from app.services import metrics

logger = logging.getLogger(__name__)


class MQTTClient:
    """MQTT client wrapper with exponential backoff reconnection."""

    def __init__(
        self,
        config: Settings,
        on_message_callback: Callable[[str, bytes], None],
        on_connect_callback: Optional[Callable[[], None]] = None,
        on_disconnect_callback: Optional[Callable[[], None]] = None,
    ):
        """
        Initialize MQTT client.

        Args:
            config: Application settings
            on_message_callback: Callback for received messages (topic, payload)
            on_connect_callback: Optional callback on successful connect
            on_disconnect_callback: Optional callback on disconnect
        """
        self.config = config
        self.on_message_callback = on_message_callback
        self.on_connect_callback = on_connect_callback
        self.on_disconnect_callback = on_disconnect_callback

        self.client = mqtt.Client(CallbackAPIVersion.VERSION2)
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_disconnect = self._on_disconnect

        self._reconnect_count = 0
        self._is_connected = False

    def _get_backoff_delay(self) -> float:
        """
        Calculate exponential backoff with jitter.

        Strategy: delay = min(2^n, max_delay) + random_jitter
        """
        base_delay = min(
            2**self._reconnect_count,
            self.config.mqtt_reconnect_max_delay,
        )
        jitter = random.uniform(-base_delay * 0.1, base_delay * 0.1)
        return max(self.config.mqtt_reconnect_min_delay, base_delay + jitter)

    def _on_connect(
        self, client: mqtt.Client, userdata: dict, flags: dict, rc: int, properties
    ) -> None:
        """Handle MQTT connection."""
        if rc == 0:
            logger.info(
                "mqtt_connected",
                extra={
                    "broker": (
                        f"{self.config.mqtt_broker_host}:"
                        f"{self.config.mqtt_broker_port}"
                    ),
                    "topic": self.config.mqtt_topic,
                },
            )
            self._reconnect_count = 0
            self._is_connected = True
            metrics.mqtt_connection_status.set(1)

            # Subscribe to telemetry topics
            client.subscribe(self.config.mqtt_topic, qos=self.config.mqtt_qos)

            # Subscribe to device status topics
            client.subscribe("devices/+/status", qos=self.config.mqtt_qos)

            if self.on_connect_callback:
                self.on_connect_callback()
        else:
            logger.error(
                "mqtt_connect_failed",
                extra={"reason_code": rc},
            )
            self._is_connected = False
            metrics.mqtt_connection_status.set(0)

    def _on_message(
        self, client: mqtt.Client, userdata: dict, msg: mqtt.MQTTMessage
    ) -> None:
        """Handle received MQTT message."""
        try:
            logger.debug(
                "mqtt_message_received",
                extra={
                    "topic": msg.topic,
                    "payload_size": len(msg.payload),
                },
            )
            metrics.mqtt_messages_received.labels(topic=msg.topic).inc()

            self.on_message_callback(msg.topic, msg.payload)
            metrics.mqtt_messages_processed.labels(topic=msg.topic).inc()

        except Exception as exc:
            logger.error(
                "mqtt_message_processing_error",
                extra={
                    "topic": msg.topic,
                    "error": str(exc),
                },
            )
            metrics.mqtt_messages_failed.labels(
                topic=msg.topic,
                reason="processing_error",
            ).inc()

    def _on_disconnect(
        self, client: mqtt.Client, userdata: dict, flags: dict, rc: int, properties
    ) -> None:
        """Handle MQTT disconnection."""
        self._is_connected = False
        metrics.mqtt_connection_status.set(0)

        if rc != 0:
            logger.warning(
                "mqtt_unexpected_disconnect",
                extra={
                    "reason_code": rc,
                    "reconnect_attempt": self._reconnect_count + 1,
                },
            )
        else:
            logger.info("mqtt_disconnected")

        if self.on_disconnect_callback:
            self.on_disconnect_callback()

    def connect(self) -> None:
        """Connect to MQTT broker with timeout."""
        try:
            logger.info(
                "mqtt_connecting",
                extra={
                    "host": self.config.mqtt_broker_host,
                    "port": self.config.mqtt_broker_port,
                },
            )

            self.client.connect(
                self.config.mqtt_broker_host,
                self.config.mqtt_broker_port,
                keepalive=60,
            )
        except Exception as exc:
            logger.error(
                "mqtt_connect_error",
                extra={"error": str(exc)},
            )
            raise

    def disconnect(self) -> None:
        """Gracefully disconnect from MQTT broker."""
        try:
            logger.info("mqtt_disconnecting")
            self.client.disconnect()
        except Exception as exc:
            logger.warning(
                "mqtt_disconnect_error",
                extra={"error": str(exc)},
            )

    def loop_forever(self) -> None:
        """Run MQTT client event loop (blocking)."""
        self.client.loop_forever()

    def loop_stop(self) -> None:
        """Stop MQTT client event loop."""
        self.client.loop_stop()

    @property
    def is_connected(self) -> bool:
        """Check if connected to MQTT broker."""
        return self._is_connected
