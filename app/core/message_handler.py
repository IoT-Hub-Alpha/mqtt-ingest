"""Message transformation and validation for MQTT ingestion."""

import hashlib
import json
import logging
import uuid
from datetime import datetime
from typing import Any, Optional

from app.services import metrics

logger = logging.getLogger(__name__)


def extract_serial_number(topic: str, payload: dict[str, Any]) -> Optional[str]:
    """
    Extract device serial number from MQTT topic or payload.

    Expected topic format: telemetry/<serial_number>
    Falls back to payload['serial_number'] if not in topic.

    Args:
        topic: MQTT topic path
        payload: Parsed JSON payload

    Returns:
        Serial number or None if not found
    """
    # Try to extract from payload first
    serial_from_payload = payload.get("serial_number")
    if isinstance(serial_from_payload, str) and serial_from_payload.strip():
        return serial_from_payload.strip()

    # Extract from topic path (last segment)
    parts = topic.strip("/").split("/")
    if len(parts) >= 2:
        return parts[-1]

    return None


def _normalize_idempotency_value(value: Any) -> Optional[str]:
    """Normalize a value for idempotency key generation."""
    if value is None:
        return None
    if isinstance(value, str):
        normalized = value.strip()
        return normalized if normalized else None
    if isinstance(value, (int, float, bool)):
        return str(value)
    return None


def build_idempotency_key(
    *,
    topic: str,
    serial_number: str,
    payload: dict[str, Any],
    payload_bytes: bytes,
) -> str:
    """
    Generate idempotency key for MQTT message.

    Strategy:
    1. Use explicit idempotency_key if provided in payload
    2. Use stable fields (message_id, seq, timestamp, etc.) if present
    3. Fall back to SHA-256 hash of full payload

    Args:
        topic: MQTT topic
        serial_number: Device serial number
        payload: Parsed JSON payload dict
        payload_bytes: Raw message bytes

    Returns:
        Idempotency key string (prefixed with "mqtt:")
    """
    # Check for explicit idempotency_key in payload
    explicit = _normalize_idempotency_value(payload.get("idempotency_key"))
    if explicit:
        return explicit

    # Check for stable fields that can uniquely identify message
    stable_fields: dict[str, str] = {}
    for field in ("message_id", "msg_id", "seq", "sequence", "timestamp", "ts"):
        normalized = _normalize_idempotency_value(payload.get(field))
        if normalized:
            stable_fields[field] = normalized

    # If we found stable fields, use them to build key
    if stable_fields:
        base = json.dumps(
            {
                "serial_number": serial_number,
                "topic": topic,
                "fields": stable_fields,
            },
            sort_keys=True,
            ensure_ascii=False,
            separators=(",", ":"),
        )
        digest = hashlib.sha256(base.encode("utf-8")).hexdigest()
        return f"mqtt:{digest}"

    # Fall back to hash of full payload
    hasher = hashlib.sha256()
    hasher.update(serial_number.encode("utf-8"))
    hasher.update(b"|")
    hasher.update(topic.encode("utf-8"))
    hasher.update(b"|")
    hasher.update(payload_bytes)
    return f"mqtt:{hasher.hexdigest()}"


def build_raw_event(
    raw_payload: dict[str, Any],
    *,
    source: str = "mqtt",
    serial_number: str,
    request_id: Optional[str] = None,
    idempotency_key: Optional[str] = None,
    ingest_index: int = 0,
    received_at: Optional[str] = None,
) -> dict[str, Any]:
    """
    Wrap raw payload into canonical telemetry.raw envelope.

    Args:
        raw_payload: Device telemetry data
        source: Ingestion protocol (mqtt, http, etc)
        serial_number: Device serial number
        request_id: Correlation ID for tracking
        idempotency_key: Deduplication key
        ingest_index: Position in batch (0-based)
        received_at: ISO timestamp when received

    Returns:
        Canonical event envelope dict
    """
    if not isinstance(ingest_index, int) or ingest_index < 0:
        raise ValueError("ingest_index must be non-negative integer")

    event = {
        "request_id": request_id or str(uuid.uuid4()),
        "ingest_protocol": source,
        "serial_number": serial_number,
        "payload": raw_payload,
        "received_at": received_at or datetime.utcnow().isoformat() + "Z",
        "ingest_index": ingest_index,
    }

    # Add idempotency key if provided
    if isinstance(idempotency_key, str):
        key = idempotency_key.strip()
        if key:
            event["idempotency_key"] = key
    elif idempotency_key is not None:
        raise ValueError("idempotency_key must be str when provided")

    return event


def validate_mqtt_payload(payload: bytes) -> tuple[Optional[dict[str, Any]], Optional[str]]:
    """
    Parse and validate MQTT payload.

    Args:
        payload: Raw message bytes

    Returns:
        Tuple of (parsed_dict, error_message)
        - On success: (dict, None)
        - On failure: (None, error_message)
    """
    try:
        data = json.loads(payload.decode("utf-8"))
    except json.JSONDecodeError as exc:
        return None, f"malformed_json: {str(exc)}"
    except UnicodeDecodeError as exc:
        return None, f"invalid_encoding: {str(exc)}"

    if not isinstance(data, dict):
        return None, f"invalid_type: expected dict, got {type(data).__name__}"

    return data, None


def handle_device_status(topic: str, payload_bytes: bytes) -> None:
    """
    Handle device status messages (online/offline).

    Expected topic format: devices/<serial>/status
    Expected payload: "online" or "offline"

    Args:
        topic: MQTT topic
        payload_bytes: Status message bytes
    """
    parts = topic.strip("/").split("/")
    if len(parts) < 2:
        logger.warning("device_status_invalid_topic", extra={"topic": topic})
        return

    serial_number = parts[-2] if parts[-1] == "status" else parts[-1]

    try:
        status = payload_bytes.decode("utf-8", errors="replace").strip().lower()
    except Exception as exc:
        logger.warning(
            "device_status_decode_error",
            extra={"topic": topic, "error": str(exc)},
        )
        status = "unknown"

    logger.info(
        "device_status_changed",
        extra={
            "serial_number": serial_number,
            "status": status,
            "topic": topic,
        },
    )

    # Record metrics
    if status in ("online", "offline"):
        metrics.device_status_changes.labels(status=status).inc()
