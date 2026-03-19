"""Health check endpoints for mqtt-ingest microservice."""

from typing import Any, Annotated

from fastapi import APIRouter, Depends, HTTPException

from app.core import MQTTClient

router = APIRouter(prefix="", tags=["health"])


@router.get("/health")
async def health_check(mqtt_client: Annotated[MQTTClient, Depends()]) -> dict[str, Any]:
    """
    Health check endpoint.

    Returns:
        Health status including MQTT connection status
    """
    mqtt_connected = mqtt_client.is_connected

    if not mqtt_connected:
        raise HTTPException(
            status_code=503,
            detail="MQTT broker not connected",
        )

    return {
        "status": "healthy",
        "service": "mqtt-ingest",
        "mqtt_connected": mqtt_connected,
    }


@router.get("/ready")
async def readiness_check(
    mqtt_client: Annotated[MQTTClient, Depends()],
) -> dict[str, Any]:
    """
    Readiness probe for Kubernetes.

    Service is ready when connected to MQTT broker.

    Returns:
        Readiness status
    """
    if not mqtt_client.is_connected:
        raise HTTPException(
            status_code=503,
            detail="Service not ready - MQTT disconnected",
        )

    return {
        "ready": True,
        "mqtt_connected": True,
    }


@router.get("/live")
async def liveness_check() -> dict[str, Any]:
    """
    Liveness probe for Kubernetes.

    Service is alive if the process is running.

    Returns:
        Liveness status
    """
    return {"alive": True}
