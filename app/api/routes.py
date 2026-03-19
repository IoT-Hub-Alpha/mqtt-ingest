"""API routes for MQTT Ingestion Service."""

from fastapi import APIRouter, Depends

from app.config import Settings, get_settings

router = APIRouter()


@router.get("/")
async def root(config: Settings = Depends(get_settings)) -> dict:
    """Root endpoint."""
    return {
        "service": config.service_name,
        "version": "0.1.0",
        "status": "running",
    }
