"""API routes package."""

from . import health
from .routes import router

__all__ = ["router", "health"]