"""Unit tests for health check endpoints."""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import Mock, patch

from app.main import create_app


@pytest.fixture
def app():
    """Create FastAPI app for testing."""
    return create_app()


@pytest.fixture
def client(app):
    """Create test client."""
    return TestClient(app)


@pytest.fixture
def mock_mqtt_client():
    """Create mock MQTT client."""
    client = Mock()
    client.is_connected = True
    return client


class TestHealthEndpoint:
    """Tests for /health endpoint."""

    def test_health_check_when_connected(self, client, mock_mqtt_client):
        """Health check should return 200 when MQTT is connected."""
        with patch("app.main.mqtt_client", mock_mqtt_client):
            response = client.get("/health")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["service"] == "mqtt-ingest"
        assert data["mqtt_connected"] is True

    def test_health_check_when_disconnected(self, client, mock_mqtt_client):
        """Health check should return 503 when MQTT is disconnected."""
        mock_mqtt_client.is_connected = False

        with patch("app.main.mqtt_client", mock_mqtt_client):
            response = client.get("/health")

        assert response.status_code == 503
        assert "MQTT broker not connected" in response.json()["detail"]


class TestReadinessEndpoint:
    """Tests for /ready endpoint."""

    def test_readiness_when_connected(self, client, mock_mqtt_client):
        """Readiness probe should return 200 when MQTT is connected."""
        with patch("app.main.mqtt_client", mock_mqtt_client):
            response = client.get("/ready")

        assert response.status_code == 200
        data = response.json()
        assert data["ready"] is True
        assert data["mqtt_connected"] is True

    def test_readiness_when_disconnected(self, client, mock_mqtt_client):
        """Readiness probe should return 503 when MQTT is disconnected."""
        mock_mqtt_client.is_connected = False

        with patch("app.main.mqtt_client", mock_mqtt_client):
            response = client.get("/ready")

        assert response.status_code == 503
        assert "not ready" in response.json()["detail"].lower()


class TestLivenessEndpoint:
    """Tests for /live endpoint."""

    def test_liveness_always_returns_200(self, client):
        """Liveness probe should always return 200 (process is running)."""
        response = client.get("/live")

        assert response.status_code == 200
        data = response.json()
        assert data["alive"] is True

    def test_liveness_independent_of_mqtt(self, client, mock_mqtt_client):
        """Liveness should return 200 even if MQTT is disconnected."""
        mock_mqtt_client.is_connected = False

        with patch("app.main.mqtt_client", mock_mqtt_client):
            response = client.get("/live")

        assert response.status_code == 200
        assert response.json()["alive"] is True


class TestRootEndpoint:
    """Tests for / endpoint."""

    def test_root_endpoint_returns_service_info(self, client):
        """Root endpoint should return service information."""
        response = client.get("/")

        assert response.status_code == 200
        data = response.json()
        assert "service" in data
        assert data["service"] == "mqtt-ingest"
        assert "version" in data
        assert "status" in data
        assert data["status"] == "running"


class TestMetricsEndpoint:
    """Tests for /metrics endpoint."""

    def test_metrics_endpoint_returns_prometheus_format(self, client):
        """Metrics endpoint should return Prometheus text format."""
        response = client.get("/metrics")

        assert response.status_code == 200
        # Prometheus format should contain # HELP or metric names
        assert (
            "# HELP" in response.text
            or "# TYPE" in response.text
            or "mqtt_" in response.text
        )
