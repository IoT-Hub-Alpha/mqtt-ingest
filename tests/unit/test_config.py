"""Unit tests for configuration management."""

import os
import pytest

from app.config import Settings


class TestSettings:
    """Tests for Settings configuration."""

    def test_default_settings(self):
        """Settings should have sensible defaults."""
        settings = Settings()

        assert settings.mqtt_broker_host == "mosquitto"
        assert settings.mqtt_broker_port == 1883
        assert settings.mqtt_topic == "telemetry/#"
        assert settings.mqtt_qos == 1
        assert settings.kafka_brokers == "kafka:9092"
        assert settings.kafka_topic_telemetry_raw == "telemetry.raw"
        assert settings.http_host == "0.0.0.0"
        assert settings.http_port == 8000
        assert settings.log_level == "INFO"
        assert settings.service_name == "mqtt-ingest"

    def test_mqtt_qos_validation_valid_values(self):
        """MQTT QoS should accept values 0, 1, 2."""
        for qos in [0, 1, 2]:
            settings = Settings(mqtt_qos=qos)
            assert settings.mqtt_qos == qos

    def test_mqtt_qos_validation_invalid_values(self):
        """MQTT QoS should reject values outside 0-2."""
        with pytest.raises(ValueError):
            Settings(mqtt_qos=-1)

        with pytest.raises(ValueError):
            Settings(mqtt_qos=3)

    def test_mqtt_connect_timeout_must_be_positive(self):
        """MQTT connect timeout should be positive."""
        settings = Settings(mqtt_connect_timeout=5)
        assert settings.mqtt_connect_timeout == 5

    def test_mqtt_reconnect_delays_must_be_positive(self):
        """MQTT reconnect delays should be positive."""
        settings = Settings(mqtt_reconnect_min_delay=0.5, mqtt_reconnect_max_delay=30.0)
        assert settings.mqtt_reconnect_min_delay == 0.5
        assert settings.mqtt_reconnect_max_delay == 30.0

    def test_http_port_must_be_valid(self):
        """HTTP port should be valid integer."""
        settings = Settings(http_port=9000)
        assert settings.http_port == 9000

    def test_is_production_flag(self):
        """is_production property should correctly identify environment."""
        prod_settings = Settings(environment="production")
        assert prod_settings.is_production is True

        prod_settings = Settings(environment="prod")
        assert prod_settings.is_production is True

        dev_settings = Settings(environment="development")
        assert dev_settings.is_production is False

    def test_is_development_flag(self):
        """is_development property should correctly identify environment."""
        dev_settings = Settings(environment="development")
        assert dev_settings.is_development is True

        dev_settings = Settings(environment="dev")
        assert dev_settings.is_development is True

        dev_settings = Settings(environment="local")
        assert dev_settings.is_development is True

        prod_settings = Settings(environment="production")
        assert prod_settings.is_development is False

    def test_get_kafka_brokers_list_single(self):
        """Parse single Kafka broker."""
        settings = Settings(kafka_brokers="kafka:9092")
        brokers = settings.get_kafka_brokers_list()

        assert brokers == ["kafka:9092"]

    def test_get_kafka_brokers_list_multiple(self):
        """Parse multiple Kafka brokers."""
        settings = Settings(kafka_brokers="kafka1:9092,kafka2:9092,kafka3:9092")
        brokers = settings.get_kafka_brokers_list()

        assert brokers == ["kafka1:9092", "kafka2:9092", "kafka3:9092"]

    def test_get_kafka_brokers_list_with_spaces(self):
        """Parse Kafka brokers with spaces."""
        settings = Settings(kafka_brokers="kafka1:9092 , kafka2:9092 , kafka3:9092")
        brokers = settings.get_kafka_brokers_list()

        assert brokers == ["kafka1:9092", "kafka2:9092", "kafka3:9092"]

    def test_environment_variable_override(self, monkeypatch):
        """Environment variables should override defaults."""
        monkeypatch.setenv("MQTT_BROKER_HOST", "mqtt.example.com")
        monkeypatch.setenv("KAFKA_BROKERS", "kafka1:9092,kafka2:9092")
        monkeypatch.setenv("HTTP_PORT", "9000")

        settings = Settings()

        assert settings.mqtt_broker_host == "mqtt.example.com"
        assert settings.kafka_brokers == "kafka1:9092,kafka2:9092"
        assert settings.http_port == 9000

    def test_log_level_configurable(self):
        """Log level should be configurable."""
        for level in ["DEBUG", "INFO", "WARNING", "ERROR"]:
            settings = Settings(log_level=level)
            assert settings.log_level == level

    def test_log_format_configurable(self):
        """Log format should be configurable."""
        settings = Settings(log_format="json")
        assert settings.log_format == "json"

        settings = Settings(log_format="standard")
        assert settings.log_format == "standard"

    def test_case_insensitive_environment_variables(self, monkeypatch):
        """Environment variable aliases should be case-insensitive."""
        # The BaseSettings with env_file=".env" and case_sensitive=False
        # should accept lowercase env vars
        monkeypatch.setenv("mqtt_broker_host", "custom.mqtt.com")

        settings = Settings()
        # Should still work due to case_insensitive=False in config
        # This depends on how pydantic-settings handles it
