"""Configuration management for MQTT ingestion microservice."""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings from environment variables."""

    # MQTT Broker
    mqtt_broker_host: str = Field(default="mosquitto", alias="MQTT_BROKER_HOST")
    mqtt_broker_port: int = Field(default=1883, alias="MQTT_BROKER_PORT")
    mqtt_topic: str = Field(default="telemetry/#", alias="MQTT_TOPIC")
    mqtt_qos: int = Field(default=1, alias="MQTT_QOS", ge=0, le=2)
    mqtt_connect_timeout: int = Field(default=10, alias="MQTT_CONNECT_TIMEOUT")
    mqtt_reconnect_min_delay: float = Field(
        default=1.0, alias="MQTT_RECONNECT_MIN_DELAY"
    )
    mqtt_reconnect_max_delay: float = Field(
        default=60.0, alias="MQTT_RECONNECT_MAX_DELAY"
    )

    # Kafka
    kafka_brokers: str = Field(default="kafka:9092", alias="KAFKA_BROKERS")
    kafka_topic_telemetry_raw: str = Field(
        default="telemetry.raw", alias="KAFKA_TOPIC_TELEMETRY_RAW"
    )

    # HTTP Server
    http_host: str = Field(default="0.0.0.0", alias="HTTP_HOST")
    http_port: int = Field(default=8000, alias="HTTP_PORT")
    metrics_port: int = Field(default=9103, alias="METRICS_PORT")

    # Logging
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    log_format: str = Field(default="json", alias="LOG_FORMAT")

    # Application
    service_name: str = Field(default="mqtt-ingest", alias="SERVICE_NAME")
    environment: str = Field(default="development", alias="ENVIRONMENT")

    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=False,
        populate_by_name=True,
    )

    @property
    def is_production(self) -> bool:
        """Check if running in production environment."""
        return self.environment.lower() in ("production", "prod")

    @property
    def is_development(self) -> bool:
        """Check if running in development environment."""
        return self.environment.lower() in ("development", "dev", "local")

    def get_kafka_brokers_list(self) -> list[str]:
        """Parse Kafka brokers from comma-separated string."""
        return [broker.strip() for broker in self.kafka_brokers.split(",")]


def get_settings() -> Settings:
    """Get application settings (singleton)."""
    return Settings()
