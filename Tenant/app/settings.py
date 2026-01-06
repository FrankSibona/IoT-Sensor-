import os
from dataclasses import dataclass

@dataclass(frozen=True)
class Settings:
    # Web
    WEB_HOST: str = os.getenv("WEB_HOST", "0.0.0.0")
    WEB_PORT: int = int(os.getenv("WEB_PORT", "8000"))

    # DB
    DB_URL: str = os.getenv("DB_URL", "sqlite:///./app.db")

    # MQTT
    MQTT_HOST: str = os.getenv("MQTT_HOST", "localhost")
    MQTT_PORT: int = int(os.getenv("MQTT_PORT", "1883"))
    MQTT_USER: str | None = os.getenv("MQTT_USER") or None
    MQTT_PASS: str | None = os.getenv("MQTT_PASS") or None
    MQTT_TLS: bool = os.getenv("MQTT_TLS", "0") == "1"

    # Topic root
    TOPIC_ROOT: str = os.getenv("TOPIC_ROOT", "plant")

settings = Settings()
