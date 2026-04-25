"""
Application configuration loaded from environment variables.
Mirrors the MongoDB and ZITADEL settings from the existing Java services.
"""

from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    """
    Configuration settings for fyntrac-py-model.
    All values can be overridden via environment variables.
    """

    # ── Service ──────────────────────────────────────────────────────────
    SERVICE_PORT: int = 8090
    SERVICE_HOST: str = "0.0.0.0"

    # ── MongoDB ──────────────────────────────────────────────────────────
    # Matches the properties in application-dev.properties from subledger/common
    MONGODB_HOST: str = "127.0.0.1"
    MONGODB_PORT: int = 27017
    MONGODB_USERNAME: str = "root"
    MONGODB_PASSWORD: str = "R3s3rv#313"
    MONGODB_AUTH_DATABASE: str = "admin"
    MONGODB_DEFAULT_DATABASE: str = "master"

    # ── ZITADEL / JWT ────────────────────────────────────────────────────
    # Same env vars consumed by fyntrac-gateway
    ZITADEL_ISSUER_URI: str = ""
    ZITADEL_PROJECT_ID: str = ""

    # ── CORS ─────────────────────────────────────────────────────────────
    CORS_ALLOWED_ORIGINS: str = "http://localhost:3030"

    # ── Pulsar ───────────────────────────────────────────────────────────
    PULSAR_SERVICE_URL: str = "pulsar://pulsar:6650"
    PULSAR_EVENT_HISTORY_QUERY_TOPIC: str = "persistent://public/default/event-history-query"
    PULSAR_EVENT_HISTORY_RESULT_TOPIC: str = "persistent://public/default/event-history-result"
    PULSAR_SUBSCRIPTION_NAME: str = "fyntrac-py-model-sub"

    # ── Memcached ────────────────────────────────────────────────────────
    MEMCACHED_HOST: str = "memcached"
    MEMCACHED_PORT: int = 11211

    # ── Pulsar — Python Model Execution ─────────────────────────────────
    PULSAR_PYTHON_MODEL_EXECUTION_TOPIC: str = "persistent://public/default/fyntrac-python-model-execution"
    PULSAR_PYTHON_MODEL_SUBSCRIPTION_NAME: str = "fyntrac-py-model-execution-sub"

    # ── Pulsar — Downstream Producers (mirror Java model service) ────────
    # Consumed by the Java aggregation service (same topic as Java model uses)
    PULSAR_AGGREGATION_TOPIC: str = "persistent://public/default/fyntrac-aggregate-execution"
    # Consumed by the Java GL service (same topic as Java model uses)
    PULSAR_GL_STAGING_TOPIC: str = "persistent://public/default/fyntrac-book-gl-staging"

    # ── Logging ──────────────────────────────────────────────────────────
    LOG_LEVEL: str = "INFO"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True

    @property
    def mongodb_connection_uri(self) -> str:
        """Build the base MongoDB connection URI (without tenant database)."""
        if self.MONGODB_USERNAME and self.MONGODB_PASSWORD:
            return (
                f"mongodb://{self.MONGODB_USERNAME}:{self.MONGODB_PASSWORD}"
                f"@{self.MONGODB_HOST}:{self.MONGODB_PORT}"
                f"/?authSource={self.MONGODB_AUTH_DATABASE}"
                f"&readPreference=primaryPreferred"
                f"&directConnection=true"
            )
        return (
            f"mongodb://{self.MONGODB_HOST}:{self.MONGODB_PORT}"
            f"/?readPreference=primaryPreferred"
            f"&directConnection=true"
        )


@lru_cache()
def get_settings() -> Settings:
    """Cached singleton for application settings."""
    return Settings()
