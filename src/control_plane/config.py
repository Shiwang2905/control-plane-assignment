"""Runtime configuration, read from CP_* environment variables."""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="CP_")

    database_url: str = "postgresql://control_plane@localhost:5432/control_plane"
    db_pool_min: int = 2
    db_pool_max: int = 10

    kafka_bootstrap: str = "localhost:19092"
    tasks_topic: str = "tenant.tasks"
    updates_topic: str = "tenant.task-updates"
    dlq_topic: str = "tenant.task-updates.dlq"
    consumer_group: str = "control-plane"

    # Outbox relay
    outbox_poll_interval_s: float = 0.2
    outbox_batch_size: int = 100

    # Retry/backoff for transient failures (DB / broker)
    backoff_initial_s: float = 0.2
    backoff_max_s: float = 10.0

    # Inside a container we must listen on all interfaces; exposure is controlled by compose.
    http_host: str = "0.0.0.0"  # noqa: S104  # nosec B104
    http_port: int = 8000
    log_level: str = "INFO"

    # Toggles so tests can run the API without Kafka.
    enable_relay: bool = True
    enable_consumer: bool = True
