"""
app/core/config.py
─────────────────
Central configuration using Pydantic BaseSettings.
All values are read from environment variables or .env file.
"""
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env.example",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # ── App ──────────────────────────────────────────────────
    app_name: str = "Kafka Traffic Backend"
    app_env: str = "development"
    app_host: str = "0.0.0.0"
    app_port: int = 8000
    api_prefix: str = "/api/v1"

    # ── Kafka ─────────────────────────────────────────────────
    kafka_bootstrap_servers: str = "localhost:9092"

    # ── Kafka Topics ──────────────────────────────────────────
    kafka_topic_user_actions: str = "user_actions"
    kafka_topic_orders: str = "orders"
    kafka_topic_transactions: str = "transactions"
    kafka_topic_system_logs: str = "system_logs"

    # ── Consumer ──────────────────────────────────────────────
    kafka_consumer_group_id: str = "traffic-consumer-group"
    kafka_consumer_auto_offset_reset: str = "earliest"

    # ── Producer ──────────────────────────────────────────────
    kafka_producer_acks: str = "all"        # "all" = highest durability
    kafka_producer_max_batch_size: int = 16384
    kafka_producer_linger_ms: int = 10      # wait up to 10ms to batch messages

    @property
    def all_topics(self) -> list[str]:
        return [
            self.kafka_topic_user_actions,
            self.kafka_topic_orders,
            self.kafka_topic_transactions,
            self.kafka_topic_system_logs,
        ]


# Singleton — import this across the app
settings = Settings()
