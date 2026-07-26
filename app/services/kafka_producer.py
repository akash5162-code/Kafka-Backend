"""
app/services/kafka_producer.py
──────────────────────────────
Async Kafka Producer using aiokafka.

Why aiokafka?
  - Fully async — does NOT block FastAPI's event loop.
  - A single producer instance is shared across all requests via app state.
  - Batching (linger_ms) and acks="all" give us both throughput and durability.

Lifecycle:
  - Producer is started in FastAPI lifespan (startup).
  - Producer is stopped in FastAPI lifespan (shutdown).
"""
import json
import logging
from typing import Any

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError, KafkaError

from app.core.config import settings

logger = logging.getLogger(__name__)


class KafkaProducerService:
    """
    Singleton async Kafka producer.

    Usage (injected via FastAPI dependency):
        producer = KafkaProducerService()
        await producer.start()
        await producer.publish("topic", {"key": "value"})
        await producer.stop()
    """

    def __init__(self):
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        """Initialize and start the Kafka producer. Called at app startup."""
        logger.info("🚀 Starting Kafka Producer — connecting to %s", settings.kafka_bootstrap_servers)
        self._producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
            acks=settings.kafka_producer_acks,
            max_batch_size=settings.kafka_producer_max_batch_size,
            linger_ms=settings.kafka_producer_linger_ms,
            # Retry config — handles transient broker failures
            request_timeout_ms=30000,
            retry_backoff_ms=500,
        )
        try:
            await self._producer.start()
            logger.info("✅ Kafka Producer connected successfully")
        except KafkaConnectionError as e:
            logger.error("❌ Failed to connect to Kafka: %s", e)
            raise

    async def stop(self) -> None:
        """Gracefully flush and stop the producer. Called at app shutdown."""
        if self._producer:
            logger.info("🛑 Stopping Kafka Producer — flushing remaining messages...")
            await self._producer.stop()
            logger.info("✅ Kafka Producer stopped cleanly")

    async def publish(
        self,
        topic: str,
        value: dict[str, Any],
        key: str | None = None,
    ) -> dict[str, Any]:
        """
        Publish a single message to a Kafka topic.

        Args:
            topic: The Kafka topic name.
            value: The message payload (will be JSON-serialized).
            key:   Optional partition key. Use a stable user_id or order_id
                   to ensure all events for the same entity go to the same partition.

        Returns:
            Metadata dict with topic, partition, and offset.

        Raises:
            RuntimeError: If the producer is not started.
            KafkaError: On publish failure.
        """
        if not self._producer:
            raise RuntimeError("Kafka producer is not started. Call start() first.")

        try:
            record_metadata = await self._producer.send_and_wait(
                topic=topic,
                value=value,
                key=key,
            )
            logger.debug(
                "📨 Published to %s | partition=%s | offset=%s | key=%s",
                topic,
                record_metadata.partition,
                record_metadata.offset,
                key,
            )
            return {
                "topic": record_metadata.topic,
                "partition": record_metadata.partition,
                "offset": record_metadata.offset,
            }
        except KafkaError as e:
            logger.error("❌ Failed to publish to topic '%s': %s", topic, e)
            raise

    async def publish_batch(
        self,
        topic: str,
        messages: list[dict[str, Any]],
    ) -> int:
        """
        Publish multiple messages to the same topic in a batch.
        More efficient than calling publish() in a loop.

        Returns:
            Number of messages successfully published.
        """
        if not self._producer:
            raise RuntimeError("Kafka producer is not started.")

        published = 0
        async with self._producer.transaction():
            for message in messages:
                await self._producer.send(
                    topic=topic,
                    value=message,
                    key=message.get("event_id", "").encode("utf-8") or None,
                )
                published += 1

        logger.info("📦 Batch published %d messages to '%s'", published, topic)
        return published

    @property
    def is_connected(self) -> bool:
        """Check if the producer is initialized and running."""
        return self._producer is not None


# Module-level singleton shared across the app
kafka_producer = KafkaProducerService()
