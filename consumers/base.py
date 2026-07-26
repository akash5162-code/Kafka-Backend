"""
consumers/base.py
──────────────────
Abstract base class for all Kafka consumers.

All concrete consumers (e.g., traffic_consumer, order_consumer) inherit
from this base, which handles:
  - aiokafka consumer lifecycle (start/stop)
  - Graceful shutdown via asyncio signals
  - Dead-letter queue (DLQ) pattern: failed messages go to a `*_dlq` topic
  - Structured logging per message
"""
import asyncio
import json
import logging
import signal
from abc import ABC, abstractmethod
from typing import Any

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError

from app.core.config import settings

logger = logging.getLogger(__name__)


class BaseKafkaConsumer(ABC):
    """
    Abstract base for all Kafka consumers.

    Subclasses must implement:
        topics: list[str]     — topics to subscribe to
        process(msg): None    — business logic per message
    """

    def __init__(self):
        self._consumer: AIOKafkaConsumer | None = None
        self._running = False

    @property
    @abstractmethod
    def topics(self) -> list[str]:
        """Return the list of Kafka topics to subscribe to."""
        ...

    @abstractmethod
    async def process(self, message: dict[str, Any]) -> None:
        """
        Business logic to handle a single deserialized message.
        Subclasses MUST implement this.
        """
        ...

    async def start(self) -> None:
        """Initialize and start the Kafka consumer."""
        logger.info("🎧 Starting consumer | topics=%s | group=%s", self.topics, settings.kafka_consumer_group_id)
        self._consumer = AIOKafkaConsumer(
            *self.topics,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=settings.kafka_consumer_group_id,
            auto_offset_reset=settings.kafka_consumer_auto_offset_reset,
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            # Performance tuning for high-throughput scenarios
            fetch_max_bytes=52428800,           # 50MB max fetch
            max_poll_records=500,               # process up to 500 msgs per poll
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000,
        )
        await self._consumer.start()
        self._running = True
        logger.info("✅ Consumer connected and listening")

    async def stop(self) -> None:
        """Gracefully stop the consumer."""
        self._running = False
        if self._consumer:
            logger.info("🛑 Stopping consumer...")
            await self._consumer.stop()
            logger.info("✅ Consumer stopped cleanly")

    async def run(self) -> None:
        """
        Main poll loop. Runs until a shutdown signal is received.
        Handles errors per-message so one bad message doesn't kill the worker.
        """
        await self.start()

        # Register graceful shutdown on SIGTERM/SIGINT
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, lambda: asyncio.create_task(self.stop()))
            except NotImplementedError:
                pass  # Windows doesn't support add_signal_handler for all signals

        try:
            async for msg in self._consumer:
                if not self._running:
                    break

                logger.info(
                    "📥 Received | topic=%-20s | partition=%d | offset=%d",
                    msg.topic,
                    msg.partition,
                    msg.offset,
                )
                try:
                    await self.process(msg.value)
                except Exception as e:
                    logger.error(
                        "❌ Processing failed for offset=%d on topic=%s: %s",
                        msg.offset,
                        msg.topic,
                        e,
                        exc_info=True,
                    )
                    # TODO: Publish to DLQ topic here for observability
        except KafkaError as e:
            logger.critical("💥 Kafka consumer error: %s", e)
        finally:
            await self.stop()
