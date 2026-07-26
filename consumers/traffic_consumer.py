"""
consumers/traffic_consumer.py
──────────────────────────────
Concrete consumer that handles ALL traffic-simulating event types:
  - user_actions
  - orders
  - transactions
  - system_logs

In a real production system, you would split these into dedicated consumers
per topic (or per consumer group) for independent scaling.
Here, a single worker handles all 4 topics for local development clarity.

Run standalone:
    python -m consumers.traffic_consumer
"""
import asyncio
import logging
from typing import Any

from consumers.base import BaseKafkaConsumer
from app.core.config import settings

logger = logging.getLogger(__name__)


class TrafficConsumer(BaseKafkaConsumer):
    """
    Multi-topic consumer that processes the 4 traffic event types.

    Processing logic per event type:
      - user_action   → Analytics aggregation (e.g., session tracking)
      - order         → Inventory check, notification dispatch
      - transaction   → Ledger update, fraud scoring
      - system_log    → Log aggregation, alerting
    """

    @property
    def topics(self) -> list[str]:
        return settings.all_topics

    async def process(self, message: dict[str, Any]) -> None:
        event_type = message.get("event_type", "unknown")
        event_id   = message.get("event_id", "N/A")
        timestamp  = message.get("timestamp", "N/A")

        logger.info("🔄 Processing | event_type=%-15s | event_id=%s", event_type, event_id)

        match event_type:
            case "user_action":
                await self._handle_user_action(message)
            case "order":
                await self._handle_order(message)
            case "transaction":
                await self._handle_transaction(message)
            case "system_log":
                await self._handle_system_log(message)
            case _:
                logger.warning("⚠️  Unknown event_type: '%s' — skipping", event_type)

    # ── Handlers ────────────────────────────────────────────────────────────────

    async def _handle_user_action(self, event: dict[str, Any]) -> None:
        payload = event.get("payload", {})
        logger.info(
            "👤 USER_ACTION | user=%-15s | action=%-15s | resource=%s",
            payload.get("user_id"),
            payload.get("action"),
            payload.get("resource"),
        )
        # ↓ Real logic here: update Redis session store, push to analytics DB, etc.
        await asyncio.sleep(0)   # Simulates async I/O (e.g., DB write)

    async def _handle_order(self, event: dict[str, Any]) -> None:
        payload = event.get("payload", {})
        items   = payload.get("items", [])
        total   = sum(i.get("unit_price", 0) * i.get("quantity", 0) for i in items)
        logger.info(
            "🛒 ORDER       | order_id=%-20s | user=%-10s | items=%d | total=₹%.2f",
            payload.get("order_id"),
            payload.get("user_id"),
            len(items),
            total,
        )
        # ↓ Real logic here: check inventory, send confirmation email, update DB
        await asyncio.sleep(0)

    async def _handle_transaction(self, event: dict[str, Any]) -> None:
        payload = event.get("payload", {})
        logger.info(
            "💳 TRANSACTION | txn_id=%-22s | amount=%-10.2f | method=%-12s | success=%s",
            payload.get("transaction_id"),
            payload.get("amount", 0.0),
            payload.get("payment_method"),
            payload.get("is_success"),
        )
        # ↓ Real logic here: update ledger, run fraud checks, send receipt
        await asyncio.sleep(0)

    async def _handle_system_log(self, event: dict[str, Any]) -> None:
        payload = event.get("payload", {})
        level   = payload.get("level", "INFO")
        service = payload.get("service", "unknown")
        message = payload.get("message", "")
        logger.info(
            "📋 SYSTEM_LOG  | service=%-12s | level=%-8s | %s",
            service, level, message,
        )
        # ↓ Real logic here: forward to ELK stack, Datadog, PagerDuty
        await asyncio.sleep(0)


# ── Entrypoint ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    logger.info("  🎧  Traffic Consumer Starting...")
    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    consumer = TrafficConsumer()
    asyncio.run(consumer.run())
