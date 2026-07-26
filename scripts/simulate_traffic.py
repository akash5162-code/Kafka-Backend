"""
scripts/simulate_traffic.py
─────────────────────────────
Realistic Traffic Simulator using asyncio + aiohttp.

This script mimics real-world traffic patterns by:
  1. Spawning N concurrent "virtual users" simultaneously.
  2. Each user fires requests in a staggered, realistic cadence.
  3. Sends a mix of all 4 event types weighted by real-world probability.
  4. Reports live throughput stats at the end.

Usage:
    python -m scripts.simulate_traffic

Tune the constants at the top to control load intensity.
"""
import asyncio
import json
import logging
import random
import string
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone

import aiohttp

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Tuning Knobs ──────────────────────────────────────────────────────────────
BASE_URL           = "http://localhost:8000/api/v1/events"
VIRTUAL_USERS      = 50          # concurrent users hammering the API
REQUESTS_PER_USER  = 20          # each user sends this many requests
THINK_TIME_MIN     = 0.05        # seconds between requests (min)
THINK_TIME_MAX     = 0.3         # seconds between requests (max)
REQUEST_TIMEOUT    = 10          # seconds before a request is considered failed

# Weighted distribution — mirrors real-world traffic ratios
EVENT_TYPE_WEIGHTS = {
    "user-action": 60,    # 60% of traffic is user behavior tracking
    "order":       20,    # 20% is order placement
    "transaction": 15,    # 15% is payment confirmation
    "log":          5,    # 5% is system log ingestion
}


# ── Stats Tracking ────────────────────────────────────────────────────────────
@dataclass
class Stats:
    success: int = 0
    failed: int = 0
    total_latency: float = 0.0
    errors: list[str] = field(default_factory=list)

    @property
    def total(self) -> int:
        return self.success + self.failed

    @property
    def avg_latency_ms(self) -> float:
        return (self.total_latency / self.success * 1000) if self.success else 0.0


# ── Payload Factories ─────────────────────────────────────────────────────────
ACTIONS   = ["page_view", "button_click", "search", "add_to_cart", "checkout_start"]
RESOURCES = ["/home", "/products", "/cart", "/checkout", "/profile", "/orders"]
SERVICES  = ["api", "scheduler", "consumer", "auth-service"]
METHODS   = ["UPI", "credit_card", "net_banking", "wallet"]
ITEMS     = ["iPhone 16", "MacBook Pro", "AirPods", "Samsung TV", "Sony Headphones"]


def _random_user_id() -> str:
    return f"usr_{''.join(random.choices(string.ascii_lowercase + string.digits, k=8))}"


def make_user_action_payload(user_id: str) -> dict:
    return {
        "event_type": "user_action",
        "payload": {
            "user_id": user_id,
            "session_id": f"sess_{uuid.uuid4().hex[:12]}",
            "action": random.choice(ACTIONS),
            "resource": random.choice(RESOURCES),
            "metadata": {"device": random.choice(["mobile", "desktop", "tablet"])},
        }
    }


def make_order_payload(user_id: str) -> dict:
    num_items = random.randint(1, 5)
    return {
        "event_type": "order",
        "payload": {
            "user_id": user_id,
            "items": [
                {
                    "sku": f"SKU-{random.randint(1000, 9999)}",
                    "name": random.choice(ITEMS),
                    "quantity": random.randint(1, 3),
                    "unit_price": round(random.uniform(100, 150000), 2),
                }
                for _ in range(num_items)
            ],
            "shipping_address": f"{random.randint(1, 999)} MG Road, Bangalore",
            "coupon_code": random.choice([None, "SAVE10", "FLAT50", None]),
        }
    }


def make_transaction_payload(user_id: str, order_id: str) -> dict:
    return {
        "event_type": "transaction",
        "payload": {
            "order_id": order_id,
            "user_id": user_id,
            "amount": round(random.uniform(100, 200000), 2),
            "currency": "INR",
            "payment_method": random.choice(METHODS),
            "is_success": random.random() > 0.05,   # 95% success rate
        }
    }


def make_log_payload() -> dict:
    return {
        "event_type": "system_log",
        "payload": {
            "service": random.choice(SERVICES),
            "level": random.choice(["INFO", "INFO", "INFO", "WARNING", "ERROR"]),
            "message": random.choice([
                "Request processed successfully",
                "Cache miss — fetching from DB",
                "Slow query detected > 500ms",
                "Rate limit threshold reached",
                "Consumer lag increasing",
            ]),
            "extra": {"request_id": str(uuid.uuid4())},
        }
    }


# ── Virtual User ──────────────────────────────────────────────────────────────
async def virtual_user(
    session: aiohttp.ClientSession,
    user_id: str,
    stats: Stats,
    semaphore: asyncio.Semaphore,
) -> None:
    """Simulates a single user sending a series of mixed events."""
    event_types = list(EVENT_TYPE_WEIGHTS.keys())
    weights     = list(EVENT_TYPE_WEIGHTS.values())
    order_id    = f"ORD-{uuid.uuid4().hex[:8].upper()}"

    for _ in range(REQUESTS_PER_USER):
        event_type = random.choices(event_types, weights=weights, k=1)[0]

        match event_type:
            case "user-action":  payload = make_user_action_payload(user_id)
            case "order":        payload = make_order_payload(user_id); order_id = payload["payload"].get("order_id", order_id)
            case "transaction":  payload = make_transaction_payload(user_id, order_id)
            case "log":          payload = make_log_payload()
            case _:              continue

        url = f"{BASE_URL}/{event_type}"
        start = time.perf_counter()

        async with semaphore:
            try:
                async with session.post(
                    url,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
                ) as resp:
                    elapsed = time.perf_counter() - start
                    if resp.status in (200, 202):
                        stats.success += 1
                        stats.total_latency += elapsed
                        logger.debug("✅ %s | %dms", event_type, int(elapsed * 1000))
                    else:
                        stats.failed += 1
                        body = await resp.text()
                        stats.errors.append(f"HTTP {resp.status}: {body[:100]}")
                        logger.warning("⚠️  %s → HTTP %d", event_type, resp.status)
            except Exception as e:
                stats.failed += 1
                stats.errors.append(str(e))
                logger.error("❌ %s → %s", event_type, e)

        # Think time — humans don't fire requests instantly
        await asyncio.sleep(random.uniform(THINK_TIME_MIN, THINK_TIME_MAX))


# ── Main Runner ───────────────────────────────────────────────────────────────
async def main() -> None:
    stats = Stats()
    semaphore = asyncio.Semaphore(VIRTUAL_USERS * 2)   # max concurrent connections

    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    logger.info("  🚦  Kafka Traffic Simulator")
    logger.info("  Target:        %s", BASE_URL)
    logger.info("  Virtual Users: %d", VIRTUAL_USERS)
    logger.info("  Requests/User: %d", REQUESTS_PER_USER)
    logger.info("  Total Requests: ~%d", VIRTUAL_USERS * REQUESTS_PER_USER)
    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    wall_start = time.perf_counter()

    connector = aiohttp.TCPConnector(limit=200, force_close=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        users = [_random_user_id() for _ in range(VIRTUAL_USERS)]
        tasks = [
            virtual_user(session, uid, stats, semaphore)
            for uid in users
        ]
        await asyncio.gather(*tasks)

    elapsed = time.perf_counter() - wall_start
    rps     = stats.success / elapsed if elapsed else 0

    logger.info("")
    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    logger.info("  📊  SIMULATION REPORT")
    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    logger.info("  ✅ Successful: %d", stats.success)
    logger.info("  ❌ Failed:     %d", stats.failed)
    logger.info("  📈 Total:      %d", stats.total)
    logger.info("  ⏱️  Elapsed:    %.2fs", elapsed)
    logger.info("  🚀 Throughput: %.1f req/sec", rps)
    logger.info("  ⚡ Avg Latency: %.1fms", stats.avg_latency_ms)
    if stats.errors:
        logger.info("  ⚠️  Sample Errors (first 5):")
        for err in stats.errors[:5]:
            logger.info("     → %s", err)
    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")


if __name__ == "__main__":
    asyncio.run(main())
