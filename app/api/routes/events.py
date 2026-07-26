"""
app/api/routes/events.py
──────────────────────────
Core event-ingestion endpoints.

Design Philosophy:
  - Each endpoint receives an incoming HTTP request (mimicking real user traffic).
  - It immediately serializes the payload and publishes to the appropriate Kafka topic.
  - Returns HTTP 202 Accepted — NOT 200 OK.
    202 is semantically correct: "I received it, but I haven't processed it yet."
  - This makes the API non-blocking and capable of handling massive concurrency.

Partition Keying:
  - All events are published with a stable key (e.g., user_id, order_id).
  - This guarantees ordering: all events for the same entity land on the same partition.
"""
import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, status

from app.api.dependencies import get_kafka_producer
from app.core.config import settings
from app.schemas.event import (
    EventPublishedResponse,
    OrderEvent,
    SystemLogEvent,
    TransactionEvent,
    UserActionEvent,
)
from app.services.kafka_producer import KafkaProducerService

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/events",
    tags=["Events"],
)

ProducerDep = Annotated[KafkaProducerService, Depends(get_kafka_producer)]


# ── Helper ─────────────────────────────────────────────────────────────────────
def _enrich_event(event: UserActionEvent | OrderEvent | TransactionEvent | SystemLogEvent, request: Request):
    """Inject network metadata (IP, User-Agent) into the event envelope."""
    event.source_ip = request.client.host if request.client else "unknown"
    event.user_agent = request.headers.get("user-agent", "unknown")
    return event


# ── 1. User Action Events ──────────────────────────────────────────────────────
@router.post(
    "/user-action",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=EventPublishedResponse,
    summary="Ingest User Action",
    description=(
        "Ingest a user behavioral event (page view, click, search) and publish "
        "it to the `user_actions` Kafka topic. Returns 202 immediately."
    ),
)
async def ingest_user_action(
    event: UserActionEvent,
    request: Request,
    producer: ProducerDep,
) -> EventPublishedResponse:
    event = _enrich_event(event, request)
    topic = settings.kafka_topic_user_actions
    key = event.payload.user_id        # partition by user for ordering

    try:
        await producer.publish(topic=topic, value=event.model_dump(mode="json"), key=key)
    except Exception as e:
        logger.error("Failed to publish user_action event %s: %s", event.event_id, e)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Kafka broker is unavailable. Please retry.",
        )

    return EventPublishedResponse(
        event_id=event.event_id,
        topic=topic,
        message=f"User action '{event.payload.action}' queued for processing",
    )


# ── 2. Order Events ────────────────────────────────────────────────────────────
@router.post(
    "/order",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=EventPublishedResponse,
    summary="Place Order",
    description=(
        "Publish a new order event to the `orders` Kafka topic. "
        "Consumers will process inventory, send notifications, etc."
    ),
)
async def ingest_order(
    event: OrderEvent,
    request: Request,
    producer: ProducerDep,
) -> EventPublishedResponse:
    event = _enrich_event(event, request)
    topic = settings.kafka_topic_orders
    key = event.payload.order_id       # partition by order for ordering

    try:
        await producer.publish(topic=topic, value=event.model_dump(mode="json"), key=key)
    except Exception as e:
        logger.error("Failed to publish order event %s: %s", event.event_id, e)
        raise HTTPException(status_code=503, detail="Kafka unavailable.")

    return EventPublishedResponse(
        event_id=event.event_id,
        topic=topic,
        message=f"Order '{event.payload.order_id}' accepted and queued",
    )


# ── 3. Transaction Events ──────────────────────────────────────────────────────
@router.post(
    "/transaction",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=EventPublishedResponse,
    summary="Record Transaction",
    description=(
        "Publish a payment/transaction event to the `transactions` Kafka topic. "
        "Consumers handle ledger updates, fraud checks, and receipts."
    ),
)
async def ingest_transaction(
    event: TransactionEvent,
    request: Request,
    producer: ProducerDep,
) -> EventPublishedResponse:
    event = _enrich_event(event, request)
    topic = settings.kafka_topic_transactions
    key = event.payload.user_id

    try:
        await producer.publish(topic=topic, value=event.model_dump(mode="json"), key=key)
    except Exception as e:
        logger.error("Failed to publish transaction event %s: %s", event.event_id, e)
        raise HTTPException(status_code=503, detail="Kafka unavailable.")

    return EventPublishedResponse(
        event_id=event.event_id,
        topic=topic,
        message=f"Transaction '{event.payload.transaction_id}' recorded",
    )


# ── 4. System Log Events ───────────────────────────────────────────────────────
@router.post(
    "/log",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=EventPublishedResponse,
    summary="Ingest System Log",
    description=(
        "Publish a structured system log event to the `system_logs` Kafka topic. "
        "Consumers can forward to ELK, Datadog, or any log aggregator."
    ),
)
async def ingest_system_log(
    event: SystemLogEvent,
    request: Request,
    producer: ProducerDep,
) -> EventPublishedResponse:
    event = _enrich_event(event, request)
    topic = settings.kafka_topic_system_logs
    key = event.payload.service

    try:
        await producer.publish(topic=topic, value=event.model_dump(mode="json"), key=key)
    except Exception as e:
        logger.error("Failed to publish log event %s: %s", event.event_id, e)
        raise HTTPException(status_code=503, detail="Kafka unavailable.")

    return EventPublishedResponse(
        event_id=event.event_id,
        topic=topic,
        message=f"Log from '{event.payload.service}' at level {event.payload.level} queued",
    )
