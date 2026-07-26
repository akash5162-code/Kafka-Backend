"""
app/schemas/event.py
────────────────────
Pydantic schemas that define the shape of all incoming request bodies.
These act as strong contracts — FastAPI validates them automatically.
"""
import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


# ── Enum: Event Types ──────────────────────────────────────────────────────────
class EventType(str, Enum):
    USER_ACTION = "user_action"
    ORDER = "order"
    TRANSACTION = "transaction"
    SYSTEM_LOG = "system_log"


class OrderStatus(str, Enum):
    PENDING = "pending"
    CONFIRMED = "confirmed"
    SHIPPED = "shipped"
    DELIVERED = "delivered"
    CANCELLED = "cancelled"


class LogLevel(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


# ── Base Event Envelope ────────────────────────────────────────────────────────
class BaseEvent(BaseModel):
    """Envelope that wraps all events. Provides traceability."""
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    source_ip: str | None = None         # injected by the API layer
    user_agent: str | None = None        # injected by the API layer


# ── User Action Event ──────────────────────────────────────────────────────────
class UserActionPayload(BaseModel):
    user_id: str = Field(..., examples=["usr_abc123"])
    session_id: str = Field(..., examples=["sess_xyz789"])
    action: str = Field(..., examples=["page_view", "button_click", "search"])
    resource: str = Field(..., examples=["/dashboard", "/products/42"])
    metadata: dict[str, Any] = Field(default_factory=dict)


class UserActionEvent(BaseEvent):
    event_type: EventType = EventType.USER_ACTION
    payload: UserActionPayload


# ── Order Event ───────────────────────────────────────────────────────────────
class OrderItem(BaseModel):
    sku: str
    name: str
    quantity: int = Field(..., gt=0)
    unit_price: float = Field(..., gt=0)


class OrderPayload(BaseModel):
    order_id: str = Field(default_factory=lambda: f"ORD-{uuid.uuid4().hex[:8].upper()}")
    user_id: str
    items: list[OrderItem]
    status: OrderStatus = OrderStatus.PENDING
    shipping_address: str
    coupon_code: str | None = None


class OrderEvent(BaseEvent):
    event_type: EventType = EventType.ORDER
    payload: OrderPayload


# ── Transaction Event ─────────────────────────────────────────────────────────
class TransactionPayload(BaseModel):
    transaction_id: str = Field(default_factory=lambda: f"TXN-{uuid.uuid4().hex[:10].upper()}")
    order_id: str
    user_id: str
    amount: float = Field(..., gt=0)
    currency: str = Field(default="INR", max_length=3)
    payment_method: str = Field(..., examples=["UPI", "credit_card", "net_banking"])
    is_success: bool = True


class TransactionEvent(BaseEvent):
    event_type: EventType = EventType.TRANSACTION
    payload: TransactionPayload


# ── System Log Event ──────────────────────────────────────────────────────────
class SystemLogPayload(BaseModel):
    service: str = Field(..., examples=["api", "consumer", "scheduler"])
    level: LogLevel = LogLevel.INFO
    message: str
    extra: dict[str, Any] = Field(default_factory=dict)


class SystemLogEvent(BaseEvent):
    event_type: EventType = EventType.SYSTEM_LOG
    payload: SystemLogPayload


# ── API Response Schemas ───────────────────────────────────────────────────────
class EventPublishedResponse(BaseModel):
    status: str = "accepted"
    event_id: str
    topic: str
    message: str = "Event successfully published to Kafka"


class HealthResponse(BaseModel):
    status: str
    kafka_connected: bool
    app_name: str
    environment: str
