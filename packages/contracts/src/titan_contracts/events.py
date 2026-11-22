"""Versioned commerce envelopes. Schema version 1 remains the only accepted version."""

from datetime import UTC, datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator

from titan_contracts.identifiers import (
    INVENTORY_UPDATED,
    ORDER_CREATED,
    PAYMENT_COMPLETED,
    SCHEMA_VERSION,
)
from titan_contracts.inventory import InventoryUpdatedPayload
from titan_contracts.orders import OrderCreatedPayload
from titan_contracts.payments import PaymentCompletedPayload


def parse_utc_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        text = value.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(text)
    else:
        raise ValueError("timestamp must be an ISO-8601 datetime")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return parsed.astimezone(UTC)


class _Envelope(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    schema_version: Literal[1] = SCHEMA_VERSION
    occurred_at: datetime
    produced_at: datetime
    source: str = Field(min_length=1, max_length=200)

    @field_validator("occurred_at", "produced_at", mode="before")
    @classmethod
    def timestamps_must_be_utc(cls, value: object) -> datetime:
        return parse_utc_datetime(value)

    @field_serializer("occurred_at", "produced_at", when_used="json")
    def serialize_timestamp(self, value: datetime) -> str:
        return value.astimezone(UTC).isoformat()

    @field_serializer("event_id", when_used="json")
    def serialize_event_id(self, value: UUID) -> str:
        return str(value)


class OrderCreatedEvent(_Envelope):
    """Canonical order envelope. Schema 1 is unchanged from Day 1."""

    event_type: Literal["order_created"] = ORDER_CREATED
    payload: OrderCreatedPayload


class PaymentCompletedEvent(_Envelope):
    event_type: Literal["payment_completed"] = PAYMENT_COMPLETED
    payload: PaymentCompletedPayload


class InventoryUpdatedEvent(_Envelope):
    event_type: Literal["inventory_updated"] = INVENTORY_UPDATED
    payload: InventoryUpdatedPayload


CommerceEvent = OrderCreatedEvent | PaymentCompletedEvent | InventoryUpdatedEvent

_MODELS: dict[str, type[CommerceEvent]] = {
    "order_created": OrderCreatedEvent,
    "payment_completed": PaymentCompletedEvent,
    "inventory_updated": InventoryUpdatedEvent,
}


def business_key(event: CommerceEvent) -> str:
    if isinstance(event, OrderCreatedEvent):
        return event.payload.order_id
    if isinstance(event, PaymentCompletedEvent):
        return event.payload.payment_id
    return event.payload.product_id


def order_reference(event: CommerceEvent) -> str | None:
    return event.payload.order_id
