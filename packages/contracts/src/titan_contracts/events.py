"""Versioned event envelope. Day 1 accepts only ``order_created`` schema 1."""

from datetime import UTC, datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator

from titan_contracts.identifiers import ORDER_CREATED, SCHEMA_VERSION
from titan_contracts.orders import OrderCreatedPayload


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


class OrderCreatedEvent(BaseModel):
    """Canonical envelope persisted and forwarded without reshaping the payload."""

    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    event_type: Literal["order_created"] = ORDER_CREATED
    schema_version: Literal[1] = SCHEMA_VERSION
    occurred_at: datetime
    produced_at: datetime
    source: str = Field(min_length=1, max_length=200)
    payload: OrderCreatedPayload

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
