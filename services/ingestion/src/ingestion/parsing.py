"""Validate Kafka record bytes into a canonical order event."""

from pydantic import ValidationError

from ingestion.errors import InvalidEvent
from titan_contracts.events import OrderCreatedEvent

RAW_VALUE_LIMIT = 16_384
DETAIL_LIMIT = 8_000


def clip(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[:limit] + "...truncated"


def parse_order_created(raw: bytes) -> OrderCreatedEvent:
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise InvalidEvent("malformed_encoding", "record value is not valid UTF-8") from exc
    try:
        return OrderCreatedEvent.model_validate_json(text)
    except ValidationError as exc:
        error_type = "schema_validation"
        errors = exc.errors()
        if errors and errors[0].get("type") == "json_invalid":
            error_type = "malformed_json"
        raise InvalidEvent(error_type, clip(exc.json(), DETAIL_LIMIT)) from exc
