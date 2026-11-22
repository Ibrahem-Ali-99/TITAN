"""Validate Kafka record bytes into a canonical commerce event."""

import json
from dataclasses import dataclass
from uuid import UUID

from ingestion.errors import InvalidEvent
from titan_contracts.events import CommerceEvent
from titan_contracts.parsing import CommerceParseError, parse_commerce_bytes

RAW_VALUE_LIMIT = 16_384
DETAIL_LIMIT = 8_000


def clip(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[:limit] + "...truncated"


@dataclass(frozen=True)
class PeekedIdentity:
    event_id: UUID | None
    event_type: str | None
    schema_version: int | None
    source: str | None


def peek_identity(text: str) -> PeekedIdentity:
    """Read identity fields without treating the record as valid."""
    try:
        body = json.loads(text)
    except json.JSONDecodeError:
        return PeekedIdentity(None, None, None, None)
    if not isinstance(body, dict):
        return PeekedIdentity(None, None, None, None)
    raw_id = body.get("event_id")
    event_id = None
    if isinstance(raw_id, str):
        try:
            event_id = UUID(raw_id)
        except ValueError:
            event_id = None
    event_type = body.get("event_type")
    version = body.get("schema_version")
    source = body.get("source")
    return PeekedIdentity(
        event_id,
        event_type if isinstance(event_type, str) else None,
        version if isinstance(version, int) and not isinstance(version, bool) else None,
        source if isinstance(source, str) else None,
    )


def parse_record(raw: bytes) -> CommerceEvent:
    try:
        return parse_commerce_bytes(raw)
    except CommerceParseError as exc:
        raise InvalidEvent(exc.code, clip(exc.detail, DETAIL_LIMIT)) from exc
