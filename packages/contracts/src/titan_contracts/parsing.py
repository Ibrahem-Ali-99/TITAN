"""Parse canonical commerce JSON without depending on Kafka or the database."""

import json
from collections.abc import Mapping
from typing import Any
from uuid import UUID

from pydantic import ValidationError

from titan_contracts.events import _MODELS, CommerceEvent


class CommerceParseError(Exception):
    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        self.detail = detail
        super().__init__(f"{code}: {detail}")


def parse_commerce_bytes(raw: bytes) -> CommerceEvent:
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise CommerceParseError("malformed_encoding", "record value is not valid UTF-8") from exc
    try:
        body = json.loads(text)
    except json.JSONDecodeError as exc:
        raise CommerceParseError("malformed_json", str(exc)[:2000]) from exc
    if not isinstance(body, dict):
        raise CommerceParseError("schema_validation", "event must be a JSON object")
    return parse_commerce_object(body)


def parse_commerce_object(body: Mapping[str, Any]) -> CommerceEvent:
    version = body.get("schema_version")
    if version != 1:
        raise CommerceParseError(
            "unsupported_schema_version",
            f"schema_version {version!r} is not supported",
        )
    event_type = body.get("event_type")
    model = _MODELS.get(event_type) if isinstance(event_type, str) else None
    if model is None:
        raise CommerceParseError(
            "unsupported_event_type",
            f"event_type {event_type!r} is not supported",
        )
    try:
        return model.model_validate(body)
    except ValidationError as exc:
        raise CommerceParseError("schema_validation", exc.json()[:8000]) from exc


def peek_event_id(value: object) -> UUID | None:
    if not isinstance(value, str):
        return None
    try:
        return UUID(value)
    except ValueError:
        return None
