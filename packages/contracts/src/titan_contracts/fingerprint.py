"""Stable payload fingerprint used to separate duplicates from conflicts."""

import hashlib
import json

from pydantic import BaseModel


def payload_fingerprint(payload: BaseModel) -> str:
    """Hash the canonical payload. Timestamps and event_id are not included."""
    return fingerprint_mapping(payload.model_dump(mode="json"))


def fingerprint_mapping(payload: dict[str, object]) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
