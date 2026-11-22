"""Classify a new event_id against business keys already stored.

The first durable event for an order or payment is ``accepted``. A later event
with the same payload fingerprint is a duplicate operation. A later event with
a different payload is a conflict. Inventory updates are state changes: each
new ``event_id`` is accepted. ``event_id`` redelivery is handled before this
function runs and does not reach it.
"""

from sqlalchemy import select
from sqlalchemy.orm import Session

from titan_contracts.events import CommerceEvent, InventoryUpdatedEvent, business_key
from titan_contracts.fingerprint import fingerprint_mapping
from titan_store.models import CommerceEvent as CommerceEventRow


def classify_event(session: Session, event: CommerceEvent, fingerprint: str) -> str:
    if isinstance(event, InventoryUpdatedEvent):
        return "accepted"
    key = business_key(event)
    rows = session.scalars(
        select(CommerceEventRow).where(
            CommerceEventRow.event_type == event.event_type,
            CommerceEventRow.business_key == key,
        )
    ).all()
    if not rows:
        return "accepted"
    for row in rows:
        existing = row.payload_fingerprint
        if existing is None:
            payload = (
                row.event_envelope.get("payload") if isinstance(row.event_envelope, dict) else None
            )
            if isinstance(payload, dict):
                existing = fingerprint_mapping(payload)
        if existing == fingerprint:
            return "duplicate_operation"
    return "conflict"
