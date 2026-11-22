"""Operator replay. Publication and persistence are recorded as different states."""

import json
from collections.abc import Callable
from datetime import UTC, datetime
from uuid import UUID, uuid4

from sqlalchemy.orm import Session

from titan_contracts.events import CommerceEvent, business_key
from titan_contracts.parsing import CommerceParseError, parse_commerce_bytes, parse_commerce_object
from titan_messaging.producer import PublishError, PublishResult
from titan_store.models import CommerceEvent as CommerceEventRow
from titan_store.models import QuarantineRecord, ReplayAttempt

MAX_REPLAY_BATCH = 20
Publisher = Callable[[CommerceEvent], PublishResult]


def run_replay(
    session: Session,
    publish: Publisher,
    *,
    quarantine_ids: list[int],
    event_ids: list[UUID],
    replacement: dict[str, object] | None,
) -> list[ReplayAttempt]:
    if replacement is not None:
        if len(quarantine_ids) != 1 or event_ids:
            raise ValueError("a replacement replay targets exactly one quarantine record")
    elif not quarantine_ids and not event_ids:
        raise ValueError("replay requires a quarantine id or an event id")
    if len(quarantine_ids) + len(event_ids) > MAX_REPLAY_BATCH:
        raise ValueError(f"replay batches are limited to {MAX_REPLAY_BATCH} records")

    attempts: list[ReplayAttempt] = []
    if replacement is not None:
        attempts.append(_replace(session, publish, quarantine_ids[0], replacement))
        return attempts
    for quarantine_id in quarantine_ids:
        attempts.append(_replay_quarantine(session, publish, quarantine_id))
    for event_id in event_ids:
        attempts.append(_replay_persisted(session, publish, event_id))
    return attempts


def downstream_state(session: Session, attempt: ReplayAttempt) -> str:
    """Separate a Kafka publication from a completed database write."""
    if attempt.outcome == "refused":
        return "refused"
    if attempt.outcome == "publish_failed":
        return "publish_failed"
    event_id = attempt.published_event_id
    if event_id is not None and session.get(CommerceEventRow, event_id) is not None:
        return "persisted"
    if attempt.outcome == "published":
        return "awaiting_persistence"
    return attempt.outcome


def _replay_quarantine(session: Session, publish: Publisher, quarantine_id: int) -> ReplayAttempt:
    record = session.get(QuarantineRecord, quarantine_id)
    if record is None:
        return _save(
            session,
            quarantine_id=quarantine_id,
            source_event_id=None,
            published_event_id=None,
            mode="exact",
            outcome="refused",
            detail="quarantine record does not exist",
        )
    if not record.raw_value:
        return _save(
            session,
            quarantine_id=quarantine_id,
            source_event_id=record.event_id,
            published_event_id=None,
            mode="exact",
            outcome="refused",
            detail="quarantine record has no raw payload to replay",
        )
    try:
        event = parse_commerce_bytes(record.raw_value.encode("utf-8"))
    except CommerceParseError as exc:
        return _save(
            session,
            quarantine_id=quarantine_id,
            source_event_id=record.event_id,
            published_event_id=None,
            mode="exact",
            outcome="refused",
            detail=f"record cannot be replayed without correction: {exc.code}",
        )
    if record.kafka_key != _key(event):
        return _save(
            session,
            quarantine_id=quarantine_id,
            source_event_id=event.event_id,
            published_event_id=None,
            mode="exact",
            outcome="refused",
            detail="Kafka key does not match the business key; submit a corrected replacement",
        )
    if record.replay_status == "needs_correction":
        return _save(
            session,
            quarantine_id=quarantine_id,
            source_event_id=event.event_id,
            published_event_id=None,
            mode="exact",
            outcome="refused",
            detail="record was quarantined for a key problem and needs a corrected replacement",
        )
    return _publish_exact(
        session,
        publish,
        event,
        quarantine_id=quarantine_id,
        source_event_id=event.event_id,
    )


def _replay_persisted(session: Session, publish: Publisher, event_id: UUID) -> ReplayAttempt:
    row = session.get(CommerceEventRow, event_id)
    if row is None:
        return _save(
            session,
            quarantine_id=None,
            source_event_id=event_id,
            published_event_id=None,
            mode="exact",
            outcome="refused",
            detail="event_id is not in the operational store",
        )
    try:
        event = parse_commerce_bytes(json.dumps(row.event_envelope).encode("utf-8"))
    except CommerceParseError as exc:
        return _save(
            session,
            quarantine_id=None,
            source_event_id=event_id,
            published_event_id=None,
            mode="exact",
            outcome="refused",
            detail=f"stored envelope cannot be republished: {exc.code}",
        )
    return _publish_exact(
        session,
        publish,
        event,
        quarantine_id=None,
        source_event_id=event.event_id,
    )


def _replace(
    session: Session,
    publish: Publisher,
    quarantine_id: int,
    replacement: dict[str, object],
) -> ReplayAttempt:
    record = session.get(QuarantineRecord, quarantine_id)
    if record is None:
        return _save(
            session,
            quarantine_id=quarantine_id,
            source_event_id=None,
            published_event_id=None,
            mode="replacement",
            outcome="refused",
            detail="quarantine record does not exist",
        )
    try:
        event = parse_commerce_object(replacement)
    except CommerceParseError as exc:
        return _save(
            session,
            quarantine_id=quarantine_id,
            source_event_id=record.event_id,
            published_event_id=None,
            mode="replacement",
            outcome="refused",
            detail=f"replacement was rejected: {exc.code}",
        )
    if record.event_id is not None and event.event_id == record.event_id:
        return _save(
            session,
            quarantine_id=quarantine_id,
            source_event_id=record.event_id,
            published_event_id=None,
            mode="replacement",
            outcome="refused",
            detail="a corrected replacement must use a new event_id",
        )
    if session.get(CommerceEventRow, event.event_id) is not None:
        return _save(
            session,
            quarantine_id=quarantine_id,
            source_event_id=record.event_id,
            published_event_id=None,
            mode="replacement",
            outcome="refused",
            detail="replacement event_id is already persisted",
        )
    attempt = _publish_exact(
        session,
        publish,
        event,
        quarantine_id=quarantine_id,
        source_event_id=record.event_id,
        mode="replacement",
    )
    if attempt.outcome == "published":
        record.replay_status = "replaced"
        session.commit()
    return attempt


def _publish_exact(
    session: Session,
    publish: Publisher,
    event: CommerceEvent,
    *,
    quarantine_id: int | None,
    source_event_id: UUID | None,
    mode: str = "exact",
) -> ReplayAttempt:
    started = _save(
        session,
        quarantine_id=quarantine_id,
        source_event_id=source_event_id,
        published_event_id=event.event_id,
        mode=mode,
        outcome="started",
        detail="publication has been requested",
    )
    try:
        result = publish(event)
    except PublishError as exc:
        started.outcome = "publish_failed"
        started.detail = f"broker did not acknowledge the event: {exc}"[:2000]
        session.commit()
        return started
    if not isinstance(result, PublishResult):
        started.outcome = "publish_failed"
        started.detail = "publisher returned an unexpected result"
        session.commit()
        return started
    started.outcome = "published"
    started.detail = (
        "Kafka acknowledged the event. Persistence is not complete until the consumer writes it."
    )
    started.kafka_topic = result.topic
    started.kafka_partition = result.partition
    started.kafka_offset = result.offset
    session.commit()
    return started


def _save(
    session: Session,
    *,
    quarantine_id: int | None,
    source_event_id: UUID | None,
    published_event_id: UUID | None,
    mode: str,
    outcome: str,
    detail: str,
) -> ReplayAttempt:
    attempt = ReplayAttempt(
        id=uuid4(),
        requested_at=datetime.now(UTC),
        quarantine_id=quarantine_id,
        source_event_id=source_event_id,
        published_event_id=published_event_id,
        mode=mode,
        outcome=outcome,
        detail=detail,
    )
    session.add(attempt)
    session.commit()
    return attempt


def _key(event: CommerceEvent) -> str:
    return business_key(event)
