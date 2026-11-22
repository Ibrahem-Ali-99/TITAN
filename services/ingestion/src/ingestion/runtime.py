"""Consume, persist, then commit.

A database outage never acknowledges a record that was not stored or
quarantined. Invalid records are quarantined and then acknowledged so one
bad message cannot stall the partition. Restarts and duplicate deliveries
rely on ``event_id`` uniqueness. This is at-least-once delivery with
idempotent persistence, not an end-to-end exactly-once transaction.
"""

import logging
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID

from ingestion.errors import CommitFailed, InvalidEvent, PartitionLost, TransientStorageError
from ingestion.parsing import clip, parse_record, peek_identity
from ingestion.positions import KafkaPosition
from ingestion.store import PersistOutcome
from titan_contracts.events import CommerceEvent, business_key

logger = logging.getLogger(__name__)


class EventStore(Protocol):
    def persist(
        self,
        event: CommerceEvent,
        position: KafkaPosition,
        ingested_at: datetime,
    ) -> PersistOutcome: ...

    def quarantine(
        self,
        position: KafkaPosition,
        raw_value: str | None,
        error_type: str,
        error_detail: str,
        received_at: datetime | None = None,
        *,
        source: str | None = None,
        event_id: UUID | None = None,
        event_type: str | None = None,
        schema_version: int | None = None,
        replay_status: str = "not_replayable",
    ) -> PersistOutcome: ...


class Polled(Protocol):
    def topic(self) -> str: ...

    def partition(self) -> int: ...

    def offset(self) -> int: ...

    def value(self) -> bytes | None: ...

    def key(self) -> bytes | None: ...

    def error(self) -> str | None: ...


class MessageConsumer(Protocol):
    def poll(self, timeout: float) -> Polled | None: ...

    def commit(self, record: Polled) -> None: ...

    def recover(
        self, record: Polled, delay_seconds: float, should_stop: Callable[[], bool]
    ) -> None: ...

    def close(self) -> None: ...


def decode_key(raw: bytes | None) -> str | None:
    if raw is None:
        return None
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return None


def position_from(record: Polled) -> KafkaPosition:
    return KafkaPosition(
        topic=record.topic(),
        partition=record.partition(),
        offset=record.offset(),
        key=decode_key(record.key()),
    )


def apply_record(record: Polled, store: EventStore, ingested_at: datetime) -> str:
    """Persist or quarantine. Raises TransientStorageError and does not commit offsets."""
    position = position_from(record)
    raw = record.value()
    if raw is None:
        store.quarantine(position, None, "empty_value", "Kafka record value is empty", ingested_at)
        return "quarantined"
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        store.quarantine(
            position, None, "malformed_encoding", "record value is not valid UTF-8", ingested_at
        )
        return "quarantined"
    try:
        event = parse_record(raw)
    except InvalidEvent as exc:
        identity = peek_identity(text)
        store.quarantine(
            position,
            text,
            exc.error_type,
            exc.detail,
            ingested_at,
            source=identity.source,
            event_id=identity.event_id,
            event_type=identity.event_type,
            schema_version=identity.schema_version,
            replay_status="not_replayable",
        )
        logger.warning(
            "quarantined invalid record",
            extra={
                "error_type": exc.error_type,
                "offset": position.offset,
                "partition": position.partition,
            },
        )
        return "quarantined"
    key = decode_key(record.key())
    expected = business_key(event)
    if key is None:
        store.quarantine(
            position,
            text,
            "missing_or_invalid_key",
            "commerce events must use the business key as a UTF-8 Kafka key",
            ingested_at,
            source=event.source,
            event_id=event.event_id,
            event_type=event.event_type,
            schema_version=event.schema_version,
            replay_status="needs_correction",
        )
        return "quarantined"
    if key != expected:
        store.quarantine(
            position,
            clip(text, 16_384),
            "key_mismatch",
            f"Kafka key {key} does not equal business key {expected}",
            ingested_at,
            source=event.source,
            event_id=event.event_id,
            event_type=event.event_type,
            schema_version=event.schema_version,
            replay_status="needs_correction",
        )
        return "quarantined"
    outcome = store.persist(event, position, ingested_at)
    logger.info(
        "applied order event",
        extra={
            "outcome": outcome.value,
            "event_id": str(event.event_id),
            "order_id": event.payload.order_id,
            "offset": position.offset,
        },
    )
    return outcome.value


def run_consumer(
    consumer: MessageConsumer,
    store: EventStore,
    *,
    should_stop: Callable[[], bool],
    heartbeat: Callable[[], None],
    max_backoff_seconds: float = 30.0,
    max_idle_polls: int | None = None,
    max_recoveries: int | None = None,
    clock: Callable[[], datetime] | None = None,
) -> None:
    backoff = 1.0
    idle = 0
    recoveries = 0
    now = clock or (lambda: datetime.now(UTC))
    try:
        while not should_stop():
            heartbeat()
            record = consumer.poll(1.0)
            if record is None:
                idle += 1
                if max_idle_polls is not None and idle >= max_idle_polls:
                    return
                continue
            idle = 0
            error = record.error()
            if error is not None:
                logger.warning("kafka poll returned an error", extra={"error": error})
                continue
            while not should_stop():
                heartbeat()
                try:
                    apply_record(record, store, now())
                    try:
                        consumer.commit(record)
                    except PartitionLost:
                        logger.warning("partition ownership lost before offset commit")
                        backoff = 1.0
                        break
                    except Exception as exc:
                        raise CommitFailed("offset commit failed") from exc
                    backoff = 1.0
                    break
                except (TransientStorageError, CommitFailed) as exc:
                    recoveries += 1
                    logger.warning(
                        "record was not acknowledged; it will be retried",
                        extra={"error": str(exc), "backoff_seconds": backoff},
                    )
                    if max_recoveries is not None and recoveries > max_recoveries:
                        raise
                    consumer.recover(record, backoff, should_stop)
                    backoff = min(backoff * 2, max_backoff_seconds)
                    if should_stop():
                        return
    finally:
        consumer.close()
