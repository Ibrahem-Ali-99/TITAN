"""Idempotent writes. Uniqueness is enforced by the database, not by memory."""

import logging
from datetime import UTC, datetime
from enum import Enum
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import DBAPIError, IntegrityError, InterfaceError, OperationalError
from sqlalchemy.orm import Session, sessionmaker

from ingestion.classify import classify_event
from ingestion.errors import TransientStorageError
from ingestion.parsing import RAW_VALUE_LIMIT, clip
from ingestion.positions import KafkaPosition
from titan_contracts.events import CommerceEvent, business_key, order_reference
from titan_contracts.fingerprint import payload_fingerprint
from titan_store.models import CommerceEvent as CommerceEventRow
from titan_store.models import IngestionObservation, QuarantineRecord

logger = logging.getLogger(__name__)


class PersistOutcome(Enum):
    INSERTED = "inserted"
    DUPLICATE = "duplicate"
    DUPLICATE_OPERATION = "duplicate_operation"
    CONFLICT = "conflict"


_DISPOSITION_OUTCOME = {
    "accepted": PersistOutcome.INSERTED,
    "duplicate_operation": PersistOutcome.DUPLICATE_OPERATION,
    "conflict": PersistOutcome.CONFLICT,
}


class SqlEventStore:
    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._session_factory = session_factory

    def persist(
        self,
        event: CommerceEvent,
        position: KafkaPosition,
        ingested_at: datetime,
    ) -> PersistOutcome:
        session = self._session_factory()
        try:
            existing = session.get(CommerceEventRow, event.event_id)
            if existing is not None:
                self._observe_duplicate(session, event.event_id, position, ingested_at)
                return PersistOutcome.DUPLICATE
            fingerprint = payload_fingerprint(event.payload)
            disposition = classify_event(session, event, fingerprint)
            session.add(
                CommerceEventRow(
                    event_id=event.event_id,
                    event_type=event.event_type,
                    schema_version=event.schema_version,
                    order_id=order_reference(event),
                    business_key=business_key(event),
                    disposition=disposition,
                    payload_fingerprint=fingerprint,
                    occurred_at=event.occurred_at,
                    ingested_at=ingested_at,
                    source=event.source,
                    event_envelope=event.model_dump(mode="json"),
                    kafka_topic=position.topic,
                    kafka_partition=position.partition,
                    kafka_offset=position.offset,
                    kafka_key=position.key,
                )
            )
            session.commit()
            return _DISPOSITION_OUTCOME[disposition]
        except IntegrityError:
            session.rollback()
            return self._classify_conflict(session, event, position, ingested_at)
        except (OperationalError, InterfaceError) as exc:
            session.rollback()
            raise TransientStorageError("database operation failed") from exc
        except DBAPIError as exc:
            session.rollback()
            if exc.connection_invalidated:
                raise TransientStorageError("database connection was invalidated") from exc
            raise
        finally:
            session.close()

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
    ) -> PersistOutcome:
        session = self._session_factory()
        try:
            session.add(
                QuarantineRecord(
                    received_at=received_at or datetime.now(UTC),
                    kafka_topic=position.topic,
                    kafka_partition=position.partition,
                    kafka_offset=position.offset,
                    kafka_key=position.key,
                    raw_value=None if raw_value is None else clip(raw_value, RAW_VALUE_LIMIT),
                    error_type=error_type,
                    error_detail=clip(error_detail, 8_000),
                    source=source,
                    event_id=event_id,
                    event_type=event_type,
                    schema_version=schema_version,
                    replay_status=replay_status,
                )
            )
            session.commit()
            return PersistOutcome.INSERTED
        except IntegrityError:
            session.rollback()
            logger.info(
                "quarantine position already stored",
                extra={
                    "topic": position.topic,
                    "partition": position.partition,
                    "offset": position.offset,
                },
            )
            return PersistOutcome.DUPLICATE
        except (OperationalError, InterfaceError) as exc:
            session.rollback()
            raise TransientStorageError("database operation failed") from exc
        except DBAPIError as exc:
            session.rollback()
            if exc.connection_invalidated:
                raise TransientStorageError("database connection was invalidated") from exc
            raise
        finally:
            session.close()

    def _observe_duplicate(
        self,
        session: Session,
        event_id: UUID,
        position: KafkaPosition,
        observed_at: datetime,
    ) -> None:
        session.add(
            IngestionObservation(
                observed_at=observed_at,
                event_id=event_id,
                outcome="duplicate_delivery",
                kafka_topic=position.topic,
                kafka_partition=position.partition,
                kafka_offset=position.offset,
            )
        )
        try:
            session.commit()
        except IntegrityError:
            session.rollback()
            logger.info(
                "duplicate delivery already recorded",
                extra={"event_id": str(event_id), "offset": position.offset},
            )

    def _classify_conflict(
        self,
        session: Session,
        event: CommerceEvent,
        position: KafkaPosition,
        ingested_at: datetime,
    ) -> PersistOutcome:
        existing = session.get(CommerceEventRow, event.event_id)
        if existing is not None:
            self._observe_duplicate(session, event.event_id, position, ingested_at)
            return PersistOutcome.DUPLICATE
        stored_at_position = session.scalar(
            select(CommerceEventRow.event_id).where(
                CommerceEventRow.kafka_topic == position.topic,
                CommerceEventRow.kafka_partition == position.partition,
                CommerceEventRow.kafka_offset == position.offset,
            )
        )
        if stored_at_position is not None:
            logger.error(
                "kafka position already stored for a different event_id",
                extra={
                    "event_id": str(event.event_id),
                    "stored_event_id": str(stored_at_position),
                    "topic": position.topic,
                    "partition": position.partition,
                    "offset": position.offset,
                },
            )
            return PersistOutcome.DUPLICATE
        raise TransientStorageError("integrity error did not match an existing row")
