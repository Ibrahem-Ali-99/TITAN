"""Idempotent writes. Uniqueness is enforced by the database, not by memory."""

import logging
from datetime import UTC, datetime
from enum import Enum

from sqlalchemy import select
from sqlalchemy.exc import DBAPIError, IntegrityError, InterfaceError, OperationalError
from sqlalchemy.orm import Session, sessionmaker

from ingestion.errors import TransientStorageError
from ingestion.parsing import RAW_VALUE_LIMIT, clip
from ingestion.positions import KafkaPosition
from titan_contracts.events import OrderCreatedEvent
from titan_store.models import CommerceEvent, QuarantineRecord

logger = logging.getLogger(__name__)


class PersistOutcome(Enum):
    INSERTED = "inserted"
    DUPLICATE = "duplicate"


class SqlEventStore:
    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._session_factory = session_factory

    def persist(
        self,
        event: OrderCreatedEvent,
        position: KafkaPosition,
        ingested_at: datetime,
    ) -> PersistOutcome:
        session = self._session_factory()
        try:
            session.add(
                CommerceEvent(
                    event_id=event.event_id,
                    event_type=event.event_type,
                    schema_version=event.schema_version,
                    order_id=event.payload.order_id,
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
            return PersistOutcome.INSERTED
        except IntegrityError:
            session.rollback()
            return self._classify_conflict(session, event, position)
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

    def _classify_conflict(
        self,
        session: Session,
        event: OrderCreatedEvent,
        position: KafkaPosition,
    ) -> PersistOutcome:
        existing = session.get(CommerceEvent, event.event_id)
        if existing is not None:
            logger.info("duplicate event_id ignored", extra={"event_id": str(event.event_id)})
            return PersistOutcome.DUPLICATE
        stored_at_position = session.scalar(
            select(CommerceEvent.event_id).where(
                CommerceEvent.kafka_topic == position.topic,
                CommerceEvent.kafka_partition == position.partition,
                CommerceEvent.kafka_offset == position.offset,
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
