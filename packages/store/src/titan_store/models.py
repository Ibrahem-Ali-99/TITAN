"""Operational tables. Schema changes ship through Alembic, not create_all."""

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import (
    JSON,
    BigInteger,
    Integer,
    String,
    Text,
    UniqueConstraint,
    Uuid,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from titan_store.types import UTCDateTime


class Base(DeclarativeBase):
    pass


def _json_column() -> JSON:
    return JSON().with_variant(JSONB(), "postgresql")


class CommerceEvent(Base):
    """One canonical event envelope, unique by ``event_id``.

    ``disposition`` classifies the business effect. A second ``event_id`` for
    the same order is stored as evidence and is not another accepted order.
    """

    __tablename__ = "commerce_events"
    __table_args__ = (
        UniqueConstraint(
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            name="uq_commerce_events_kafka_position",
        ),
    )

    event_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False)
    order_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    business_key: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    disposition: Mapped[str] = mapped_column(
        String(32), nullable=False, default="accepted", server_default="accepted"
    )
    payload_fingerprint: Mapped[str | None] = mapped_column(String(64), nullable=True)
    occurred_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
    ingested_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False, index=True)
    source: Mapped[str] = mapped_column(String(200), nullable=False)
    event_envelope: Mapped[dict[str, Any]] = mapped_column(_json_column(), nullable=False)
    kafka_topic: Mapped[str] = mapped_column(String(255), nullable=False)
    kafka_partition: Mapped[int] = mapped_column(Integer, nullable=False)
    kafka_offset: Mapped[int] = mapped_column(BigInteger, nullable=False)
    kafka_key: Mapped[str | None] = mapped_column(String(255), nullable=True)


class QuarantineRecord(Base):
    """Invalid Kafka records that were durably recorded and then acknowledged.

    PostgreSQL is the authoritative dead-letter store. There is no second
    Kafka dead-letter topic, so a failure record does not depend on a dual write.
    """

    __tablename__ = "ingestion_quarantine"
    __table_args__ = (
        UniqueConstraint(
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            name="uq_quarantine_kafka_position",
        ),
    )

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True
    )
    received_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
    kafka_topic: Mapped[str] = mapped_column(String(255), nullable=False)
    kafka_partition: Mapped[int] = mapped_column(Integer, nullable=False)
    kafka_offset: Mapped[int] = mapped_column(BigInteger, nullable=False)
    kafka_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_type: Mapped[str] = mapped_column(String(64), nullable=False)
    error_detail: Mapped[str] = mapped_column(Text, nullable=False)
    source: Mapped[str | None] = mapped_column(String(200), nullable=True)
    event_id: Mapped[UUID | None] = mapped_column(Uuid(as_uuid=True), nullable=True)
    event_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    schema_version: Mapped[int | None] = mapped_column(Integer, nullable=True)
    replay_status: Mapped[str] = mapped_column(
        String(32), nullable=False, default="not_replayable", server_default="not_replayable"
    )


class IngestionObservation(Base):
    """Durable note that a Kafka record was a duplicate delivery.

    Uniqueness is the Kafka position, so a crash between this insert and the
    offset commit cannot inflate the count when the record is retried.
    """

    __tablename__ = "ingestion_observations"
    __table_args__ = (
        UniqueConstraint(
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "outcome",
            name="uq_observation_position_outcome",
        ),
    )

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True
    )
    observed_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
    event_id: Mapped[UUID | None] = mapped_column(Uuid(as_uuid=True), nullable=True)
    outcome: Mapped[str] = mapped_column(String(64), nullable=False)
    kafka_topic: Mapped[str] = mapped_column(String(255), nullable=False)
    kafka_partition: Mapped[int] = mapped_column(Integer, nullable=False)
    kafka_offset: Mapped[int] = mapped_column(BigInteger, nullable=False)


class SourceSubmission(Base):
    """One REST or CSV publication attempt. This is not a persisted business event."""

    __tablename__ = "source_submissions"

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    channel: Mapped[str] = mapped_column(String(16), nullable=False)
    received_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    event_id: Mapped[UUID | None] = mapped_column(Uuid(as_uuid=True), nullable=True)
    event_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    source_name: Mapped[str | None] = mapped_column(String(200), nullable=True)
    detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    kafka_topic: Mapped[str | None] = mapped_column(String(255), nullable=True)
    kafka_partition: Mapped[int | None] = mapped_column(Integer, nullable=True)
    kafka_offset: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    import_id: Mapped[UUID | None] = mapped_column(Uuid(as_uuid=True), nullable=True)


class CsvImport(Base):
    __tablename__ = "csv_imports"

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    filename: Mapped[str] = mapped_column(String(255), nullable=False)
    received_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False, index=True)
    completed_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    accepted_rows: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    rejected_rows: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    publish_failed_rows: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    detail: Mapped[str | None] = mapped_column(Text, nullable=True)


class CsvImportRow(Base):
    __tablename__ = "csv_import_rows"

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True
    )
    import_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), nullable=False, index=True)
    row_number: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    event_id: Mapped[UUID | None] = mapped_column(Uuid(as_uuid=True), nullable=True)
    event_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    event_envelope: Mapped[dict[str, Any] | None] = mapped_column(_json_column(), nullable=True)
    error_detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    kafka_topic: Mapped[str | None] = mapped_column(String(255), nullable=True)
    kafka_partition: Mapped[int | None] = mapped_column(Integer, nullable=True)
    kafka_offset: Mapped[int | None] = mapped_column(BigInteger, nullable=True)


class ReplayAttempt(Base):
    """Operator-requested replay. Outcome is the publication result, not persistence."""

    __tablename__ = "replay_attempts"

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    requested_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False, index=True)
    quarantine_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    source_event_id: Mapped[UUID | None] = mapped_column(Uuid(as_uuid=True), nullable=True)
    published_event_id: Mapped[UUID | None] = mapped_column(Uuid(as_uuid=True), nullable=True)
    mode: Mapped[str] = mapped_column(String(32), nullable=False)
    outcome: Mapped[str] = mapped_column(String(32), nullable=False)
    detail: Mapped[str] = mapped_column(Text, nullable=False)
    kafka_topic: Mapped[str | None] = mapped_column(String(255), nullable=True)
    kafka_partition: Mapped[int | None] = mapped_column(Integer, nullable=True)
    kafka_offset: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
