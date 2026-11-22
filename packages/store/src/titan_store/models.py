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
    """One canonical event envelope, unique by ``event_id``."""

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
    order_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    occurred_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
    ingested_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False, index=True)
    source: Mapped[str] = mapped_column(String(200), nullable=False)
    event_envelope: Mapped[dict[str, Any]] = mapped_column(_json_column(), nullable=False)
    kafka_topic: Mapped[str] = mapped_column(String(255), nullable=False)
    kafka_partition: Mapped[int] = mapped_column(Integer, nullable=False)
    kafka_offset: Mapped[int] = mapped_column(BigInteger, nullable=False)
    kafka_key: Mapped[str | None] = mapped_column(String(255), nullable=True)


class QuarantineRecord(Base):
    """Invalid Kafka records that were durably recorded and then acknowledged."""

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
