"""Create the operational event store and quarantine table.

Revision ID: 0001_operational_store
Revises:
Create Date: 2026-11-22
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001_operational_store"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "commerce_events",
        sa.Column("event_id", sa.Uuid(as_uuid=True), primary_key=True),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("order_id", sa.String(length=64), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ingested_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source", sa.String(length=200), nullable=False),
        sa.Column("event_envelope", postgresql.JSONB(), nullable=False),
        sa.Column("kafka_topic", sa.String(length=255), nullable=False),
        sa.Column("kafka_partition", sa.Integer(), nullable=False),
        sa.Column("kafka_offset", sa.BigInteger(), nullable=False),
        sa.Column("kafka_key", sa.String(length=255), nullable=True),
        sa.UniqueConstraint(
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            name="uq_commerce_events_kafka_position",
        ),
    )
    op.create_index("ix_commerce_events_event_type", "commerce_events", ["event_type"])
    op.create_index("ix_commerce_events_order_id", "commerce_events", ["order_id"])
    op.create_index("ix_commerce_events_ingested_at", "commerce_events", ["ingested_at"])

    op.create_table(
        "ingestion_quarantine",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("kafka_topic", sa.String(length=255), nullable=False),
        sa.Column("kafka_partition", sa.Integer(), nullable=False),
        sa.Column("kafka_offset", sa.BigInteger(), nullable=False),
        sa.Column("kafka_key", sa.Text(), nullable=True),
        sa.Column("raw_value", sa.Text(), nullable=True),
        sa.Column("error_type", sa.String(length=64), nullable=False),
        sa.Column("error_detail", sa.Text(), nullable=False),
        sa.UniqueConstraint(
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            name="uq_quarantine_kafka_position",
        ),
    )


def downgrade() -> None:
    op.drop_table("ingestion_quarantine")
    op.drop_index("ix_commerce_events_ingested_at", table_name="commerce_events")
    op.drop_index("ix_commerce_events_order_id", table_name="commerce_events")
    op.drop_index("ix_commerce_events_event_type", table_name="commerce_events")
    op.drop_table("commerce_events")
