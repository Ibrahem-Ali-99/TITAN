"""Add ingestion hub tables and event disposition.

Revision ID: 0002_ingestion_hub
Revises: 0001_operational_store
Create Date: 2026-11-22
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0002_ingestion_hub"
down_revision: str | Sequence[str] | None = "0001_operational_store"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "commerce_events",
        sa.Column("business_key", sa.String(length=64), nullable=True),
    )
    op.add_column(
        "commerce_events",
        sa.Column(
            "disposition",
            sa.String(length=32),
            nullable=False,
            server_default="accepted",
        ),
    )
    op.add_column(
        "commerce_events",
        sa.Column("payload_fingerprint", sa.String(length=64), nullable=True),
    )
    op.execute("UPDATE commerce_events SET business_key = order_id WHERE business_key IS NULL")
    op.alter_column(
        "commerce_events", "order_id", existing_type=sa.String(length=64), nullable=True
    )
    op.create_index("ix_commerce_events_business_key", "commerce_events", ["business_key"])
    op.alter_column("commerce_events", "disposition", server_default=None)

    op.add_column("ingestion_quarantine", sa.Column("source", sa.String(length=200), nullable=True))
    op.add_column("ingestion_quarantine", sa.Column("event_id", sa.Uuid(), nullable=True))
    op.add_column(
        "ingestion_quarantine", sa.Column("event_type", sa.String(length=64), nullable=True)
    )
    op.add_column("ingestion_quarantine", sa.Column("schema_version", sa.Integer(), nullable=True))
    op.add_column(
        "ingestion_quarantine",
        sa.Column(
            "replay_status",
            sa.String(length=32),
            nullable=False,
            server_default="not_replayable",
        ),
    )
    op.alter_column("ingestion_quarantine", "replay_status", server_default=None)

    op.create_table(
        "ingestion_observations",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("event_id", sa.Uuid(), nullable=True),
        sa.Column("outcome", sa.String(length=64), nullable=False),
        sa.Column("kafka_topic", sa.String(length=255), nullable=False),
        sa.Column("kafka_partition", sa.Integer(), nullable=False),
        sa.Column("kafka_offset", sa.BigInteger(), nullable=False),
        sa.UniqueConstraint(
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "outcome",
            name="uq_observation_position_outcome",
        ),
    )
    op.create_table(
        "source_submissions",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("channel", sa.String(length=16), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("event_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("event_type", sa.String(length=64), nullable=True),
        sa.Column("source_name", sa.String(length=200), nullable=True),
        sa.Column("detail", sa.Text(), nullable=True),
        sa.Column("kafka_topic", sa.String(length=255), nullable=True),
        sa.Column("kafka_partition", sa.Integer(), nullable=True),
        sa.Column("kafka_offset", sa.BigInteger(), nullable=True),
        sa.Column("import_id", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.create_index("ix_source_submissions_received_at", "source_submissions", ["received_at"])
    op.create_table(
        "csv_imports",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("filename", sa.String(length=255), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("accepted_rows", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("rejected_rows", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("publish_failed_rows", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("detail", sa.Text(), nullable=True),
    )
    op.create_index("ix_csv_imports_received_at", "csv_imports", ["received_at"])
    op.create_table(
        "csv_import_rows",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("import_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("row_number", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("event_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("event_type", sa.String(length=64), nullable=True),
        sa.Column("event_envelope", postgresql.JSONB(), nullable=True),
        sa.Column("error_detail", sa.Text(), nullable=True),
        sa.Column("kafka_topic", sa.String(length=255), nullable=True),
        sa.Column("kafka_partition", sa.Integer(), nullable=True),
        sa.Column("kafka_offset", sa.BigInteger(), nullable=True),
    )
    op.create_index("ix_csv_import_rows_import_id", "csv_import_rows", ["import_id"])
    op.create_table(
        "replay_attempts",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("requested_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("quarantine_id", sa.BigInteger(), nullable=True),
        sa.Column("source_event_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("published_event_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("mode", sa.String(length=32), nullable=False),
        sa.Column("outcome", sa.String(length=32), nullable=False),
        sa.Column("detail", sa.Text(), nullable=False),
        sa.Column("kafka_topic", sa.String(length=255), nullable=True),
        sa.Column("kafka_partition", sa.Integer(), nullable=True),
        sa.Column("kafka_offset", sa.BigInteger(), nullable=True),
    )
    op.create_index("ix_replay_attempts_requested_at", "replay_attempts", ["requested_at"])


def downgrade() -> None:
    op.drop_index("ix_replay_attempts_requested_at", table_name="replay_attempts")
    op.drop_table("replay_attempts")
    op.drop_index("ix_csv_import_rows_import_id", table_name="csv_import_rows")
    op.drop_table("csv_import_rows")
    op.drop_index("ix_csv_imports_received_at", table_name="csv_imports")
    op.drop_table("csv_imports")
    op.drop_index("ix_source_submissions_received_at", table_name="source_submissions")
    op.drop_table("source_submissions")
    op.drop_table("ingestion_observations")
    op.drop_column("ingestion_quarantine", "replay_status")
    op.drop_column("ingestion_quarantine", "schema_version")
    op.drop_column("ingestion_quarantine", "event_type")
    op.drop_column("ingestion_quarantine", "event_id")
    op.drop_column("ingestion_quarantine", "source")
    op.drop_index("ix_commerce_events_business_key", table_name="commerce_events")
    op.alter_column(
        "commerce_events", "order_id", existing_type=sa.String(length=64), nullable=False
    )
    op.drop_column("commerce_events", "payload_fingerprint")
    op.drop_column("commerce_events", "disposition")
    op.drop_column("commerce_events", "business_key")
