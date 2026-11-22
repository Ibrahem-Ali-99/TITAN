"""Record Spark job reports and Silver publication versions.

Revision ID: 0003_lake_publications
Revises: 0002_ingestion_hub
Create Date: 2026-11-22
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003_lake_publications"
down_revision: str | Sequence[str] | None = "0002_ingestion_hub"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "spark_job_status",
        sa.Column("job_name", sa.String(length=64), primary_key=True),
        sa.Column("state", sa.String(length=32), nullable=False),
        sa.Column("last_batch_id", sa.BigInteger(), nullable=True),
        sa.Column("last_batch_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_offsets", sa.Text(), nullable=True),
        sa.Column("detail", sa.Text(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("error_detail", sa.Text(), nullable=True),
    )
    op.create_table(
        "lake_publications",
        sa.Column("publication_id", sa.String(length=36), primary_key=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("version_uri", sa.Text(), nullable=False),
        sa.Column("result_kind", sa.String(length=64), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("bronze_record_count", sa.Integer(), nullable=True),
        sa.Column("bronze_distinct_offsets", sa.Integer(), nullable=True),
        sa.Column("pending_event_count", sa.Integer(), nullable=True),
        sa.Column("silver_order_count", sa.Integer(), nullable=True),
        sa.Column("silver_payment_count", sa.Integer(), nullable=True),
        sa.Column("silver_inventory_count", sa.Integer(), nullable=True),
        sa.Column("order_metric_rows", sa.Integer(), nullable=True),
        sa.Column("accepted_revenue_json", sa.Text(), nullable=True),
        sa.Column("identifier_conflict_count", sa.Integer(), nullable=True),
        sa.Column("max_occurred_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_detail", sa.Text(), nullable=True),
    )
    op.create_index(
        "ix_lake_publications_status_finished",
        "lake_publications",
        ["status", "finished_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_lake_publications_status_finished", table_name="lake_publications")
    op.drop_table("lake_publications")
    op.drop_table("spark_job_status")
