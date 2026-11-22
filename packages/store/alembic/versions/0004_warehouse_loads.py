"""Bronze commit manifest, pipeline attempts, and warehouse loads.

Revision ID: 0004_warehouse_loads
Revises: 0003_lake_publications
Create Date: 2026-11-22
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0004_warehouse_loads"
down_revision: str | Sequence[str] | None = "0003_lake_publications"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "bronze_batch_commits",
        sa.Column("batch_id", sa.BigInteger(), primary_key=True),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("object_prefix", sa.Text(), nullable=False),
        sa.Column("record_count", sa.Integer(), nullable=False),
        sa.Column("committed_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "pipeline_runs",
        sa.Column("id", sa.Integer(), sa.Identity(), primary_key=True),
        sa.Column("job_name", sa.String(length=64), nullable=False),
        sa.Column("state", sa.String(length=32), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("duration_ms", sa.Integer(), nullable=True),
        sa.Column("detail", sa.Text(), nullable=True),
        sa.Column("error_detail", sa.Text(), nullable=True),
        sa.Column("metadata_json", sa.Text(), nullable=True),
    )
    op.create_index("ix_pipeline_runs_job_name", "pipeline_runs", ["job_name"])
    op.create_table(
        "warehouse_loads",
        sa.Column("load_id", sa.String(length=36), primary_key=True),
        sa.Column("silver_publication_id", sa.String(length=36), nullable=False),
        sa.Column("silver_finished_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("warehouse_publication_id", sa.String(length=36), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("order_count", sa.Integer(), nullable=True),
        sa.Column("payment_count", sa.Integer(), nullable=True),
        sa.Column("inventory_count", sa.Integer(), nullable=True),
        sa.Column("item_count", sa.Integer(), nullable=True),
        sa.Column("accepted_revenue_json", sa.Text(), nullable=True),
        sa.Column("error_detail", sa.Text(), nullable=True),
    )
    op.create_index(
        "ix_warehouse_loads_status_finished",
        "warehouse_loads",
        ["status", "finished_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_warehouse_loads_status_finished", table_name="warehouse_loads")
    op.drop_table("warehouse_loads")
    op.drop_index("ix_pipeline_runs_job_name", table_name="pipeline_runs")
    op.drop_table("pipeline_runs")
    op.drop_table("bronze_batch_commits")
