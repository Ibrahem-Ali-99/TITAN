"""Quality findings, ML runs, and prediction history.

Revision ID: 0005_quality_and_ml
Revises: 0004_warehouse_loads
Create Date: 2026-11-22
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0005_quality_and_ml"
down_revision: str | Sequence[str] | None = "0004_warehouse_loads"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "quality_runs",
        sa.Column("run_id", sa.String(length=36), primary_key=True),
        sa.Column("warehouse_publication_id", sa.String(length=36), nullable=False),
        sa.Column("silver_publication_id", sa.String(length=36), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("blocking", sa.Integer(), nullable=False),
        sa.Column("injected", sa.String(length=64), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("failure_count", sa.Integer(), nullable=False),
    )
    op.create_index("ix_quality_runs_finished", "quality_runs", ["finished_at"])
    op.create_table(
        "quality_findings",
        sa.Column("id", sa.Integer(), sa.Identity(), primary_key=True),
        sa.Column("run_id", sa.String(length=36), nullable=False),
        sa.Column("check_id", sa.String(length=80), nullable=False),
        sa.Column("check_type", sa.String(length=64), nullable=False),
        sa.Column("dataset", sa.String(length=64), nullable=False),
        sa.Column("severity", sa.String(length=32), nullable=False),
        sa.Column("blocks_publication", sa.Integer(), nullable=False),
        sa.Column("observed", sa.Text(), nullable=False),
        sa.Column("expected", sa.Text(), nullable=False),
        sa.Column("passed", sa.Integer(), nullable=False),
        sa.Column("business_id", sa.String(length=80), nullable=True),
        sa.Column("detail", sa.Text(), nullable=False),
    )
    op.create_index("ix_quality_findings_run_id", "quality_findings", ["run_id"])
    op.create_table(
        "ml_runs",
        sa.Column("run_id", sa.String(length=36), primary_key=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("warehouse_publication_id", sa.String(length=36), nullable=False),
        sa.Column("algorithm", sa.String(length=80), nullable=False),
        sa.Column("hyperparameters_json", sa.Text(), nullable=False),
        sa.Column("feature_version", sa.String(length=32), nullable=False),
        sa.Column("train_start", sa.String(length=16), nullable=True),
        sa.Column("train_end", sa.String(length=16), nullable=True),
        sa.Column("test_start", sa.String(length=16), nullable=True),
        sa.Column("test_end", sa.String(length=16), nullable=True),
        sa.Column("metrics_json", sa.Text(), nullable=True),
        sa.Column("artifact_uri", sa.Text(), nullable=True),
        sa.Column("trained_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("detail", sa.Text(), nullable=True),
    )
    op.create_table(
        "ml_predictions",
        sa.Column("id", sa.Integer(), sa.Identity(), primary_key=True),
        sa.Column("run_id", sa.String(length=36), nullable=False),
        sa.Column("product_id", sa.String(length=80), nullable=False),
        sa.Column("target_date", sa.String(length=16), nullable=False),
        sa.Column("predicted_quantity", sa.String(length=32), nullable=False),
        sa.Column("actual_quantity", sa.String(length=32), nullable=True),
        sa.Column("kind", sa.String(length=32), nullable=False),
        sa.Column("generated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_ml_predictions_run_id", "ml_predictions", ["run_id"])
    op.create_table(
        "ml_anomalies",
        sa.Column("id", sa.Integer(), sa.Identity(), primary_key=True),
        sa.Column("run_id", sa.String(length=36), nullable=False),
        sa.Column("product_id", sa.String(length=80), nullable=False),
        sa.Column("activity_date", sa.String(length=16), nullable=False),
        sa.Column("observed", sa.String(length=32), nullable=False),
        sa.Column("baseline", sa.String(length=32), nullable=False),
        sa.Column("score", sa.String(length=32), nullable=True),
        sa.Column("flagged", sa.Integer(), nullable=False),
        sa.Column("method", sa.String(length=64), nullable=False),
        sa.Column("detail", sa.Text(), nullable=False),
    )
    op.create_index("ix_ml_anomalies_run_id", "ml_anomalies", ["run_id"])


def downgrade() -> None:
    op.drop_index("ix_ml_anomalies_run_id", table_name="ml_anomalies")
    op.drop_table("ml_anomalies")
    op.drop_index("ix_ml_predictions_run_id", table_name="ml_predictions")
    op.drop_table("ml_predictions")
    op.drop_table("ml_runs")
    op.drop_index("ix_quality_findings_run_id", table_name="quality_findings")
    op.drop_table("quality_findings")
    op.drop_index("ix_quality_runs_finished", table_name="quality_runs")
    op.drop_table("quality_runs")
