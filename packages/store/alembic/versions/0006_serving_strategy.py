"""Serving strategy on a forecast decision.

Revision ID: 0006_serving_strategy
Revises: 0005_quality_and_ml
Create Date: 2026-11-22
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0006_serving_strategy"
down_revision: str | Sequence[str] | None = "0005_quality_and_ml"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("ml_runs", sa.Column("serving_strategy", sa.String(length=64), nullable=True))
    op.add_column("ml_runs", sa.Column("decision_rationale", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("ml_runs", "decision_rationale")
    op.drop_column("ml_runs", "serving_strategy")
