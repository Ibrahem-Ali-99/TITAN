"""Warehouse publication rules. ClickHouse writes are not part of a Postgres transaction."""

from datetime import datetime


def can_promote(
    candidate_silver_finished_at: datetime,
    active_silver_finished_at: datetime | None,
) -> bool:
    """A load may publish when it is the current Silver snapshot or a reload of it.

    An older Silver snapshot must not replace a warehouse publication built from
    a later Silver version.
    """
    if active_silver_finished_at is None:
        return True
    return candidate_silver_finished_at >= active_silver_finished_at


def warehouse_is_aligned(
    postgres_publication_id: str | None,
    clickhouse_publication_id: str | None,
) -> bool:
    return (
        postgres_publication_id is not None
        and clickhouse_publication_id is not None
        and postgres_publication_id == clickhouse_publication_id
    )
