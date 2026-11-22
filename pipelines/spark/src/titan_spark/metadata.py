"""Durable job and publication rows. The API reads these instead of scanning MinIO."""

from datetime import datetime, timezone
from typing import Any

from titan_spark.config import Settings
from titan_spark.policy import pipeline_finished_at

SILVER_LOCK_KEY = 740001
WAREHOUSE_LOCK_KEY = 740002


def record_job(
    settings: Settings,
    *,
    job_name: str,
    state: str,
    last_batch_id: int | None = None,
    last_offsets: str | None = None,
    detail: str | None = None,
    error_detail: str | None = None,
) -> None:
    now = datetime.now(timezone.utc)
    with _connect(settings) as connection:
        connection.execute(
            """
            insert into spark_job_status (
                job_name, state, last_batch_id, last_batch_at, last_offsets,
                detail, updated_at, error_detail
            ) values (%s, %s, %s, %s, %s, %s, %s, %s)
            on conflict (job_name) do update set
                state = excluded.state,
                last_batch_id = coalesce(excluded.last_batch_id, spark_job_status.last_batch_id),
                last_batch_at = excluded.last_batch_at,
                last_offsets = coalesce(excluded.last_offsets, spark_job_status.last_offsets),
                detail = excluded.detail,
                updated_at = excluded.updated_at,
                error_detail = excluded.error_detail
            """,
            (
                job_name,
                state,
                last_batch_id,
                now,
                last_offsets,
                detail,
                now,
                error_detail,
            ),
        )
        connection.execute(
            """
            insert into pipeline_runs (
                job_name, state, started_at, finished_at, detail, error_detail
            ) values (%s, %s, %s, %s, %s, %s)
            """,
            (job_name, state, now, pipeline_finished_at(state, now), detail, error_detail),
        )
        connection.commit()


def start_publication(settings: Settings, publication_id: str, version_uri: str) -> None:
    now = datetime.now(timezone.utc)
    with _connect(settings) as connection:
        connection.execute(
            """
            insert into lake_publications (
                publication_id, status, version_uri, started_at, result_kind
            ) values (%s, 'staging', %s, %s, 'canonical_batch')
            """,
            (publication_id, version_uri, now),
        )
        connection.commit()


def finish_publication(
    settings: Settings,
    publication_id: str,
    *,
    status: str,
    error_detail: str | None = None,
    bronze_record_count: int | None = None,
    bronze_distinct_offsets: int | None = None,
    pending_event_count: int | None = None,
    silver_order_count: int | None = None,
    silver_payment_count: int | None = None,
    silver_inventory_count: int | None = None,
    order_metric_rows: int | None = None,
    accepted_revenue_json: str | None = None,
    identifier_conflict_count: int | None = None,
    max_occurred_at: Any = None,
) -> None:
    now = datetime.now(timezone.utc)
    with _connect(settings) as connection:
        connection.execute(
            """
            update lake_publications set
                status = %s,
                finished_at = %s,
                error_detail = %s,
                bronze_record_count = %s,
                bronze_distinct_offsets = %s,
                pending_event_count = %s,
                silver_order_count = %s,
                silver_payment_count = %s,
                silver_inventory_count = %s,
                order_metric_rows = %s,
                accepted_revenue_json = %s,
                identifier_conflict_count = %s,
                max_occurred_at = %s
            where publication_id = %s
            """,
            (
                status,
                now,
                error_detail,
                bronze_record_count,
                bronze_distinct_offsets,
                pending_event_count,
                silver_order_count,
                silver_payment_count,
                silver_inventory_count,
                order_metric_rows,
                accepted_revenue_json,
                identifier_conflict_count,
                max_occurred_at,
                publication_id,
            ),
        )
        connection.commit()


def next_bronze_attempt(settings: Settings, batch_id: int) -> int:
    with _connect(settings) as connection:
        row = connection.execute(
            """
            select coalesce(max(attempt), 0) + 1
            from bronze_batch_commits
            where batch_id = %s
            """,
            (batch_id,),
        ).fetchone()
    return int(row[0]) if row is not None else 1


def commit_bronze_batch(
    settings: Settings,
    batch_id: int,
    attempt: int,
    object_prefix: str,
    record_count: int,
) -> None:
    """Point readers at one finished prefix. A lower attempt cannot replace it."""
    now = datetime.now(timezone.utc)
    with _connect(settings) as connection:
        connection.execute(
            """
            insert into bronze_batch_commits (
                batch_id, attempt, object_prefix, record_count, committed_at
            ) values (%s, %s, %s, %s, %s)
            on conflict (batch_id) do update set
                attempt = excluded.attempt,
                object_prefix = excluded.object_prefix,
                record_count = excluded.record_count,
                committed_at = excluded.committed_at
            where excluded.attempt >= bronze_batch_commits.attempt
            """,
            (batch_id, attempt, object_prefix, record_count, now),
        )
        connection.commit()


def backfill_bronze_batch(
    settings: Settings,
    batch_id: int,
    object_prefix: str,
    record_count: int,
) -> None:
    """Register a Day 3 prefix that already has a Spark _SUCCESS marker."""
    now = datetime.now(timezone.utc)
    with _connect(settings) as connection:
        connection.execute(
            """
            insert into bronze_batch_commits (
                batch_id, attempt, object_prefix, record_count, committed_at
            ) values (%s, 0, %s, %s, %s)
            on conflict (batch_id) do nothing
            """,
            (batch_id, object_prefix, record_count, now),
        )
        connection.commit()


def committed_bronze_prefixes(settings: Settings) -> list[str]:
    with _connect(settings) as connection:
        rows = connection.execute(
            "select object_prefix from bronze_batch_commits order by batch_id"
        ).fetchall()
    return [str(row[0]) for row in rows]


class advisory_lock:
    """Hold a PostgreSQL advisory lock for the lifetime of one connection."""

    def __init__(self, settings: Settings, key: int) -> None:
        self.settings = settings
        self.key = key
        self.connection: Any = None

    def __enter__(self) -> "advisory_lock":
        self.connection = _connect(self.settings)
        self.connection.execute("select pg_advisory_lock(%s)", (self.key,))
        return self

    def __exit__(self, *_exc: object) -> None:
        try:
            if self.connection is not None:
                self.connection.execute("select pg_advisory_unlock(%s)", (self.key,))
        finally:
            if self.connection is not None:
                self.connection.close()


def _connect(settings: Settings):
    import psycopg

    return psycopg.connect(settings.libpq_url())
