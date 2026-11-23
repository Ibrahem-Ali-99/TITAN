"""Deterministic checks for the disposable Spark-to-serving CI pipeline."""

from __future__ import annotations

import json
import os
import time
from collections.abc import Sequence
from decimal import Decimal
from pathlib import Path

import psycopg

from event_generator.generator import generate_history
from titan_contracts.events import CommerceEvent
from titan_messaging.producer import connect_producer, publish_record

ROOT = Path(__file__).resolve().parents[1]
EVENT_COUNT = 14
SEED = 33031


def _database_url() -> str:
    return os.environ.get(
        "DATABASE_URL", "postgresql://titan:titan_full_ci_password@localhost:35435/titan"
    )


def _brokers() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:39092")


def _wait_for_persistence(event_ids: set[str], timeout: float = 90.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with psycopg.connect(_database_url()) as connection:
            found = {
                str(row[0])
                for row in connection.execute(
                    "select event_id from commerce_events where event_id = any(%s)",
                    (list(event_ids),),
                ).fetchall()
            }
        if found == event_ids:
            return
        time.sleep(1)
    raise AssertionError(f"only {len(found)} of {len(event_ids)} events persisted")


def _expected_revenue(events: Sequence[CommerceEvent]) -> dict[str, str]:
    totals: dict[str, Decimal] = {}
    for event in events:
        if event.event_type != "order_created":
            continue
        currency = event.payload.currency
        totals[currency] = totals.get(currency, Decimal("0")) + event.payload.total_amount
    return {currency: format(value, ".2f") for currency, value in sorted(totals.items())}


def main() -> None:
    project = os.environ.get("COMPOSE_PROJECT_NAME", "")
    if not project or project in {"titan", ""}:
        raise SystemExit("refusing to run outside an isolated Compose project")
    events = generate_history(14, SEED)
    event_ids = {str(event.event_id) for event in events}
    producer = connect_producer(_brokers(), client_id="titan-full-pipeline-ci")
    try:
        for event in events:
            publish_record(producer, event)
        for event in events:
            publish_record(producer, event)
    finally:
        producer.flush(30)
    _wait_for_persistence(event_ids)
    with psycopg.connect(_database_url()) as connection:
        persisted_row = connection.execute(
            "select count(*) from commerce_events where event_id = any(%s)",
            (list(event_ids),),
        ).fetchone()
        if persisted_row is None:
            raise AssertionError("PostgreSQL count query returned no row")
        persisted = int(persisted_row[0])
        dispositions = {
            str(row[0]): int(row[1])
            for row in connection.execute(
                """
                select disposition, count(*)
                from commerce_events
                where event_id = any(%s)
                group by disposition
                """,
                (list(event_ids),),
            ).fetchall()
        }
    if persisted != len(event_ids) or dispositions.get("accepted") != len(event_ids):
        raise AssertionError(f"PostgreSQL reconciliation failed: {dispositions}")
    expected = _expected_revenue(events)
    report = {
        "event_count": len(events),
        "unique_event_ids": len(event_ids),
        "persisted_rows": persisted,
        "dispositions": dispositions,
        "expected_order_revenue_by_currency": expected,
    }
    (ROOT / "benchmarks" / "full_pipeline_ci.json").write_text(
        json.dumps(report, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
