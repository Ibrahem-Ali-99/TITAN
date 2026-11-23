"""Bounded sustained order load against an explicitly selected environment."""

from __future__ import annotations

import argparse
import json
import os
import statistics
import threading
import time
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

import psycopg

from event_generator.generator import generate_orders
from event_generator.producer import publish_one
from titan_messaging.admin import ensure_commerce_topics
from titan_messaging.producer import connect_producer

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rates", type=float, nargs="+", default=[10.0, 50.0, 100.0])
    parser.add_argument("--duration-seconds", type=float, default=10.0)
    parser.add_argument("--persistence-timeout-seconds", type=float, default=90.0)
    parser.add_argument(
        "--output",
        default=str(ROOT / "benchmarks" / "sustained-load-2026-11-23.json"),
    )
    parser.add_argument("--seed", type=int, default=20261123)
    parser.add_argument("--api", default=os.environ.get("API_BASE_URL", "http://localhost:8000"))
    parser.add_argument(
        "--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    parser.add_argument(
        "--database",
        default=os.environ.get(
            "DATABASE_URL", "postgresql://titan:titan_dev_password@localhost:5435/titan"
        ),
    )
    args = parser.parse_args()
    report = {
        "measured_at": datetime.now(UTC).isoformat(),
        "api": args.api,
        "bootstrap": args.bootstrap,
        "rates": args.rates,
        "duration_seconds": args.duration_seconds,
        "phases": [],
    }
    for phase_number, rate in enumerate(args.rates):
        phase = _run_phase(args, phase_number, rate)
        report["phases"].append(phase)
        print(json.dumps(phase), flush=True)
    destination = Path(args.output)
    destination.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))


def _run_phase(args: argparse.Namespace, phase_number: int, rate: float) -> dict[str, object]:
    count = max(1, int(round(rate * args.duration_seconds)))
    _wait_for_readiness(args.api)
    events = generate_orders(
        count,
        args.seed + phase_number,
        deterministic_event_ids=True,
        occurred_span=None,
    )
    event_ids = [str(event.event_id) for event in events]
    before_events = _event_dispositions(args.database, event_ids)
    before = _database_counts(args.database)
    before_stats = _get(args.api, "/api/v1/ingestion/stats")
    before_monitoring = _get(args.api, "/api/v1/monitoring/overview")
    lag_samples: list[int] = []
    stop_sampling = threading.Event()
    sampler = threading.Thread(
        target=_sample_lag,
        args=(args.api, stop_sampling, lag_samples),
        daemon=True,
    )
    sampler.start()
    ensure_commerce_topics(args.bootstrap)
    producer = connect_producer(args.bootstrap, client_id=f"titan-sustained-{phase_number}")
    ack_seconds: list[float] = []
    ack_times: dict[str, datetime] = {}
    attempted = 0
    acknowledged = 0
    started = time.perf_counter()
    try:
        for event in events:
            attempted += 1
            one = time.perf_counter()
            publish_one(producer, event)
            acknowledged += 1
            ack_seconds.append(time.perf_counter() - one)
            ack_times[str(event.event_id)] = datetime.now(UTC)
            elapsed = time.perf_counter() - started
            target_elapsed = acknowledged / rate
            if target_elapsed > elapsed:
                time.sleep(target_elapsed - elapsed)
    finally:
        producer.flush(30)
        stop_sampling.set()
        sampler.join(timeout=2)
    elapsed = time.perf_counter() - started
    persistence_started = time.perf_counter()
    after = before
    after_events = before_events
    while time.perf_counter() - persistence_started < args.persistence_timeout_seconds:
        after = _database_counts(args.database)
        after_events = _event_dispositions(args.database, event_ids)
        if len(after_events) == acknowledged:
            break
        time.sleep(0.5)
    persistence_seconds = time.perf_counter() - persistence_started
    after_stats = _get(args.api, "/api/v1/ingestion/stats")
    after_monitoring = _get(args.api, "/api/v1/monitoring/overview")
    tracking = _tracking_summary(event_ids, before_events, after_events, ack_times)
    return {
        "phase": phase_number,
        "target_rate_per_second": rate,
        "duration_seconds": args.duration_seconds,
        "attempted": attempted,
        "acknowledged": acknowledged,
        "producer_elapsed_seconds": round(elapsed, 6),
        "ack_latency_seconds": _percentiles(ack_seconds),
        "persisted_delta": after["commerce_events"] - before["commerce_events"],
        "persistence_wait_seconds": round(persistence_seconds, 6),
        "event_tracking": tracking,
        "dispositions_before": before,
        "dispositions_after": after,
        "api_stats_before": before_stats,
        "api_stats_after": after_stats,
        "monitoring_before": before_monitoring,
        "monitoring_after": after_monitoring,
        "lag_samples": lag_samples,
        "max_sampled_lag": max(lag_samples) if lag_samples else None,
        "lag_recovered_to": (
            after_monitoring.get("ingestion_lag", {}).get("total_lag")
            if isinstance(after_monitoring, dict)
            else None
        ),
    }


def _sample_lag(api: str, stop: threading.Event, samples: list[int]) -> None:
    while not stop.is_set():
        body = _get(api, "/api/v1/monitoring/overview")
        if isinstance(body, dict):
            lag = body.get("ingestion_lag")
            if isinstance(lag, dict) and isinstance(lag.get("total_lag"), int):
                samples.append(int(lag["total_lag"]))
        stop.wait(0.5)


def _wait_for_readiness(api: str, timeout_seconds: float = 60.0) -> None:
    """Require API/schema and ingestion monitoring readiness before producing."""
    deadline = time.monotonic() + timeout_seconds
    last = "no readiness response"
    while time.monotonic() < deadline:
        health = _get(api, "/health/ready")
        monitoring = _get(api, "/api/v1/monitoring/overview")
        lag = monitoring.get("ingestion_lag") if isinstance(monitoring, dict) else None
        if (
            isinstance(health, dict)
            and health.get("status") == "ready"
            and isinstance(lag, dict)
            and lag.get("available") is True
        ):
            return
        last = f"health={health!r} monitoring={monitoring!r}"
        time.sleep(0.5)
    raise RuntimeError(f"isolated ingestion was not ready within {timeout_seconds}s: {last}")


def _event_dispositions(database: str, event_ids: list[str]) -> dict[str, dict[str, object]]:
    if not event_ids:
        return {}
    database = database.replace("postgresql+psycopg://", "postgresql://", 1)
    with psycopg.connect(database) as connection:
        rows = connection.execute(
            """
            select event_id::text, disposition, ingested_at
            from commerce_events
            where event_id = any(%s::uuid[])
            """,
            (event_ids,),
        ).fetchall()
    return {str(row[0]): {"disposition": str(row[1]), "ingested_at": row[2]} for row in rows}


def _disposition_summary(events: dict[str, dict[str, object]]) -> dict[str, int]:
    summary: dict[str, int] = {}
    for value in events.values():
        disposition = str(value["disposition"])
        summary[disposition] = summary.get(disposition, 0) + 1
    return summary


def _tracking_summary(
    event_ids: list[str],
    before: dict[str, dict[str, object]],
    after: dict[str, dict[str, object]],
    ack_times: dict[str, datetime],
) -> dict[str, object]:
    """Report event fate without assuming each acknowledgement adds a row."""
    newly_persisted = [event_id for event_id in event_ids if event_id not in before]
    accounted = [event_id for event_id in event_ids if event_id in after]
    persistence_latencies = [
        (ingested_at - ack_times[event_id]).total_seconds()
        for event_id in newly_persisted
        for ingested_at in [after[event_id].get("ingested_at")]
        if event_id in ack_times and isinstance(ingested_at, datetime)
    ]
    return {
        "event_count": len(event_ids),
        "preexisting_event_ids": len(before),
        "newly_persisted_event_ids": len(newly_persisted),
        "accounted_event_ids": len(accounted),
        "unaccounted_event_ids": sorted(set(event_ids) - set(accounted)),
        "dispositions": _disposition_summary(after),
        "ack_to_persistence_seconds": _percentiles(persistence_latencies),
    }


def _database_counts(database: str) -> dict[str, int]:
    database = database.replace("postgresql+psycopg://", "postgresql://", 1)
    with psycopg.connect(database) as connection:
        row = connection.execute(
            """
            select
              (select count(*) from commerce_events),
              (select count(*) from ingestion_quarantine),
              (select count(*) from commerce_events where disposition='accepted'),
              (select count(*) from commerce_events where disposition='conflict'),
              (select count(*) from commerce_events where disposition='duplicate_operation')
            """
        ).fetchone()
    assert row is not None
    return {
        "commerce_events": int(row[0]),
        "quarantine": int(row[1]),
        "accepted": int(row[2]),
        "conflict": int(row[3]),
        "duplicate_operation": int(row[4]),
    }


def _get(api: str, path: str) -> object:
    try:
        with urllib.request.urlopen(api + path, timeout=5) as response:
            return json.loads(response.read().decode())
    except Exception:
        return {}


def _percentiles(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {"count": 0, "p50": None, "p95": None, "p99": None}
    ordered = sorted(values)
    return {
        "count": len(ordered),
        "p50": _percentile(ordered, 49),
        "p95": _percentile(ordered, 94),
        "p99": _percentile(ordered, 98),
    }


def _percentile(values: list[float], index: int) -> float:
    if len(values) == 1:
        return round(values[0], 6)
    return round(statistics.quantiles(values, n=100, method="inclusive")[index], 6)


if __name__ == "__main__":
    main()
