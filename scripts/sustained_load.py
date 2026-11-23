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

from event_generator.generator import generate_orders  # type: ignore[import-untyped]
from event_generator.producer import publish_one  # type: ignore[import-untyped]
from titan_messaging.admin import ensure_commerce_topics
from titan_messaging.producer import connect_producer

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rates", type=float, nargs="+", default=[10.0, 50.0, 100.0])
    parser.add_argument("--duration-seconds", type=float, default=10.0)
    parser.add_argument("--seed", type=int, default=20261123)
    parser.add_argument(
        "--api", default=os.environ.get("API_BASE_URL", "http://localhost:8000")
    )
    parser.add_argument(
        "--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    parser.add_argument("--database", default=os.environ.get("DATABASE_URL", "postgresql://titan:titan_dev_password@localhost:5435/titan"))
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
    destination = ROOT / "benchmarks" / "sustained-load-2026-11-23.json"
    destination.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))


def _run_phase(args: argparse.Namespace, phase_number: int, rate: float) -> dict[str, object]:
    count = max(1, int(round(rate * args.duration_seconds)))
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
    events = generate_orders(
        count,
        args.seed + phase_number,
        deterministic_event_ids=True,
        occurred_span=None,
    )
    ensure_commerce_topics(args.bootstrap)
    producer = connect_producer(args.bootstrap, client_id=f"titan-sustained-{phase_number}")
    ack_seconds: list[float] = []
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
            elapsed = time.perf_counter() - started
            target_elapsed = acknowledged / rate
            if target_elapsed > elapsed:
                time.sleep(target_elapsed - elapsed)
    finally:
        producer.flush(30)
        stop_sampling.set()
        sampler.join(timeout=2)
    elapsed = time.perf_counter() - started
    target = before["commerce_events"] + acknowledged
    persistence_started = time.perf_counter()
    after = before
    while time.perf_counter() - persistence_started < 90:
        after = _database_counts(args.database)
        if after["commerce_events"] >= target:
            break
        time.sleep(0.5)
    persistence_seconds = time.perf_counter() - persistence_started
    after_stats = _get(args.api, "/api/v1/ingestion/stats")
    after_monitoring = _get(args.api, "/api/v1/monitoring/overview")
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
