"""Measure a bounded producer burst. This does not estimate production capacity."""

import json
import os
import time
from datetime import UTC, datetime
from pathlib import Path

from envfile import load_env_file
from event_generator.generator import generate_orders
from titan_messaging.admin import ensure_commerce_topics
from titan_messaging.producer import connect_producer, publish_record

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    load_env_file(ROOT / ".env")
    count = int(os.environ.get("TITAN_BENCH_COUNT", "100"))
    seed = int(os.environ.get("TITAN_BENCH_SEED", "61022"))
    events = generate_orders(count, seed=seed, deterministic_event_ids=True)
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    ensure_commerce_topics(bootstrap)
    producer = connect_producer(bootstrap)
    samples: list[float] = []
    started = time.perf_counter()
    try:
        for event in events:
            one = time.perf_counter()
            publish_record(producer, event)
            samples.append(time.perf_counter() - one)
    finally:
        producer.flush(10)
    elapsed = time.perf_counter() - started
    ordered = sorted(samples)
    result = {
        "measured_at": datetime.now(UTC).isoformat(),
        "count": count,
        "seed": seed,
        "elapsed_seconds": round(elapsed, 3),
        "acknowledged_per_second": round(count / elapsed, 3) if elapsed else None,
        "ack_seconds": {
            "p50": round(_percentile(ordered, 0.50), 6),
            "p95": round(_percentile(ordered, 0.95), 6),
            "p99": round(_percentile(ordered, 0.99), 6),
        },
        "definition": (
            "Producer ack time is local monotonic time around publish_record. "
            "It is not warehouse freshness and it is not an HTTP response time."
        ),
    }
    destination = ROOT / "benchmarks" / f"day6-{count}.json"
    destination.parent.mkdir(exist_ok=True)
    destination.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))


def _percentile(ordered: list[float], fraction: float) -> float:
    """Nearest-rank percentile. The sample is the producer ack times from this run."""
    if not ordered:
        raise SystemExit("no samples")
    index = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * fraction))))
    return ordered[index]


if __name__ == "__main__":
    main()
