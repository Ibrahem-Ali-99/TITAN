"""Measure producer acknowledgements on an isolated topic.

The topic is ``titan.benchmark.acks``. Ingestion does not subscribe to it, so
this run does not create accepted orders. The numbers are one local sample.
"""

import json
import os
import time
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka import admin as kafka_admin

from scripts.envfile import load_env_file

ROOT = Path(__file__).resolve().parents[1]
TOPIC = "titan.benchmark.acks"


def main() -> None:
    load_env_file(ROOT / ".env")
    count = int(os.environ.get("TITAN_BENCH_COUNT", "100"))
    seed = int(os.environ.get("TITAN_BENCH_SEED", "71022"))
    base = os.environ.get("API_BASE_URL", "http://localhost:8000")
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    before_orders = _orders(base)
    before_lag = _lag(base)
    _ensure_topic(bootstrap)
    producer = Producer(
        {
            "bootstrap.servers": bootstrap,
            "client.id": "titan-benchmark",
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 5,
            "allow.auto.create.topics": False,
        }
    )
    samples: list[float] = []
    started = time.perf_counter()
    try:
        for index in range(count):
            one = time.perf_counter()
            _produce(producer, f"{seed}-{index}".encode())
            samples.append(time.perf_counter() - one)
    finally:
        producer.flush(30)
    elapsed = time.perf_counter() - started
    after_orders = _orders(base)
    if before_orders is not None and after_orders is not None and after_orders != before_orders:
        raise SystemExit(f"benchmark changed logical_orders from {before_orders} to {after_orders}")
    ordered = sorted(samples)
    result = {
        "measured_at": datetime.now(UTC).isoformat(),
        "count": count,
        "seed": seed,
        "topic": TOPIC,
        "elapsed_seconds": round(elapsed, 3),
        "acknowledged_per_second": round(count / elapsed, 3) if elapsed else None,
        "ack_seconds": {
            "p50": round(_percentile(ordered, 0.50), 6),
            "p95": round(_percentile(ordered, 0.95), 6),
            "p99": round(_percentile(ordered, 0.99), 6),
        },
        "logical_orders_before": before_orders,
        "logical_orders_after": after_orders,
        "ingestion_lag_before": before_lag,
        "ingestion_lag_after": _lag(base),
        "scope": (
            "Producer acknowledgement on titan.benchmark.acks. "
            "Ingestion does not consume this topic. "
            "This is not warehouse freshness and it is not an HTTP response time."
        ),
    }
    destination = ROOT / "benchmarks" / f"day7-{count}.json"
    destination.parent.mkdir(exist_ok=True)
    destination.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))


def _produce(producer: Producer, key: bytes) -> None:
    box: dict[str, object] = {}

    def on_delivery(err: object, msg: object, box: dict[str, object] = box) -> None:
        box["err"] = err
        box["msg"] = msg

    producer.produce(TOPIC, key=key, value=b'{"benchmark":true}', on_delivery=on_delivery)
    remaining = producer.flush(30)
    if remaining != 0 or box.get("err") is not None or box.get("msg") is None:
        raise SystemExit(f"benchmark produce was not acknowledged: {box.get('err')}")


def _ensure_topic(bootstrap: str) -> None:
    client = kafka_admin.AdminClient({"bootstrap.servers": bootstrap})
    metadata = client.list_topics(timeout=10)
    topic = metadata.topics.get(TOPIC)
    if topic is not None and topic.error is None:
        return
    created = client.create_topics(
        [kafka_admin.NewTopic(TOPIC, num_partitions=1, replication_factor=1)]  # type: ignore[attr-defined]
    )
    try:
        created[TOPIC].result(timeout=30)
    except Exception as exc:
        if "already exists" not in str(exc).lower():
            raise


def _orders(base: str) -> int | None:
    body = _get(base, "/api/v1/ingestion/stats")
    if not isinstance(body, dict) or "logical_orders" not in body:
        return None
    return int(body["logical_orders"])


def _lag(base: str) -> int | None:
    body = _get(base, "/api/v1/monitoring/overview")
    if not isinstance(body, dict):
        return None
    lag = body.get("ingestion_lag")
    if not isinstance(lag, dict) or not lag.get("available"):
        return None
    total = lag.get("total_lag")
    return None if total is None else int(total)


def _get(base: str, path: str) -> object:
    try:
        with urllib.request.urlopen(base + path, timeout=10) as response:
            return json.loads(response.read().decode())
    except Exception:
        return None


def _percentile(ordered: list[float], fraction: float) -> float:
    if not ordered:
        raise SystemExit("no samples")
    index = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * fraction))))
    return ordered[index]


if __name__ == "__main__":
    main()
