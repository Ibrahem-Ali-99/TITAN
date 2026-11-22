"""Synthetic end-to-end demonstration. It adds one new order and publishes it.

A rerun uses a new identifier, so it does not collide with the previous run.
The quality injection fails one candidate and leaves the previous publication
in place. The following Dagster run publishes the new Silver version.
"""

import json
import os
import subprocess
import time
import urllib.request
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from titan_contracts.events import InventoryUpdatedEvent, OrderCreatedEvent, PaymentCompletedEvent
from titan_contracts.inventory import InventoryUpdatedPayload
from titan_contracts.orders import OrderCreatedPayload, OrderItem
from titan_contracts.payments import PaymentCompletedPayload
from titan_messaging.producer import connect_producer

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    base = os.environ.get("API_BASE_URL", "http://localhost:8000")
    _wait_ready(base)
    before = _json(base, "/api/v1/warehouse/status")
    if not before.get("ready"):
        raise SystemExit(f"warehouse is not ready: {before.get('reason')}")
    models = _json(base, "/api/v1/ml/models?limit=3")
    serving = next(row for row in models["models"] if row["status"] == "serving")
    token = uuid.uuid4().hex[:8]
    order_id = f"ord_{token}demo"
    customer_id = f"cus_{token}demo"
    payment_id = f"pay_{token}demo"
    now = datetime.now(UTC)
    order = OrderCreatedEvent(
        event_id=uuid.uuid5(uuid.NAMESPACE_URL, f"titan-day7-{token}-order"),
        occurred_at=now,
        produced_at=now + timedelta(seconds=1),
        source="synthetic.day7-demo",
        payload=OrderCreatedPayload(
            order_id=order_id,
            customer_id=customer_id,
            currency="EUR",
            total_amount=Decimal("19.95"),
            items=[OrderItem(product_id="prd_day7demo1", quantity=1, unit_price=Decimal("19.95"))],
        ),
    )
    conflict = OrderCreatedEvent(
        event_id=uuid.uuid5(uuid.NAMESPACE_URL, f"titan-day7-{token}-conflict"),
        occurred_at=now,
        produced_at=now + timedelta(seconds=2),
        source="synthetic.day7-demo",
        payload=OrderCreatedPayload(
            order_id=order_id,
            customer_id=customer_id,
            currency="EUR",
            total_amount=Decimal("39.90"),
            items=[OrderItem(product_id="prd_day7demo1", quantity=2, unit_price=Decimal("19.95"))],
        ),
    )
    payment = PaymentCompletedEvent(
        event_id=uuid.uuid5(uuid.NAMESPACE_URL, f"titan-day7-{token}-payment"),
        occurred_at=now + timedelta(minutes=1),
        produced_at=now + timedelta(minutes=1, seconds=1),
        source="synthetic.day7-demo",
        payload=PaymentCompletedPayload(
            payment_id=payment_id,
            order_id=order_id,
            customer_id=customer_id,
            currency="EUR",
            amount=Decimal("19.95"),
            method="card",
        ),
    )
    inventory = InventoryUpdatedEvent(
        event_id=uuid.uuid5(uuid.NAMESPACE_URL, f"titan-day7-{token}-inventory"),
        occurred_at=now,
        produced_at=now + timedelta(seconds=3),
        source="synthetic.day7-demo",
        payload=InventoryUpdatedPayload(
            product_id="prd_day7demo1",
            warehouse_id="wh_day7",
            quantity_on_hand=8,
            delta=-1,
            order_id=order_id,
        ),
    )
    stats_before = _json(base, "/api/v1/ingestion/stats")
    body = order.model_dump(mode="json")
    _post(base, "/api/v1/ingestion/events", body)
    _post(base, "/api/v1/ingestion/events", body)
    _post(base, "/api/v1/ingestion/events", conflict.model_dump(mode="json"))
    _post(base, "/api/v1/ingestion/events", payment.model_dump(mode="json"))
    _post(base, "/api/v1/ingestion/events", inventory.model_dump(mode="json"))
    _invalid_record(str(order.event_id))
    _wait_event(base, str(order.event_id))
    _wait_stat(base, "order_conflicts", int(stats_before["order_conflicts"]) + 1)
    _wait_stat(base, "quarantined_records", int(stats_before["quarantined_records"]) + 1)
    stats_mid = _json(base, "/api/v1/ingestion/stats")
    if int(stats_mid["logical_orders"]) != int(stats_before["logical_orders"]) + 1:
        raise SystemExit("the duplicate or conflict changed the accepted order count")
    _compose("--profile", "processing", "run", "--rm", "spark-bronze")
    _compose("--profile", "processing", "run", "--rm", "spark-silver")
    pointer = before["warehouse_publication_id"]
    failed = _dagster(
        "clickhouse_load",
        "warehouse_models",
        "quality_gate",
        inject="revenue_mismatch",
    )
    if failed.returncode == 0:
        raise SystemExit("injected quality failure returned success")
    if _json(base, "/api/v1/warehouse/status")["warehouse_publication_id"] != pointer:
        raise SystemExit("quality failure replaced the published warehouse")
    published = _dagster(
        "clickhouse_load",
        "warehouse_models",
        "quality_gate",
        "analytics_ready",
    )
    if published.returncode != 0:
        raise SystemExit(published.stderr[-3000:] or published.stdout[-3000:])
    status = _json(base, "/api/v1/warehouse/status")
    if not status.get("ready") or status["warehouse_publication_id"] == pointer:
        raise SystemExit(f"new publication was not served: {status.get('reason')}")
    orders = _json(base, "/api/v1/analytics/orders?currency=EUR&limit=20")
    matched = [
        row
        for row in orders["orders"]
        if row["order_id"] == order_id and row["total_amount"] == "19.95"
    ]
    if not matched:
        raise SystemExit(f"published orders did not include {order_id}")
    after_models = _json(base, "/api/v1/ml/models?limit=3")
    still = next(row for row in after_models["models"] if row["status"] == "serving")
    if still["serving_strategy"] != "seasonal_naive":
        raise SystemExit("serving strategy changed during the demo")
    if still["run_id"] != serving["run_id"]:
        raise SystemExit("the demo replaced the serving model")
    if still["warehouse_publication_id"] != serving["warehouse_publication_id"]:
        raise SystemExit("training lineage changed without a training run")
    report = {
        "run_token": token,
        "order_id": order_id,
        "event_id": str(order.event_id),
        "previous_publication": pointer,
        "publication": status["warehouse_publication_id"],
        "silver_publication_id": status["silver_publication_id"],
        "revenue": status["accepted_revenue"],
        "order_count": status["order_count"],
        "serving_run_id": still["run_id"],
        "serving_strategy": still["serving_strategy"],
        "training_publication_id": still["warehouse_publication_id"],
        "source": "synthetic.day7-demo",
    }
    destination = ROOT / "benchmarks" / "demo-day7.json"
    destination.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))


def _invalid_record(key: str) -> None:
    producer = connect_producer(os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    producer.produce("titan.commerce.order-events", key=key.encode(), value=b"not-json")
    remaining = producer.flush(30)
    if remaining:
        raise SystemExit("invalid record was not acknowledged")


def _dagster(*assets: str, inject: str | None = None) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env.pop("TITAN_QUALITY_INJECT", None)
    if inject is not None:
        env["TITAN_QUALITY_INJECT"] = inject
    env["PYTHONPATH"] = os.pathsep.join(
        [
            str(ROOT / "pipelines" / "warehouse" / "src"),
            str(ROOT / "pipelines" / "quality" / "src"),
            str(ROOT / "pipelines" / "ml" / "src"),
            str(ROOT / "pipelines" / "dagster" / "src"),
            env.get("PYTHONPATH", ""),
        ]
    )
    env.setdefault("CLICKHOUSE_HOST", "localhost")
    env.setdefault("S3_ENDPOINT", "http://localhost:9000")
    return subprocess.run(
        ["uv", "run", "--group", "orchestration", "python", "-m", "titan_dagster", *assets],
        cwd=ROOT,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )


def _compose(*args: str) -> None:
    completed = subprocess.run(
        ["docker", "compose", *args],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise SystemExit((completed.stdout + completed.stderr)[-4000:])


def _wait_stat(base: str, key: str, minimum: int) -> None:
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        stats = _json(base, "/api/v1/ingestion/stats")
        if int(stats[key]) >= minimum:
            return
        time.sleep(1)
    raise SystemExit(f"{key} did not reach {minimum}")


def _wait_event(base: str, event_id: str) -> None:
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        recent = _json(base, "/api/v1/events/recent?limit=50")
        if any(item["event_id"] == event_id for item in recent["items"]):
            return
        time.sleep(1)
    raise SystemExit(f"event {event_id} was not stored")


def _wait_ready(base: str) -> None:
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            body = _json(base, "/health/ready")
        except Exception:
            time.sleep(2)
            continue
        if body.get("status") == "ready":
            return
        time.sleep(2)
    raise SystemExit("API did not become ready")


def _post(base: str, path: str, payload: dict[str, Any]) -> None:
    request = urllib.request.Request(
        base + path,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        if response.status not in {200, 202}:
            raise SystemExit(f"{path} returned {response.status}")


def _json(base: str, path: str) -> dict[str, Any]:
    with urllib.request.urlopen(base + path, timeout=30) as response:
        body = json.loads(response.read().decode())
    if not isinstance(body, dict):
        raise SystemExit(f"{path} did not return an object")
    return body


if __name__ == "__main__":
    main()
