"""Exercise Bronze, a Spark restart, and two Silver publications.

Requires the Compose stack, migration 0003, and the processing profile image.
"""

import json
import os
import random
import subprocess
import time
import urllib.request
from pathlib import Path
from typing import Any
from uuid import uuid4

from scripts.envfile import load_env_file
from event_generator.generator import generate_orders
from titan_contracts.money import order_total, parse_money


def main() -> None:
    load_env_file(Path(__file__).resolve().parents[1] / ".env")
    _compose("--profile", "processing", "run", "--rm", "lake-init")
    base = os.environ.get("API_BASE_URL", "http://localhost:8000")
    _compose("run", "--rm", "--build", "migrate")
    _compose("up", "-d", "--build", "api")
    _wait_ready(base)

    before = _json(base, "/api/v1/ingestion/stats")
    event = generate_orders(1, seed=_seed(), deterministic_event_ids=False)[0]
    _send(base, event.model_dump(mode="json"))
    _send(base, event.model_dump(mode="json"))
    conflict = event.model_dump(mode="json")
    conflict["event_id"] = str(uuid4())
    changed = conflict["payload"]["items"][0]
    changed["quantity"] = 3 if changed["quantity"] == 2 else 2
    conflict["payload"]["total_amount"] = format(
        order_total(
            (item["quantity"], parse_money(item["unit_price"]))
            for item in conflict["payload"]["items"]
        ),
        "f",
    )
    _send(base, conflict)
    _wait_event(base, str(event.event_id))
    _wait_event(base, conflict["event_id"])
    after_conflict = _json(base, "/api/v1/ingestion/stats")
    if after_conflict["logical_orders"] != before["logical_orders"] + 1:
        raise SystemExit(
            "conflict changed the logical order count: "
            f"{before['logical_orders']} -> {after_conflict['logical_orders']}"
        )
    _send(base, _payment(event.payload.order_id, event.payload.customer_id))
    _send(base, _inventory())
    _invalid_kafka()
    _wait_stat(base, "accepted_payments", after_conflict["accepted_payments"] + 1)
    _wait_stat(base, "inventory_updates", after_conflict["inventory_updates"] + 1)

    _compose("--profile", "processing", "run", "--rm", "--build", "spark-bronze")
    first_lake = _report()
    _compose("--profile", "processing", "run", "--rm", "spark-bronze")
    restarted = _report()
    if restarted["bronze_rows"] != first_lake["bronze_rows"]:
        raise SystemExit(f"restart changed bronze rows: {first_lake} -> {restarted}")
    if restarted["bronze_distinct_offsets"] != restarted["bronze_rows"]:
        raise SystemExit(f"bronze rows are not one per offset: {restarted}")
    if restarted["bronze_invalid_rows"] < 1:
        raise SystemExit(f"invalid kafka record is missing from bronze: {restarted}")

    extra = generate_orders(1, seed=_seed(), deterministic_event_ids=False)[0]
    _send(base, extra.model_dump(mode="json"))
    _wait_event(base, str(extra.event_id))
    _compose("--profile", "processing", "run", "--rm", "spark-bronze")
    grown = _report()
    if grown["bronze_distinct_offsets"] != restarted["bronze_distinct_offsets"] + 1:
        raise SystemExit(f"new event was not consumed once: {restarted} -> {grown}")

    _compose("--profile", "processing", "run", "--rm", "spark-silver")
    _compose("--profile", "processing", "run", "--rm", "spark-silver")
    final = _report()
    published = [row for row in final["publications"] if row["status"] == "published"]
    if len(published) < 2:
        raise SystemExit(f"expected two published versions: {final['publications']}")
    counts = {row["silver_order_count"] for row in published[:2]}
    stats = _json(base, "/api/v1/ingestion/stats")
    if counts != {stats["logical_orders"]}:
        raise SystemExit(
            f"silver counts {counts} do not match logical orders {stats['logical_orders']}"
        )
    if final["current_silver_orders"] != stats["logical_orders"]:
        raise SystemExit("current parquet count does not match the published statistic")
    if not isinstance(final["current_revenue"], dict) or not final["current_revenue"]:
        raise SystemExit(f"revenue was not grouped by currency: {final['current_revenue']}")
    lake = _json(base, "/api/v1/lake/status")
    if lake["current_publication"]["silver_order_count"] != stats["logical_orders"]:
        raise SystemExit("API lake status does not match the published order count")
    print(
        json.dumps(
            {"bronze": grown, "publications": published[:2], "lake": lake["current_publication"]},
            indent=2,
        )
    )


def _report() -> dict[str, Any]:
    output = _compose(
        "--profile",
        "processing",
        "run",
        "--rm",
        "spark-silver",
        "python",
        "-m",
        "titan_spark.report",
    )
    # spark-silver command override: compose run service command...
    # The service command is replaced by the trailing args. Good.
    for line in reversed(output.splitlines()):
        if line.startswith("{"):
            parsed = json.loads(line)
            if isinstance(parsed, dict):
                return parsed
    raise SystemExit(f"lake report was not json:\n{output}")


def _compose(*args: str) -> str:
    completed = subprocess.run(
        ["docker", "compose", *args],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise SystemExit(completed.stderr[-4000:] or completed.stdout[-4000:])
    return completed.stdout


def _send(base: str, payload: dict[str, Any]) -> None:
    request = urllib.request.Request(
        f"{base}/api/v1/ingestion/events",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        body = json.loads(response.read().decode())
    if response.status != 202 or body.get("persisted") is not False:
        raise SystemExit(f"unexpected publish response: {response.status} {body}")


def _json(base: str, path: str) -> dict[str, Any]:
    with urllib.request.urlopen(base + path, timeout=30) as response:
        body = json.loads(response.read().decode())
    if not isinstance(body, dict):
        raise RuntimeError("response was not an object")
    return body


def _wait_ready(base: str) -> None:
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        try:
            if _json(base, "/health/ready")["status"] == "ready":
                return
        except Exception:
            time.sleep(2)
            continue
        time.sleep(2)
    raise SystemExit("API did not become ready")


def _wait_event(base: str, event_id: str) -> None:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        recent = _json(base, "/api/v1/events/recent?limit=100")
        if any(item["event_id"] == event_id for item in recent["items"]):
            return
        time.sleep(0.5)
    raise SystemExit(f"event {event_id} was not persisted")


def _wait_stat(base: str, field: str, minimum: int) -> None:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if _json(base, "/api/v1/ingestion/stats")[field] >= minimum:
            return
        time.sleep(0.5)
    raise SystemExit(f"{field} did not reach {minimum}")


def _seed() -> int:
    return random.SystemRandom().randrange(1, 2**31 - 1)


def _payment(order_id: str, customer_id: str) -> dict[str, Any]:
    moment = "2026-11-22T15:00:00+00:00"
    return {
        "event_id": str(uuid4()),
        "event_type": "payment_completed",
        "schema_version": 1,
        "occurred_at": moment,
        "produced_at": moment,
        "source": "rest.day3",
        "payload": {
            "payment_id": f"pay_{uuid4().hex[:16]}",
            "order_id": order_id,
            "customer_id": customer_id,
            "currency": "USD",
            "amount": "10.00",
            "method": "card",
        },
    }


def _inventory() -> dict[str, Any]:
    moment = "2026-11-22T15:00:00+00:00"
    return {
        "event_id": str(uuid4()),
        "event_type": "inventory_updated",
        "schema_version": 1,
        "occurred_at": moment,
        "produced_at": moment,
        "source": "rest.day3",
        "payload": {
            "product_id": f"prd_{uuid4().hex[:12]}",
            "warehouse_id": "wh_main0001",
            "quantity_on_hand": 8,
            "delta": -1,
        },
    }


def _invalid_kafka() -> None:
    from confluent_kafka import Producer

    servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    producer = Producer({"bootstrap.servers": servers})
    producer.produce("titan.commerce.order-events", key=b"ord_badkey01", value=b"not-json")
    producer.flush(10)


if __name__ == "__main__":
    main()
