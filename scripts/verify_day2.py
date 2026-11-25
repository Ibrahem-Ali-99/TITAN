"""Exercise REST, duplicate delivery, an order conflict, and a partial CSV import.

Requires the Compose stack, including the Day 2 migration and the consumer.
"""

import json
import os
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any
from uuid import uuid4

from scripts.envfile import load_env_file
from event_generator.generator import generate_orders
from titan_contracts.money import order_total, parse_money


def main() -> None:
    load_env_file(Path(__file__).resolve().parents[1] / ".env")
    base = os.environ.get("API_BASE_URL", "http://localhost:8000")
    event = generate_orders(1, seed=int(uuid4().int % 1_000_000), deterministic_event_ids=False)[0]
    before = _json(base, "/api/v1/ingestion/stats")
    first = _send(base, event.model_dump(mode="json"))
    if first["status"] != "published_to_kafka" or first["persisted"] is not False:
        raise SystemExit(f"REST ack was not a Kafka acknowledgement: {first}")
    _wait_event(base, str(event.event_id))
    after_first = _json(base, "/api/v1/ingestion/stats")
    second = _send(base, event.model_dump(mode="json"))
    if second["persisted"] is not False:
        raise SystemExit("republish was reported as persisted")
    time.sleep(1.5)
    recent = _json(base, "/api/v1/events/recent?limit=100")
    matches = [item for item in recent["items"] if item["event_id"] == str(event.event_id)]
    if len(matches) != 1:
        raise SystemExit(f"expected one persisted copy, saw {len(matches)}")
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
    _wait_event(base, conflict["event_id"])
    after_conflict = _json(base, "/api/v1/ingestion/stats")
    if after_conflict["logical_orders"] != after_first["logical_orders"]:
        raise SystemExit(
            "conflict changed logical_orders: "
            f"{after_first['logical_orders']} -> {after_conflict['logical_orders']}"
        )
    if after_conflict["order_conflicts"] != before["order_conflicts"] + 1:
        raise SystemExit(
            "conflict was not recorded: "
            f"{before['order_conflicts']} -> {after_conflict['order_conflicts']}"
        )
    header = (
        "event_type,occurred_at,order_id,customer_id,currency,product_id,quantity,unit_price,"
        "payment_id,amount,payment_method,quantity_on_hand,inventory_delta,warehouse_id"
    )
    good = (
        "order_created,2026-01-01T00:00:00Z,ord_day2csv001,cus_day2cust01,"
        "USD,prd_day2prod01,1,5.00,,,,,,"
    )
    bad = "order_created,2026-01-01T00:00:00Z,bad,cus_day2cust01,USD,prd_day2prod01,1,5.00,,,,,,"
    imported = _csv(base, f"{header}\n{good}\n{bad}\n")
    detail = _wait_import(base, imported["import_id"])
    statuses = {row["status"] for row in detail["rows"]}
    report = {
        "rest_event_id": str(event.event_id),
        "logical_orders_before": before["logical_orders"],
        "logical_orders_after_conflict": after_conflict["logical_orders"],
        "duplicate_persisted_rows": len(matches),
        "csv_statuses": sorted(statuses),
        "order_conflicts": after_conflict["order_conflicts"],
    }
    print(json.dumps(report, indent=2))
    if statuses != {"published", "rejected"}:
        raise SystemExit(f"unexpected csv statuses: {statuses}")


def _send(base: str, payload: dict[str, Any]) -> dict[str, Any]:
    request = urllib.request.Request(
        f"{base}/api/v1/ingestion/events",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read().decode()
    except urllib.error.HTTPError as exc:
        raise SystemExit(f"event publish failed: {exc.code} {exc.read().decode()}") from exc
    body = json.loads(raw)
    if not isinstance(body, dict):
        raise RuntimeError("event response was not an object")
    return body


def _csv(base: str, text: str) -> dict[str, Any]:
    boundary = "titanboundary"
    body = (
        f"--{boundary}\r\n"
        'Content-Disposition: form-data; name="file"; filename="day2.csv"\r\n'
        "Content-Type: text/csv\r\n\r\n"
        f"{text}\r\n"
        f"--{boundary}--\r\n"
    ).encode()
    request = urllib.request.Request(
        f"{base}/api/v1/ingestion/imports",
        data=body,
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        parsed = json.loads(response.read().decode())
    if not isinstance(parsed, dict):
        raise RuntimeError("import response was not an object")
    return parsed


def _json(base: str, path: str) -> dict[str, Any]:
    with urllib.request.urlopen(f"{base}{path}", timeout=10) as response:
        body = json.loads(response.read().decode())
    if not isinstance(body, dict):
        raise RuntimeError(f"{path} was not an object")
    return body


def _wait_event(base: str, event_id: str) -> None:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        recent = _json(base, "/api/v1/events/recent?limit=100")
        items = recent.get("items")
        if isinstance(items, list) and any(
            isinstance(item, dict) and item.get("event_id") == event_id for item in items
        ):
            return
        time.sleep(0.5)
    raise SystemExit(f"event {event_id} was not persisted")


def _wait_import(base: str, import_id: object) -> dict[str, Any]:
    deadline = time.monotonic() + 30
    last: dict[str, Any] = {}
    while time.monotonic() < deadline:
        last = _json(base, f"/api/v1/ingestion/imports/{import_id}")
        if last.get("status") in {"completed", "failed"}:
            return last
        time.sleep(0.5)
    raise SystemExit(f"import did not finish: {last}")


if __name__ == "__main__":
    main()
