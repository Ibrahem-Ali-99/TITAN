"""Publish a fresh, unique portfolio traffic burst through the real REST/Kafka path."""

from __future__ import annotations

import argparse
import json
import os
import time
import urllib.error
import urllib.request
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, cast

from titan_contracts.events import (
    InventoryUpdatedEvent,
    OrderCreatedEvent,
    PaymentCompletedEvent,
)
from titan_contracts.inventory import InventoryUpdatedPayload
from titan_contracts.orders import OrderCreatedPayload, OrderItem
from titan_contracts.payments import PaymentCompletedPayload

PRODUCTS = (
    ("prd_100000000001", Decimal("12.50")),
    ("prd_100000000002", Decimal("19.99")),
    ("prd_100000000003", Decimal("4.00")),
    ("prd_100000000004", Decimal("75.00")),
    ("prd_100000000005", Decimal("8.25")),
    ("prd_100000000006", Decimal("120.00")),
)
CURRENCIES = ("EUR", "GBP", "USD")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--orders", type=int, default=8, help="Unique order cycles to publish")
    parser.add_argument("--interval", type=float, default=6.0, help="Seconds between order cycles")
    parser.add_argument("--wait", type=float, default=60.0, help="Seconds to wait for persistence")
    parser.add_argument("--seed", default=None, help="Optional run token; random by default")
    parser.add_argument(
        "--api-base", default=os.environ.get("API_BASE_URL", "http://localhost:18000")
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if args.orders < 1 or args.orders > 50:
        raise SystemExit("--orders must be between 1 and 50")
    if args.interval < 0 or args.interval > 60:
        raise SystemExit("--interval must be between 0 and 60 seconds")
    base = args.api_base.rstrip("/")
    read_token = os.environ.get("TITAN_READ_TOKEN", "titan-local-read-token")
    write_token = os.environ.get("TITAN_WRITE_TOKEN", "titan-local-write-token")
    before = request_json(base, "/api/v1/ingestion/stats", read_token)
    run_token = args.seed or uuid.uuid4().hex[:10]
    event_ids: list[str] = []
    for index in range(args.orders):
        for event in build_cycle(run_token, index):
            response = post_json(base, "/api/v1/ingestion/events", write_token, event)
            event_ids.append(str(response.get("event_id", event["event_id"])))
        if index + 1 < args.orders:
            time.sleep(args.interval)
    after = wait_for_persistence(
        base,
        read_token,
        int(before["persisted_events"]) + args.orders * 3,
        args.wait,
    )
    if not after.get("recent_activity"):
        raise SystemExit("events persisted but no current activity buckets were returned")
    report = {
        "run_token": run_token,
        "source": "synthetic.portfolio-demo",
        "orders": args.orders,
        "events_published": len(event_ids),
        "persisted_before": before["persisted_events"],
        "persisted_after": after["persisted_events"],
        "recent_activity": after["recent_activity"],
        "event_ids": event_ids,
    }
    print(json.dumps(report, indent=2))


def build_cycle(run_token: str, index: int) -> list[dict[str, Any]]:
    product_id, unit_price = PRODUCTS[index % len(PRODUCTS)]
    currency = CURRENCIES[index % len(CURRENCIES)]
    quantity = (index % 3) + 1
    total = unit_price * quantity
    order_id = f"ord_portfolio{run_token}{index:02d}"
    customer_id = f"cus_portfolio{run_token}{index:02d}"
    now = datetime.now(UTC)
    order = OrderCreatedEvent(
        event_id=uuid.uuid4(),
        occurred_at=now,
        produced_at=now,
        source="synthetic.portfolio-demo",
        payload=OrderCreatedPayload(
            order_id=order_id,
            customer_id=customer_id,
            currency=currency,
            total_amount=total,
            items=[OrderItem(product_id=product_id, quantity=quantity, unit_price=unit_price)],
        ),
    )
    payment = PaymentCompletedEvent(
        event_id=uuid.uuid4(),
        occurred_at=now,
        produced_at=now,
        source="synthetic.portfolio-demo",
        payload=PaymentCompletedPayload(
            payment_id=f"pay_portfolio{run_token}{index:02d}",
            order_id=order_id,
            customer_id=customer_id,
            currency=currency,
            amount=total,
            method=("card", "wallet", "bank_transfer")[index % 3],
        ),
    )
    inventory = InventoryUpdatedEvent(
        event_id=uuid.uuid4(),
        occurred_at=now,
        produced_at=now,
        source="synthetic.portfolio-demo",
        payload=InventoryUpdatedPayload(
            product_id=product_id,
            warehouse_id="wh_portfolio",
            quantity_on_hand=500 - (index * quantity),
            delta=-quantity,
            order_id=order_id,
        ),
    )
    return [
        order.model_dump(mode="json"),
        payment.model_dump(mode="json"),
        inventory.model_dump(mode="json"),
    ]


def request_json(base: str, path: str, token: str) -> dict[str, Any]:
    request = urllib.request.Request(
        base + path,
        headers={"Authorization": f"Bearer {token}"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return cast(dict[str, Any], json.load(response))


def post_json(base: str, path: str, token: str, payload: dict[str, Any]) -> dict[str, Any]:
    request = urllib.request.Request(
        base + path,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            if response.status not in {200, 202}:
                raise RuntimeError(f"{path} returned {response.status}")
            return cast(dict[str, Any], json.load(response))
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"{path} returned {exc.code}: {exc.read().decode()}") from exc


def wait_for_persistence(base: str, token: str, minimum: int, timeout: float) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    latest: dict[str, Any] = {}
    while time.monotonic() < deadline:
        latest = request_json(base, "/api/v1/ingestion/stats", token)
        if int(latest["persisted_events"]) >= minimum:
            return latest
        time.sleep(1)
    raise SystemExit(
        f"persisted_events did not reach {minimum}; latest={latest.get('persisted_events')}"
    )


if __name__ == "__main__":
    main()
