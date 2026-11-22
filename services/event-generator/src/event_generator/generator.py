"""Seeded synthetic orders. The payload RNG is not used to mint event ids."""

import random
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from titan_contracts.events import OrderCreatedEvent
from titan_contracts.identifiers import ORDER_CREATED, SCHEMA_VERSION, SYNTHETIC_SOURCE
from titan_contracts.money import SUPPORTED_CURRENCIES, order_total
from titan_contracts.orders import OrderCreatedPayload, OrderItem

# Stable namespace so deterministic ids do not depend on the payload RNG.
EVENT_ID_NAMESPACE = uuid.UUID("6f0d3e2a-1b7c-4d5e-8f90-a1b2c3d4e5f6")
TIMELINE_ORIGIN = datetime(2026, 1, 1, tzinfo=UTC)

CATALOG: tuple[tuple[str, Decimal], ...] = (
    ("prd_100000000001", Decimal("12.50")),
    ("prd_100000000002", Decimal("19.99")),
    ("prd_100000000003", Decimal("4.00")),
    ("prd_100000000004", Decimal("75.00")),
    ("prd_100000000005", Decimal("8.25")),
    ("prd_100000000006", Decimal("120.00")),
)


def _token(rng: random.Random, prefix: str) -> str:
    alphabet = "0123456789abcdef"
    return prefix + "".join(rng.choice(alphabet) for _ in range(12))


def _event_id(seed: int, index: int, deterministic_event_ids: bool) -> uuid.UUID:
    if deterministic_event_ids:
        return uuid.uuid5(EVENT_ID_NAMESPACE, f"order_created:{seed}:{index}")
    return uuid.uuid4()


def generate_orders(
    count: int,
    seed: int,
    *,
    deterministic_event_ids: bool = False,
    produced_at: datetime | None = None,
) -> list[OrderCreatedEvent]:
    if count < 1:
        raise ValueError("count must be at least 1")
    rng = random.Random(seed)
    wall_clock = produced_at or datetime.now(UTC)
    events: list[OrderCreatedEvent] = []
    for index in range(count):
        item_count = rng.randint(1, 3)
        chosen = rng.sample(CATALOG, item_count)
        items = [
            OrderItem(product_id=product_id, quantity=rng.randint(1, 4), unit_price=price)
            for product_id, price in chosen
        ]
        currency = rng.choice(SUPPORTED_CURRENCIES)
        total = order_total((item.quantity, item.unit_price) for item in items)
        occurred_at = TIMELINE_ORIGIN + timedelta(seconds=index)
        event_produced_at = (
            occurred_at + timedelta(seconds=1) if deterministic_event_ids else wall_clock
        )
        events.append(
            OrderCreatedEvent(
                event_id=_event_id(seed, index, deterministic_event_ids),
                event_type=ORDER_CREATED,
                schema_version=SCHEMA_VERSION,
                occurred_at=occurred_at,
                produced_at=event_produced_at,
                source=SYNTHETIC_SOURCE,
                payload=OrderCreatedPayload(
                    order_id=_token(rng, "ord_"),
                    customer_id=_token(rng, "cus_"),
                    items=items,
                    currency=currency,
                    total_amount=total,
                ),
            )
        )
    return events
