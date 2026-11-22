"""Seeded synthetic orders. The payload RNG is not used to mint event ids."""

import random
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from titan_contracts.events import (
    CommerceEvent,
    InventoryUpdatedEvent,
    OrderCreatedEvent,
    PaymentCompletedEvent,
)
from titan_contracts.identifiers import (
    INVENTORY_UPDATED,
    ORDER_CREATED,
    PAYMENT_COMPLETED,
    SCHEMA_VERSION,
    SYNTHETIC_SOURCE,
)
from titan_contracts.inventory import InventoryUpdatedPayload
from titan_contracts.money import SUPPORTED_CURRENCIES, order_total
from titan_contracts.orders import OrderCreatedPayload, OrderItem
from titan_contracts.payments import PaymentCompletedPayload

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


def _occurred_at(
    index: int,
    count: int,
    occurred_span: timedelta | None,
    late_every: int | None,
    late_by: timedelta | None,
) -> datetime:
    """Default timestamps stay one second apart from 2026-01-01.

    ``occurred_span`` spreads the same seed across a longer business timeline.
    ``late_every`` moves the last event of each group earlier by ``late_by``.
    """
    if occurred_span is None:
        return TIMELINE_ORIGIN + timedelta(seconds=index)
    if occurred_span.total_seconds() < 0:
        raise ValueError("occurred_span must be zero or positive")
    if count == 1:
        occurred_at = TIMELINE_ORIGIN
    else:
        offset_seconds = int(occurred_span.total_seconds()) * index // (count - 1)
        occurred_at = TIMELINE_ORIGIN + timedelta(seconds=offset_seconds)
    if (
        late_every is not None
        and late_by is not None
        and late_every > 0
        and (index + 1) % late_every == 0
    ):
        return occurred_at - late_by
    return occurred_at


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
    occurred_span: timedelta | None = None,
    late_every: int | None = None,
    late_by: timedelta | None = None,
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
        occurred_at = _occurred_at(index, count, occurred_span, late_every, late_by)
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


def generate_history(days: int, seed: int) -> list[CommerceEvent]:
    """Deterministic multi-day orders, matching payments, and daily inventory.

    This is the ML backfill scenario. It is not the short local demo generator.
    Event ids are stable for the seed, so a second publish does not add orders.
    """
    if days < 14 or days > 120:
        raise ValueError("history days must be between 14 and 120")
    factors = (1.0, 1.0, 1.1, 1.2, 1.6, 0.7, 0.5)
    currencies = ("USD", "EUR", "GBP")
    events: list[CommerceEvent] = []
    sequence = 0
    for day in range(days):
        moment = TIMELINE_ORIGIN + timedelta(days=day)
        sold: dict[str, int] = {product_id: 0 for product_id, _price in CATALOG}
        order_count = max(2, int(round(6 * factors[moment.weekday()] + day // 14)))
        for index in range(order_count):
            product_id, price = CATALOG[(day * 3 + index + seed) % len(CATALOG)]
            quantity = 1 + (day + index) % 4
            sold[product_id] += quantity
            total = order_total(((quantity, price),))
            occurred_at = moment + timedelta(hours=9, minutes=index)
            customer_id = f"cus_hist{(index + day) % 12:08d}"
            order_id = f"ord_h{seed % 10000:04d}{day:03d}{index:02d}"
            currency = currencies[(day + index) % 3]
            events.append(
                OrderCreatedEvent(
                    event_id=_history_id(seed, "order", sequence),
                    event_type=ORDER_CREATED,
                    schema_version=SCHEMA_VERSION,
                    occurred_at=occurred_at,
                    produced_at=occurred_at + timedelta(minutes=30),
                    source=SYNTHETIC_SOURCE,
                    payload=OrderCreatedPayload(
                        order_id=order_id,
                        customer_id=customer_id,
                        items=[
                            OrderItem(
                                product_id=product_id,
                                quantity=quantity,
                                unit_price=price,
                            )
                        ],
                        currency=currency,
                        total_amount=total,
                    ),
                )
            )
            events.append(
                PaymentCompletedEvent(
                    event_id=_history_id(seed, "payment", sequence),
                    event_type=PAYMENT_COMPLETED,
                    schema_version=SCHEMA_VERSION,
                    occurred_at=occurred_at + timedelta(minutes=20),
                    produced_at=occurred_at + timedelta(minutes=40),
                    source=SYNTHETIC_SOURCE,
                    payload=PaymentCompletedPayload(
                        payment_id=f"pay_h{seed % 10000:04d}{day:03d}{index:02d}",
                        order_id=order_id,
                        customer_id=customer_id,
                        currency=currency,
                        amount=total,
                        method="card",
                    ),
                )
            )
            sequence += 1
        for product_index, (product_id, _price) in enumerate(CATALOG):
            events.append(
                InventoryUpdatedEvent(
                    event_id=_history_id(seed, "inventory", day * 10 + product_index),
                    event_type=INVENTORY_UPDATED,
                    schema_version=SCHEMA_VERSION,
                    occurred_at=moment + timedelta(hours=8),
                    produced_at=moment + timedelta(hours=8, minutes=5),
                    source=SYNTHETIC_SOURCE,
                    payload=InventoryUpdatedPayload(
                        product_id=product_id,
                        warehouse_id="wh_main0001",
                        quantity_on_hand=max(0, 500 - day * 3),
                        delta=-sold[product_id],
                    ),
                )
            )
    return events


def _history_id(seed: int, kind: str, index: int) -> uuid.UUID:
    return uuid.uuid5(EVENT_ID_NAMESPACE, f"history:{seed}:{kind}:{index}")
