"""Seeded synthetic orders. The payload RNG is not used to mint event ids."""

import random
import uuid
from dataclasses import dataclass
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
FORECASTING_TIMELINE_ORIGIN = datetime(2026, 10, 12, tzinfo=UTC)
FORECASTING_SOURCE = "synthetic.forecasting-profile"

CATALOG: tuple[tuple[str, Decimal], ...] = (
    ("prd_100000000001", Decimal("12.50")),
    ("prd_100000000002", Decimal("19.99")),
    ("prd_100000000003", Decimal("4.00")),
    ("prd_100000000004", Decimal("75.00")),
    ("prd_100000000005", Decimal("8.25")),
    ("prd_100000000006", Decimal("120.00")),
)


@dataclass(frozen=True)
class ForecastProduct:
    product_id: str
    unit_price: Decimal
    base_units: float
    pattern: str
    intermittent_probability: float = 0.0


FORECASTING_CATALOG: tuple[ForecastProduct, ...] = (
    ForecastProduct("prd_fcst0001", Decimal("18.50"), 10.0, "weekday"),
    ForecastProduct("prd_fcst0002", Decimal("29.00"), 7.0, "weekend"),
    ForecastProduct("prd_fcst0003", Decimal("7.25"), 16.0, "steady"),
    ForecastProduct("prd_fcst0004", Decimal("64.00"), 4.0, "payday"),
    ForecastProduct("prd_fcst0005", Decimal("12.00"), 12.0, "weekday", 0.08),
    ForecastProduct("prd_fcst0006", Decimal("42.50"), 6.0, "weekend", 0.12),
    ForecastProduct("prd_fcst0007", Decimal("9.75"), 19.0, "steady"),
    ForecastProduct("prd_fcst0008", Decimal("88.00"), 3.0, "payday", 0.18),
    ForecastProduct("prd_fcst0009", Decimal("15.00"), 9.0, "weekday", 0.05),
    ForecastProduct("prd_fcst0010", Decimal("55.00"), 5.0, "weekend", 0.10),
    ForecastProduct("prd_fcst0011", Decimal("6.50"), 14.0, "steady", 0.04),
    ForecastProduct("prd_fcst0012", Decimal("120.00"), 2.5, "payday", 0.25),
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


def generate_forecasting_history(
    days: int,
    seed: int,
    *,
    include_edge_cases: bool = True,
) -> list[CommerceEvent]:
    """Generate a deterministic, product-varied demand history for forecasting.

    The profile is deliberately separate from the short demo and legacy 28-day
    backfill. Each product has a stable identity, a demand pattern, a rate, and
    an intermittent probability. Daily quantities are generated from a seeded
    noise source, then represented by valid orders, matching payments, and one
    inventory state update per product-day. Optional edge cases reuse valid
    order identities to exercise duplicate and conflict classification; the
    CLI separately publishes one malformed envelope for quarantine evidence.
    """
    if days < 28 or days > 120:
        raise ValueError("forecasting history days must be between 28 and 120")
    weekly = {
        "weekday": (0.75, 0.9, 1.0, 1.1, 1.35, 1.7, 1.4),
        "weekend": (0.55, 0.65, 0.75, 0.9, 1.2, 1.9, 2.2),
        "steady": (1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0),
        "payday": (1.0, 1.0, 1.0, 1.0, 1.15, 1.25, 1.05),
    }
    currencies = ("USD", "EUR", "GBP")
    events: list[CommerceEvent] = []
    orders: list[OrderCreatedEvent] = []
    sold: dict[str, int] = {product.product_id: 0 for product in FORECASTING_CATALOG}
    sequence = 0
    for day in range(days):
        moment = FORECASTING_TIMELINE_ORIGIN + timedelta(days=day)
        daily_sold: dict[str, int] = {product.product_id: 0 for product in FORECASTING_CATALOG}
        for product_index, product in enumerate(FORECASTING_CATALOG):
            rng = random.Random(seed + (day * 1_009) + (product_index * 9_176))
            weekly_factor = weekly[product.pattern][moment.weekday()]
            noise = max(0.45, min(1.8, rng.gauss(1.0, 0.16)))
            intermittent = rng.random() < product.intermittent_probability
            demand = (
                0
                if intermittent
                else max(1, int(round(product.base_units * weekly_factor * noise)))
            )
            if demand == 0:
                continue
            chunks = 2 if demand >= 8 and rng.random() < 0.28 else 1
            remaining = demand
            for part in range(chunks):
                quantity = remaining if part == chunks - 1 else max(1, remaining // 2)
                remaining -= quantity
                order_id = f"ord_fcst{seed % 100000:05d}{day:03d}{product_index:02d}{part}"
                customer_id = f"cus_fcst{(day * 17 + product_index * 7 + part) % 64:08d}"
                currency = currencies[(seed + day + product_index) % len(currencies)]
                occurred_at = moment + timedelta(hours=8 + (product_index % 8), minutes=part * 7)
                total = order_total(((quantity, product.unit_price),))
                order = OrderCreatedEvent(
                    event_id=_forecast_id(seed, "order", sequence),
                    event_type=ORDER_CREATED,
                    schema_version=SCHEMA_VERSION,
                    occurred_at=occurred_at,
                    produced_at=occurred_at + timedelta(minutes=12),
                    source=FORECASTING_SOURCE,
                    payload=OrderCreatedPayload(
                        order_id=order_id,
                        customer_id=customer_id,
                        items=[
                            OrderItem(
                                product_id=product.product_id,
                                quantity=quantity,
                                unit_price=product.unit_price,
                            )
                        ],
                        currency=currency,
                        total_amount=total,
                    ),
                )
                payment = PaymentCompletedEvent(
                    event_id=_forecast_id(seed, "payment", sequence),
                    event_type=PAYMENT_COMPLETED,
                    schema_version=SCHEMA_VERSION,
                    occurred_at=occurred_at + timedelta(minutes=5),
                    produced_at=occurred_at + timedelta(minutes=17),
                    source=FORECASTING_SOURCE,
                    payload=PaymentCompletedPayload(
                        payment_id=f"pay_fcst{seed % 100000:05d}{day:03d}{product_index:02d}{part}",
                        order_id=order_id,
                        customer_id=customer_id,
                        currency=currency,
                        amount=total,
                        method=("card", "wallet", "bank_transfer")[
                            (day + product_index + part) % 3
                        ],
                    ),
                )
                events.extend((order, payment))
                orders.append(order)
                sold[product.product_id] += quantity
                daily_sold[product.product_id] += quantity
                sequence += 1
        for product_index, product in enumerate(FORECASTING_CATALOG):
            quantity_on_hand = max(0, 1_200 - sold[product.product_id])
            occurred_at = moment + timedelta(hours=6, minutes=product_index)
            events.append(
                InventoryUpdatedEvent(
                    event_id=_forecast_id(
                        seed,
                        "inventory",
                        day * len(FORECASTING_CATALOG) + product_index,
                    ),
                    event_type=INVENTORY_UPDATED,
                    schema_version=SCHEMA_VERSION,
                    occurred_at=occurred_at,
                    produced_at=occurred_at + timedelta(minutes=3),
                    source=FORECASTING_SOURCE,
                    payload=InventoryUpdatedPayload(
                        product_id=product.product_id,
                        warehouse_id="wh_fcst01",
                        quantity_on_hand=quantity_on_hand,
                        delta=-daily_sold[product.product_id],
                    ),
                )
            )
    if include_edge_cases and len(orders) >= 2:
        duplicate = orders[0].model_copy(update={"event_id": _forecast_id(seed, "duplicate", 0)})
        events.append(duplicate)
        original = orders[1]
        item = original.payload.items[0]
        conflict_quantity = item.quantity + 1
        conflict_total = order_total(((conflict_quantity, item.unit_price),))
        conflict_payload = OrderCreatedPayload(
            order_id=original.payload.order_id,
            customer_id=original.payload.customer_id,
            items=[
                OrderItem(
                    product_id=item.product_id,
                    quantity=conflict_quantity,
                    unit_price=item.unit_price,
                )
            ],
            currency=original.payload.currency,
            total_amount=conflict_total,
        )
        events.append(
            OrderCreatedEvent(
                event_id=_forecast_id(seed, "conflict", 0),
                event_type=ORDER_CREATED,
                schema_version=SCHEMA_VERSION,
                occurred_at=original.occurred_at + timedelta(seconds=1),
                produced_at=original.produced_at + timedelta(seconds=1),
                source=FORECASTING_SOURCE,
                payload=conflict_payload,
            )
        )
    return events


def forecasting_quarantine_fixture(seed: int) -> dict[str, object]:
    """Return one deterministic invalid order envelope for the quarantine path."""
    event_id = _forecast_id(seed, "quarantine", 0)
    return {
        "event_id": str(event_id),
        "event_type": ORDER_CREATED,
        "schema_version": SCHEMA_VERSION,
        "occurred_at": (FORECASTING_TIMELINE_ORIGIN + timedelta(days=1)).isoformat(),
        "produced_at": (FORECASTING_TIMELINE_ORIGIN + timedelta(days=1, minutes=1)).isoformat(),
        "source": FORECASTING_SOURCE,
        "payload": {
            "order_id": f"ord_bad{seed % 100000:05d}",
            "customer_id": f"cus_bad{seed % 100000:05d}",
            "items": [
                {"product_id": "prd_fcst0001", "quantity": 0, "unit_price": "18.50"}
            ],
            "currency": "USD",
            "total_amount": "0.00",
        },
    }


def _history_id(seed: int, kind: str, index: int) -> uuid.UUID:
    return uuid.uuid5(EVENT_ID_NAMESPACE, f"history:{seed}:{kind}:{index}")


def _forecast_id(seed: int, kind: str, index: int) -> uuid.UUID:
    return uuid.uuid5(EVENT_ID_NAMESPACE, f"forecasting:{seed}:{kind}:{index}")
