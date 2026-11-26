from datetime import timedelta

from event_generator.generator import (
    FORECASTING_SOURCE,
    FORECASTING_TIMELINE_ORIGIN,
    TIMELINE_ORIGIN,
    generate_forecasting_history,
    generate_orders,
)
from titan_contracts.events import OrderCreatedEvent, PaymentCompletedEvent
from titan_contracts.identifiers import SYNTHETIC_SOURCE


def test_same_seed_reproduces_payloads_but_not_event_ids() -> None:
    first = generate_orders(4, seed=42)
    second = generate_orders(4, seed=42)
    assert [event.payload for event in first] == [event.payload for event in second]
    assert [event.occurred_at for event in first] == [event.occurred_at for event in second]
    assert [event.event_id for event in first] != [event.event_id for event in second]
    assert {event.source for event in first} == {SYNTHETIC_SOURCE}


def test_deterministic_ids_do_not_change_the_seeded_payload() -> None:
    random_ids = generate_orders(3, seed=7)
    stable_a = generate_orders(3, seed=7, deterministic_event_ids=True)
    stable_b = generate_orders(3, seed=7, deterministic_event_ids=True)
    assert [event.payload for event in random_ids] == [event.payload for event in stable_a]
    assert [event.event_id for event in stable_a] == [event.event_id for event in stable_b]
    assert stable_a[0].produced_at == stable_b[0].produced_at


def test_span_and_late_events_keep_the_seeded_payload() -> None:
    plain = generate_orders(4, seed=9, deterministic_event_ids=True)
    spanned = generate_orders(
        4,
        seed=9,
        deterministic_event_ids=True,
        occurred_span=timedelta(hours=48),
        late_every=4,
        late_by=timedelta(hours=20),
    )
    assert [event.payload for event in plain] == [event.payload for event in spanned]
    assert plain[3].occurred_at == TIMELINE_ORIGIN + timedelta(seconds=3)
    assert spanned[0].occurred_at == TIMELINE_ORIGIN
    assert spanned[3].occurred_at < spanned[2].occurred_at


def test_different_seeds_change_orders() -> None:
    left = generate_orders(3, seed=1, deterministic_event_ids=True)
    right = generate_orders(3, seed=2, deterministic_event_ids=True)
    assert [event.payload.order_id for event in left] != [event.payload.order_id for event in right]


def test_forecasting_profile_is_reproducible_and_varied() -> None:
    first = generate_forecasting_history(42, seed=61226)
    second = generate_forecasting_history(42, seed=61226)
    assert [event.model_dump(mode="json") for event in first] == [
        event.model_dump(mode="json") for event in second
    ]
    assert {event.source for event in first} == {FORECASTING_SOURCE}
    orders = [event for event in first if isinstance(event, OrderCreatedEvent)]
    payments = [event for event in first if isinstance(event, PaymentCompletedEvent)]
    assert len(orders) > 300
    assert len(payments) == len(orders) - 2
    assert len({item.payload.items[0].product_id for item in orders}) == 12
    assert {item.payload.currency for item in orders} == {"USD", "EUR", "GBP"}
    dates = {item.occurred_at.date() for item in orders}
    assert min(dates) == FORECASTING_TIMELINE_ORIGIN.date()
    assert max(dates) == FORECASTING_TIMELINE_ORIGIN.date() + timedelta(days=41)
    order_ids = [item.payload.order_id for item in orders]
    assert len(order_ids) > len(set(order_ids))  # one duplicate and one conflict fixture
    assert len({item.payload.total_amount for item in orders}) > 20
