from event_generator.generator import generate_orders
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


def test_different_seeds_change_orders() -> None:
    left = generate_orders(3, seed=1, deterministic_event_ids=True)
    right = generate_orders(3, seed=2, deterministic_event_ids=True)
    assert [event.payload.order_id for event in left] != [event.payload.order_id for event in right]
