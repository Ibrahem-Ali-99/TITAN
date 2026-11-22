import json
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from pydantic import ValidationError

from titan_contracts.events import OrderCreatedEvent
from titan_contracts.identifiers import ORDER_CREATED, SCHEMA_VERSION, SYNTHETIC_SOURCE
from titan_contracts.orders import OrderCreatedPayload, OrderItem


def _item(price: str = "10.00", quantity: int = 2) -> OrderItem:
    return OrderItem(product_id="prd_100000000001", quantity=quantity, unit_price=Decimal(price))


def _payload(**overrides: object) -> OrderCreatedPayload:
    item = _item()
    data: dict[str, object] = {
        "order_id": "ord_abc123def456",
        "customer_id": "cus_abc123def456",
        "items": [item],
        "currency": "USD",
        "total_amount": item.amount,
    }
    data.update(overrides)
    return OrderCreatedPayload.model_validate(data)


def _event(payload: OrderCreatedPayload | None = None) -> OrderCreatedEvent:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    return OrderCreatedEvent(
        event_id=uuid4(),
        event_type=ORDER_CREATED,
        schema_version=SCHEMA_VERSION,
        occurred_at=now,
        produced_at=now,
        source=SYNTHETIC_SOURCE,
        payload=payload or _payload(),
    )


def test_valid_order_accepts_exact_total() -> None:
    payload = _payload()
    assert payload.total_amount == Decimal("20.00")
    event = _event(payload)
    restored = OrderCreatedEvent.model_validate_json(event.model_dump_json())
    assert restored.payload.total_amount == payload.total_amount
    assert restored.occurred_at.tzinfo is not None


def test_money_is_serialized_as_decimal_strings() -> None:
    encoded = json.loads(_event().model_dump_json())
    assert isinstance(encoded["payload"]["total_amount"], str)
    assert isinstance(encoded["payload"]["items"][0]["unit_price"], str)


def test_float_money_is_rejected() -> None:
    with pytest.raises(ValidationError):
        OrderItem.model_validate(
            {"product_id": "prd_100000000001", "quantity": 1, "unit_price": 1.25}
        )


def test_zero_and_negative_quantity_are_rejected() -> None:
    with pytest.raises(ValidationError):
        _item(quantity=0)
    with pytest.raises(ValidationError):
        OrderItem(product_id="prd_100000000001", quantity=-1, unit_price=Decimal("1.00"))


def test_extra_scale_and_non_positive_money_are_rejected() -> None:
    with pytest.raises(ValidationError):
        _item(price="1.005")
    with pytest.raises(ValidationError):
        _item(price="0.00")
    with pytest.raises(ValidationError):
        _item(price="-1.00")


def test_total_must_match_lines() -> None:
    with pytest.raises(ValidationError, match="does not equal"):
        _payload(total_amount=Decimal("1.00"))


def test_unsupported_currency_and_naive_timestamp_are_rejected() -> None:
    with pytest.raises(ValidationError):
        _payload(currency="JPY")
    payload = _payload()
    with pytest.raises(ValidationError):
        OrderCreatedEvent(
            event_id=uuid4(),
            event_type=ORDER_CREATED,
            schema_version=SCHEMA_VERSION,
            occurred_at=datetime(2026, 1, 1),
            produced_at=datetime(2026, 1, 1, tzinfo=UTC),
            source=SYNTHETIC_SOURCE,
            payload=payload,
        )


def test_unknown_schema_version_and_extra_fields_are_rejected() -> None:
    event = _event()
    body = json.loads(event.model_dump_json())
    body["schema_version"] = 2
    with pytest.raises(ValidationError):
        OrderCreatedEvent.model_validate(body)
    body = json.loads(event.model_dump_json())
    body["unexpected"] = True
    with pytest.raises(ValidationError):
        OrderCreatedEvent.model_validate(body)
