"""Day 2 contracts, CSV conversion, classification, REST, and replay."""

from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from event_generator.generator import generate_orders
from ingestion.parsing import parse_record
from ingestion.positions import KafkaPosition
from ingestion.store import PersistOutcome, SqlEventStore
from titan_api.config import ApiSettings
from titan_api.csv_import import CSV_HEADERS, parse_csv_bytes
from titan_api.main import create_app
from titan_contracts.events import OrderCreatedEvent
from titan_contracts.money import line_amount
from titan_messaging.producer import PublishError, PublishResult
from titan_store.db import create_db_engine, create_session_factory
from titan_store.models import Base, CommerceEvent, QuarantineRecord


def _order(seed: int = 4) -> OrderCreatedEvent:
    return generate_orders(1, seed=seed, deterministic_event_ids=True)[0]


def _clone(event: OrderCreatedEvent, *, quantity: int | None = None) -> OrderCreatedEvent:
    body = event.model_dump(mode="json")
    body["event_id"] = str(uuid4())
    if quantity is not None:
        price = Decimal(body["payload"]["items"][0]["unit_price"])
        body["payload"]["items"][0]["quantity"] = quantity
        body["payload"]["total_amount"] = format(line_amount(quantity, price), "f")
    return OrderCreatedEvent.model_validate(body)


def _csv(*rows: str) -> bytes:
    return ("\n".join((",".join(CSV_HEADERS), *rows)) + "\n").encode()


def test_schema_version_two_is_rejected_as_unsupported() -> None:
    try:
        parse_record(b'{"schema_version": 2, "event_type": "order_created"}')
    except Exception as exc:
        assert exc.error_type == "unsupported_schema_version"  # type: ignore[attr-defined]
    else:
        raise AssertionError("schema version 2 was accepted")


def test_csv_accepts_an_order_and_rejects_a_bad_row() -> None:
    good = (
        "order_created,2026-01-01T00:00:00Z,ord_csvorder01,cus_csvcustomer,"
        "USD,prd_csvproduct1,2,10.00,,,,,,"
    )
    bad = (
        "order_created,2026-01-01T00:00:00Z,not-an-order,cus_csvcustomer,"
        "USD,prd_csvproduct1,1,10.00,,,,,,"
    )
    parsed = parse_csv_bytes(
        _csv(good, bad),
        source="csv.import.test",
        produced_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    assert isinstance(parsed[0].event, OrderCreatedEvent)
    assert parsed[0].event.payload.total_amount == Decimal("20.00")
    assert parsed[1].event is None
    assert parsed[1].error is not None


def test_csv_upload_publishes_valid_rows_and_keeps_rejected_rows() -> None:
    app = create_app(ApiSettings(database_url="sqlite://", cors_origins="http://localhost:3000"))
    Base.metadata.create_all(app.state.engine)
    good = (
        "order_created,2026-01-01T00:00:00Z,ord_csvorder01,cus_csvcustomer,"
        "USD,prd_csvproduct1,2,10.00,,,,,,"
    )
    bad = (
        "order_created,2026-01-01T00:00:00Z,not-an-order,cus_csvcustomer,"
        "USD,prd_csvproduct1,1,10.00,,,,,,"
    )

    def publish(value: OrderCreatedEvent) -> PublishResult:
        return PublishResult(
            value.event_id, value.payload.order_id, "titan.commerce.order-events", 1, 1
        )

    app.state.publish = publish
    response = TestClient(app).post(
        "/api/v1/ingestion/imports",
        files={"file": ("orders.csv", _csv(good, bad), "text/csv")},
    )
    assert response.status_code == 202
    detail = TestClient(app).get(f"/api/v1/ingestion/imports/{response.json()['import_id']}")
    statuses = {row["status"] for row in detail.json()["rows"]}
    assert statuses == {"published", "rejected"}


def test_same_order_id_with_a_different_payload_is_a_conflict() -> None:
    engine = create_db_engine("sqlite://")
    Base.metadata.create_all(engine)
    store = SqlEventStore(create_session_factory(engine))
    first = _order()
    now = datetime.now(UTC)
    assert (
        store.persist(
            first, KafkaPosition("titan.commerce.order-events", 0, 1, first.payload.order_id), now
        )
        is PersistOutcome.INSERTED
    )
    duplicate = _clone(first)
    conflict = _clone(first, quantity=2)
    assert (
        store.persist(
            duplicate,
            KafkaPosition("titan.commerce.order-events", 0, 2, duplicate.payload.order_id),
            now,
        )
        is PersistOutcome.DUPLICATE_OPERATION
    )
    assert (
        store.persist(
            conflict,
            KafkaPosition("titan.commerce.order-events", 0, 3, conflict.payload.order_id),
            now,
        )
        is PersistOutcome.CONFLICT
    )
    with Session(engine) as session:
        accepted = session.query(CommerceEvent).filter_by(disposition="accepted").count()
    assert accepted == 1


def test_rest_publish_is_not_reported_as_persisted() -> None:
    app = create_app(ApiSettings(database_url="sqlite://", cors_origins="http://localhost:3000"))
    Base.metadata.create_all(app.state.engine)
    event = _order()

    def publish(value: OrderCreatedEvent) -> PublishResult:
        return PublishResult(
            event.event_id,
            event.payload.order_id,
            "titan.commerce.order-events",
            0,
            4,
        )

    app.state.publish = publish
    response = TestClient(app).post("/api/v1/ingestion/events", json=event.model_dump(mode="json"))
    assert response.status_code == 202
    body = response.json()
    assert body["persisted"] is False
    assert body["status"] == "published_to_kafka"
    assert body["offset"] == 4


def test_invalid_rest_payload_is_rejected() -> None:
    app = create_app(ApiSettings(database_url="sqlite://", cors_origins="http://localhost:3000"))
    Base.metadata.create_all(app.state.engine)
    response = TestClient(app).post(
        "/api/v1/ingestion/events", json={"event_type": "order_created"}
    )
    assert response.status_code == 422
    listed = TestClient(app).get("/api/v1/ingestion/submissions")
    assert listed.json()["items"][0]["status"] == "rejected"


def test_topic_setup_failure_is_reported_as_a_broker_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(_servers: str, *, attempts: int = 5) -> None:
        raise RuntimeError("broker down")

    monkeypatch.setattr("titan_api.main.ensure_commerce_topics", fail)
    app = create_app(ApiSettings(database_url="sqlite://", cors_origins="http://localhost:3000"))
    Base.metadata.create_all(app.state.engine)
    response = TestClient(app).post(
        "/api/v1/ingestion/events", json=_order().model_dump(mode="json")
    )
    assert response.status_code == 503
    assert "not persisted" in response.json()["detail"]
    listed = TestClient(app).get("/api/v1/ingestion/submissions")
    assert listed.json()["items"][0]["status"] == "publish_failed"


def test_broker_failure_does_not_claim_persistence() -> None:
    app = create_app(ApiSettings(database_url="sqlite://", cors_origins="http://localhost:3000"))
    Base.metadata.create_all(app.state.engine)
    event = _order()

    def publish(_value: OrderCreatedEvent) -> PublishResult:
        raise PublishError("broker down")

    app.state.publish = publish
    response = TestClient(app).post("/api/v1/ingestion/events", json=event.model_dump(mode="json"))
    assert response.status_code == 503
    assert "not persisted" in response.json()["detail"]
    with Session(app.state.engine) as session:
        assert session.query(CommerceEvent).count() == 0


def test_malformed_quarantine_replay_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    app = create_app(ApiSettings(database_url="sqlite://", cors_origins="http://localhost:3000"))
    Base.metadata.create_all(app.state.engine)
    called = False

    def publish(_value: OrderCreatedEvent) -> PublishResult:
        nonlocal called
        called = True
        raise AssertionError("refused replay must not publish")

    app.state.publish = publish
    with Session(app.state.engine) as session:
        session.add(
            QuarantineRecord(
                received_at=datetime.now(UTC),
                kafka_topic="titan.commerce.order-events",
                kafka_partition=0,
                kafka_offset=9,
                raw_value="not-json",
                error_type="malformed_json",
                error_detail="bad",
                replay_status="not_replayable",
            )
        )
        session.commit()
        quarantine_id = session.query(QuarantineRecord.id).one()[0]
    monkeypatch.setenv("TITAN_ADMIN_TOKEN", "unit-admin-token")
    response = TestClient(app).post(
        "/api/v1/ingestion/replays",
        json={"quarantine_ids": [quarantine_id]},
            headers={"Authorization": "Bearer unit-admin-token"},
    )
    assert response.status_code == 200
    item = response.json()["items"][0]
    assert item["outcome"] == "refused"
    assert item["downstream_state"] == "refused"
    assert called is False


def test_exact_replay_of_a_persisted_event_does_not_add_a_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = create_app(ApiSettings(database_url="sqlite://", cors_origins="http://localhost:3000"))
    Base.metadata.create_all(app.state.engine)
    event = _order()
    with Session(app.state.engine) as session:
        session.add(
            CommerceEvent(
                event_id=event.event_id,
                event_type=event.event_type,
                schema_version=event.schema_version,
                order_id=event.payload.order_id,
                business_key=event.payload.order_id,
                disposition="accepted",
                payload_fingerprint="abc",
                occurred_at=event.occurred_at,
                ingested_at=datetime.now(UTC),
                source=event.source,
                event_envelope=event.model_dump(mode="json"),
                kafka_topic="titan.commerce.order-events",
                kafka_partition=0,
                kafka_offset=1,
                kafka_key=event.payload.order_id,
            )
        )
        session.commit()

    def publish(value: OrderCreatedEvent) -> PublishResult:
        assert value.event_id == event.event_id
        return PublishResult(
            event.event_id, event.payload.order_id, "titan.commerce.order-events", 0, 8
        )

    app.state.publish = publish
    monkeypatch.setenv("TITAN_ADMIN_TOKEN", "unit-admin-token")
    response = TestClient(app).post(
        "/api/v1/ingestion/replays",
        json={"event_ids": [str(event.event_id)]},
            headers={"Authorization": "Bearer unit-admin-token"},
    )
    item = response.json()["items"][0]
    assert item["outcome"] == "published"
    assert item["downstream_state"] == "persisted"
    with Session(app.state.engine) as session:
        assert session.query(CommerceEvent).count() == 1
