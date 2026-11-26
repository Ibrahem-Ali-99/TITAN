from datetime import UTC, datetime, timedelta
from uuid import uuid4

from fastapi.testclient import TestClient
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from event_generator.generator import generate_orders
from titan_api.config import ApiSettings
from titan_api.main import create_app
from titan_api.queries import METRIC_DEFINITIONS, load_ingestion_stats
from titan_store.models import Base, CommerceEvent, PipelineRun


class BrokenSession:
    def execute(self, *_args: object, **_kwargs: object) -> None:
        raise OperationalError("SELECT 1", {}, Exception("down"))

    def scalar(self, *_args: object, **_kwargs: object) -> None:
        raise OperationalError("SELECT", {}, Exception("down"))

    def scalars(self, *_args: object, **_kwargs: object) -> None:
        raise OperationalError("SELECT", {}, Exception("down"))

    def get_bind(self) -> None:
        raise OperationalError("bind", {}, Exception("down"))

    def close(self) -> None:
        return None


def _client(url: str = "sqlite://") -> TestClient:
    app = create_app(ApiSettings(database_url=url, cors_origins="http://localhost:3000"))
    return TestClient(app)


def test_monitoring_reads_legacy_durations_without_updating_history() -> None:
    app = create_app(ApiSettings(database_url="sqlite://"))
    engine = app.state.engine
    Base.metadata.create_all(engine)
    started = datetime.now(UTC)
    with Session(engine) as session:
        legacy = PipelineRun(
            job_name="legacy",
            state="success",
            started_at=started,
            finished_at=started + timedelta(microseconds=2500),
        )
        session.add_all(
            [
                legacy,
                PipelineRun(
                    job_name="recorded",
                    state="success",
                    started_at=started,
                    finished_at=started + timedelta(seconds=1),
                    duration_ms=900,
                ),
                PipelineRun(job_name="in_progress", state="running", started_at=started),
            ]
        )
        session.commit()
        legacy_id = legacy.id

    client = TestClient(app)
    assert client.get("/api/v1/monitoring/overview").status_code == 200
    for endpoint, key in (("runs", "runs"), ("jobs", "jobs")):
        response = client.get(f"/api/v1/monitoring/{endpoint}")
        assert response.status_code == 200
        assert {row["job_name"]: row["duration_ms"] for row in response.json()[key]} == {
            "legacy": 3,
            "recorded": 900,
            "in_progress": None,
        }

    with Session(engine) as session:
        row = session.get(PipelineRun, legacy_id)
        assert row is not None
        assert row.duration_ms is None


def test_liveness_does_not_require_the_database() -> None:
    app = create_app(ApiSettings(database_url="sqlite://", cors_origins="http://localhost:3000"))

    def broken() -> object:
        yield BrokenSession()
        return None

    app.dependency_overrides[__import__("titan_api.main", fromlist=["get_session"]).get_session] = (
        broken
    )
    response = TestClient(app).get("/health/live")
    assert response.status_code == 200
    assert response.json() == {"status": "live"}


def test_failed_stats_query_is_an_error_not_zeros() -> None:
    app = create_app(ApiSettings(database_url="sqlite://", cors_origins="http://localhost:3000"))
    from titan_api.main import get_session

    def broken() -> object:
        yield BrokenSession()
        return None

    app.dependency_overrides[get_session] = broken
    client = TestClient(app)
    ready = client.get("/health/ready")
    stats = client.get("/api/v1/ingestion/stats")
    recent = client.get("/api/v1/events/recent")
    assert ready.status_code == 503
    assert stats.status_code == 503
    assert recent.status_code == 503
    assert "persisted_events" not in stats.text
    assert "0" not in stats.json()["detail"]


def test_ready_requires_migrated_tables_and_stats_read_real_rows() -> None:
    app = create_app(ApiSettings(database_url="sqlite://", cors_origins="http://localhost:3000"))
    client = TestClient(app)
    assert client.get("/health/ready").status_code == 503
    assert "schema is not migrated" in client.get("/health/ready").text

    Base.metadata.create_all(app.state.engine)
    assert client.get("/health/ready").json()["checks"]["postgres"] == "ok"
    warehouse = client.get("/api/v1/warehouse/status")
    assert warehouse.status_code == 200
    assert warehouse.json()["ready"] is False
    assert client.get("/api/v1/analytics/overview").status_code == 503
    empty = client.get("/api/v1/ingestion/stats")
    assert empty.status_code == 200
    assert empty.json()["persisted_events"] == 0
    assert "not Kafka broker throughput" in empty.json()["metric_definitions"]["persisted_events"]

    event = generate_orders(1, seed=21, deterministic_event_ids=True)[0]
    older = datetime.now(UTC) - timedelta(hours=3)
    recent = datetime.now(UTC)
    with Session(app.state.engine) as session:
        session.add(
            CommerceEvent(
                event_id=event.event_id,
                event_type=event.event_type,
                schema_version=event.schema_version,
                order_id=event.payload.order_id,
                occurred_at=event.occurred_at,
                ingested_at=recent,
                source=event.source,
                event_envelope=event.model_dump(mode="json"),
                kafka_topic="titan.commerce.order-events",
                kafka_partition=0,
                kafka_offset=1,
                kafka_key=event.payload.order_id,
            )
        )
        session.add(
            CommerceEvent(
                event_id=uuid4(),
                event_type="order_created",
                schema_version=1,
                order_id="ord_ffffeeeedddd",
                occurred_at=older,
                ingested_at=older,
                source="synthetic.event-generator",
                event_envelope={"payload": {"currency": "EUR", "total_amount": "5.00"}},
                kafka_topic="titan.commerce.order-events",
                kafka_partition=0,
                kafka_offset=2,
                kafka_key="ord_ffffeeeedddd",
            )
        )
        session.commit()
        stats = load_ingestion_stats(session, now=datetime.now(UTC))

    assert stats.persisted_events == 2
    assert stats.persisted_order_created_events == 2
    assert stats.quarantined_records == 0
    assert len(stats.recent_activity) == 1
    assert stats.metric_definitions == METRIC_DEFINITIONS

    body = client.get("/api/v1/events/recent?limit=1")
    assert body.status_code == 200
    parsed = body.json()
    assert parsed["limit"] == 1
    assert parsed["returned"] == 1
    assert parsed["items"][0]["synthetic"] is True
    assert client.get("/api/v1/events/recent?limit=101").status_code == 422
    assert client.get("/api/v1/events/recent?limit=0").status_code == 422
