"""Publication selection and lake status. Spark itself is exercised in the image."""

from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from titan_api.config import ApiSettings
from titan_api.main import create_app
from titan_contracts.topics import COMMERCE_TOPICS
from titan_spark.policy import (
    batch_keeps_accepted_order,
    current_publication,
    logical_order,
    streaming_keeps_event,
)
from titan_spark.topics import COMMERCE_TOPIC_NAMES
from titan_store.models import Base, LakePublication, SparkJobStatus


def test_spark_topics_match_the_contract_catalog() -> None:
    assert tuple(spec.name for spec in COMMERCE_TOPICS) == COMMERCE_TOPIC_NAMES


def test_watermark_does_not_remove_an_accepted_order_from_batch_totals() -> None:
    event_time = datetime(2026, 1, 1, 12, 40, tzinfo=UTC)
    watermark = datetime(2026, 1, 1, 13, 20, tzinfo=UTC)
    assert streaming_keeps_event(event_time, watermark) is False
    assert batch_keeps_accepted_order("order_created", "accepted", event_time, watermark) is True
    assert logical_order("order_created", "conflict") is False


def test_failed_publication_does_not_become_current() -> None:
    older = datetime(2026, 1, 1, tzinfo=UTC)
    newer = datetime(2026, 1, 2, tzinfo=UTC)
    selected = current_publication(
        [
            {"publication_id": "old", "status": "published", "finished_at": older},
            {"publication_id": "bad", "status": "failed", "finished_at": newer},
            {"publication_id": "staging", "status": "staging", "finished_at": None},
        ]
    )
    assert selected is not None
    assert selected["publication_id"] == "old"


def test_lake_status_uses_recorded_rows_and_marks_a_stale_job() -> None:
    app = create_app(ApiSettings(database_url="sqlite://", cors_origins="http://localhost:3000"))
    Base.metadata.create_all(app.state.engine)
    empty = TestClient(app).get("/api/v1/lake/status")
    assert empty.status_code == 200
    assert empty.json()["current_publication"] is None
    assert empty.json()["spark_jobs"] == []

    now = datetime.now(UTC)
    with Session(app.state.engine) as session:
        session.add(
            LakePublication(
                publication_id="published-1",
                status="published",
                version_uri="s3a://titan-lake/silver/versions/published-1",
                result_kind="canonical_batch",
                started_at=now - timedelta(hours=2),
                finished_at=now - timedelta(hours=1),
                silver_order_count=3,
                accepted_revenue_json='{"USD": "10.00"}',
            )
        )
        session.add(
            LakePublication(
                publication_id="failed-1",
                status="failed",
                version_uri="s3a://titan-lake/silver/versions/failed-1",
                result_kind="canonical_batch",
                started_at=now,
                finished_at=now,
                error_detail="validation failed",
            )
        )
        session.add(
            SparkJobStatus(
                job_name="bronze_stream",
                state="running",
                updated_at=now - timedelta(minutes=10),
                detail="old heartbeat",
            )
        )
        session.commit()

    body = TestClient(app).get("/api/v1/lake/status").json()
    assert body["current_publication"]["publication_id"] == "published-1"
    assert body["current_publication"]["accepted_revenue"] == {"USD": "10.00"}
    assert body["failed_publications"] == 1
    assert body["spark_jobs"][0]["display_state"] == "stale"
