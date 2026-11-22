"""Lake status from recorded publications. This does not scan MinIO."""

import json
from datetime import UTC, datetime

from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import Session

from titan_store.models import LakePublication, SparkJobStatus

STALE_AFTER_SECONDS = 90

DEFINITIONS = {
    "spark_jobs": (
        "Rows in spark_job_status. display_state is derived from state and updated_at. "
        "A running container that has not written this table is not_reported."
    ),
    "current_publication": (
        "The lake_publications row with status published and the latest finished_at. "
        "Staging and failed rows are not current. Counts are the values validated "
        "when that version was published, not a live object-store scan."
    ),
    "failed_publications": "Count of lake_publications rows with status failed.",
    "silver_order_count": (
        "Accepted order_created rows written into the current Silver version. "
        "Conflicts and duplicate operations are excluded."
    ),
    "accepted_revenue": (
        "Sum of accepted order total_amount grouped by currency in the current version. "
        "Currencies are not converted or added together."
    ),
}


class SparkJobView(BaseModel):
    job_name: str
    state: str
    display_state: str
    last_batch_id: int | None
    last_offsets: str | None
    detail: str | None
    updated_at: datetime
    error_detail: str | None


class PublicationView(BaseModel):
    publication_id: str
    version_uri: str
    finished_at: datetime
    silver_order_count: int | None
    silver_payment_count: int | None
    silver_inventory_count: int | None
    order_metric_rows: int | None
    bronze_record_count: int | None
    bronze_distinct_offsets: int | None
    pending_event_count: int | None
    identifier_conflict_count: int | None
    accepted_revenue: dict[str, str]
    max_occurred_at: datetime | None
    result_kind: str


class LakeStatus(BaseModel):
    spark_jobs: list[SparkJobView]
    current_publication: PublicationView | None
    failed_publications: int
    metric_definitions: dict[str, str]


def display_state(state: str | None, updated_at: datetime | None, now: datetime) -> str:
    if state is None or updated_at is None:
        return "not_reported"
    if state == "failed":
        return "failed"
    if state == "succeeded":
        return "succeeded"
    if state == "running":
        age = (now - updated_at).total_seconds()
        if age > STALE_AFTER_SECONDS:
            return "stale"
        return "running"
    return "not_reported"


def load_lake_status(session: Session, now: datetime | None = None) -> LakeStatus:
    moment = now or datetime.now(UTC)
    jobs = session.scalars(select(SparkJobStatus).order_by(SparkJobStatus.job_name)).all()
    published = session.scalars(
        select(LakePublication)
        .where(LakePublication.status == "published")
        .order_by(LakePublication.finished_at.desc())
    ).all()
    failed = session.scalars(
        select(LakePublication).where(LakePublication.status == "failed")
    ).all()
    current = next((row for row in published if row.finished_at is not None), None)
    return LakeStatus(
        spark_jobs=[_job(row, moment) for row in jobs],
        current_publication=None if current is None else _publication(current),
        failed_publications=len(failed),
        metric_definitions=DEFINITIONS,
    )


def _job(row: SparkJobStatus, now: datetime) -> SparkJobView:
    return SparkJobView(
        job_name=row.job_name,
        state=row.state,
        display_state=display_state(row.state, row.updated_at, now),
        last_batch_id=row.last_batch_id,
        last_offsets=row.last_offsets,
        detail=row.detail,
        updated_at=row.updated_at,
        error_detail=row.error_detail,
    )


def _publication(row: LakePublication) -> PublicationView:
    if row.finished_at is None:
        raise ValueError("a published row needs finished_at")
    revenue: dict[str, str] = {}
    if row.accepted_revenue_json:
        parsed = json.loads(row.accepted_revenue_json)
        if isinstance(parsed, dict):
            revenue = {str(key): str(value) for key, value in parsed.items()}
    return PublicationView(
        publication_id=row.publication_id,
        version_uri=row.version_uri,
        finished_at=row.finished_at,
        silver_order_count=row.silver_order_count,
        silver_payment_count=row.silver_payment_count,
        silver_inventory_count=row.silver_inventory_count,
        order_metric_rows=row.order_metric_rows,
        bronze_record_count=row.bronze_record_count,
        bronze_distinct_offsets=row.bronze_distinct_offsets,
        pending_event_count=row.pending_event_count,
        identifier_conflict_count=row.identifier_conflict_count,
        accepted_revenue=revenue,
        max_occurred_at=row.max_occurred_at,
        result_kind=row.result_kind,
    )
