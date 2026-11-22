"""Read quality, forecast, and monitoring rows. These routes do not train or publish."""

import hashlib
import json
from datetime import UTC, datetime
from typing import Any

from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, TopicPartition
from pydantic import BaseModel
from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from titan_api.config import ApiSettings
from titan_contracts.topics import CONSUMER_GROUP
from titan_store.models import (
    CommerceEvent,
    LakePublication,
    MlAnomaly,
    MlPrediction,
    MlRun,
    PipelineRun,
    QualityFinding,
    QualityRun,
    SparkJobStatus,
    WarehouseLoad,
)


class QualitySummary(BaseModel):
    available: bool
    reason: str | None
    latest_run_id: str | None
    latest_status: str | None
    warehouse_publication_id: str | None
    failure_count: int | None
    injected: str | None
    finished_at: datetime | None


class QualityRunView(BaseModel):
    run_id: str
    status: str
    warehouse_publication_id: str
    silver_publication_id: str | None
    failure_count: int
    injected: str | None
    finished_at: datetime | None


class QualityRuns(BaseModel):
    returned: int
    runs: list[QualityRunView]


class FindingView(BaseModel):
    id: int
    run_id: str
    check_id: str
    check_type: str
    dataset: str
    severity: str
    blocks_publication: bool
    observed: str
    expected: str
    passed: bool
    business_id: str | None
    detail: str


class FindingsResponse(BaseModel):
    returned: int
    findings: list[FindingView]


class ModelView(BaseModel):
    run_id: str
    status: str
    warehouse_publication_id: str
    algorithm: str
    serving_strategy: str | None
    decision_rationale: str | None
    feature_version: str
    train_start: str | None
    train_end: str | None
    test_start: str | None
    test_end: str | None
    metrics: dict[str, Any] | None
    artifact_uri: str | None
    trained_at: datetime
    detail: str | None


class ModelsResponse(BaseModel):
    available: bool
    reason: str | None
    models: list[ModelView]


class ForecastView(BaseModel):
    product_id: str
    target_date: str
    predicted_quantity: str
    actual_quantity: str | None
    kind: str


class ForecastsResponse(BaseModel):
    available: bool
    run_id: str | None
    returned: int
    forecasts: list[ForecastView]


class AnomalyView(BaseModel):
    product_id: str
    activity_date: str
    observed: str
    baseline: str
    score: str | None
    flagged: bool
    method: str
    detail: str


class AnomaliesResponse(BaseModel):
    available: bool
    run_id: str | None
    returned: int
    anomalies: list[AnomalyView]


class StageFreshness(BaseModel):
    stage: str
    observed_at: datetime | None
    age_seconds: int | None
    detail: str


class LagView(BaseModel):
    consumer_group: str
    available: bool
    total_lag: int | None
    detail: str


class MonitoringOverview(BaseModel):
    freshness: list[StageFreshness]
    ingestion_lag: LagView
    spark_progress: str | None
    latest_failure: str | None
    model_serving: str | None
    training_publication_id: str | None
    admin_auth: str


class RunView(BaseModel):
    id: int
    job_name: str
    state: str
    started_at: datetime
    finished_at: datetime | None
    duration_ms: int | None
    detail: str | None
    error_detail: str | None
    scope: str | None = None


class RunsResponse(BaseModel):
    returned: int
    runs: list[RunView]


class JobStatusResponse(BaseModel):
    returned: int
    jobs: list[RunView]


def quality_summary(session: Session) -> QualitySummary:
    run = session.scalar(select(QualityRun).order_by(QualityRun.finished_at.desc()).limit(1))
    if run is None:
        return QualitySummary(
            available=False,
            reason="no quality run has been recorded",
            latest_run_id=None,
            latest_status=None,
            warehouse_publication_id=None,
            failure_count=None,
            injected=None,
            finished_at=None,
        )
    return QualitySummary(
        available=True,
        reason=None,
        latest_run_id=run.run_id,
        latest_status=run.status,
        warehouse_publication_id=run.warehouse_publication_id,
        failure_count=run.failure_count,
        injected=run.injected,
        finished_at=run.finished_at,
    )


def quality_runs(session: Session, limit: int) -> QualityRuns:
    rows = session.scalars(
        select(QualityRun).order_by(QualityRun.finished_at.desc()).limit(limit)
    ).all()
    return QualityRuns(
        returned=len(rows),
        runs=[
            QualityRunView(
                run_id=row.run_id,
                status=row.status,
                warehouse_publication_id=row.warehouse_publication_id,
                silver_publication_id=row.silver_publication_id,
                failure_count=row.failure_count,
                injected=row.injected,
                finished_at=row.finished_at,
            )
            for row in rows
        ],
    )


def quality_findings(
    session: Session,
    limit: int,
    run_id: str | None,
    failed_only: bool,
) -> FindingsResponse:
    statement = select(QualityFinding).order_by(QualityFinding.id.desc())
    if run_id is not None:
        statement = statement.where(QualityFinding.run_id == run_id)
    if failed_only:
        statement = statement.where(QualityFinding.passed == 0)
    rows = session.scalars(statement.limit(limit)).all()
    return FindingsResponse(
        returned=len(rows),
        findings=[
            FindingView(
                id=row.id,
                run_id=row.run_id,
                check_id=row.check_id,
                check_type=row.check_type,
                dataset=row.dataset,
                severity=row.severity,
                blocks_publication=bool(row.blocks_publication),
                observed=row.observed,
                expected=row.expected,
                passed=bool(row.passed),
                business_id=row.business_id,
                detail=row.detail,
            )
            for row in rows
        ],
    )


def ml_models(session: Session, limit: int) -> ModelsResponse:
    rows = session.scalars(select(MlRun).order_by(MlRun.trained_at.desc()).limit(limit)).all()
    if not rows:
        return ModelsResponse(available=False, reason="no model run has been recorded", models=[])
    return ModelsResponse(available=True, reason=None, models=[_model(row) for row in rows])


def ml_forecasts(session: Session, limit: int, kind: str | None) -> ForecastsResponse:
    run = _promoted(session)
    if run is None:
        return ForecastsResponse(available=False, run_id=None, returned=0, forecasts=[])
    statement = select(MlPrediction).where(MlPrediction.run_id == run.run_id)
    if kind is not None:
        statement = statement.where(MlPrediction.kind == kind)
    rows = session.scalars(statement.order_by(MlPrediction.target_date).limit(limit)).all()
    return ForecastsResponse(
        available=True,
        run_id=run.run_id,
        returned=len(rows),
        forecasts=[
            ForecastView(
                product_id=row.product_id,
                target_date=row.target_date,
                predicted_quantity=row.predicted_quantity,
                actual_quantity=row.actual_quantity,
                kind=row.kind,
            )
            for row in rows
        ],
    )


def ml_anomalies(session: Session, limit: int, flagged_only: bool) -> AnomaliesResponse:
    run = _promoted(session)
    if run is None:
        return AnomaliesResponse(available=False, run_id=None, returned=0, anomalies=[])
    statement = select(MlAnomaly).where(MlAnomaly.run_id == run.run_id)
    if flagged_only:
        statement = statement.where(MlAnomaly.flagged == 1)
    rows = session.scalars(statement.order_by(MlAnomaly.id.desc()).limit(limit)).all()
    return AnomaliesResponse(
        available=True,
        run_id=run.run_id,
        returned=len(rows),
        anomalies=[
            AnomalyView(
                product_id=row.product_id,
                activity_date=row.activity_date,
                observed=row.observed,
                baseline=row.baseline,
                score=row.score,
                flagged=bool(row.flagged),
                method=row.method,
                detail=row.detail,
            )
            for row in rows
        ],
    )


def monitoring_overview(session: Session, settings: ApiSettings) -> MonitoringOverview:
    _backfill_durations(session)
    return MonitoringOverview(
        freshness=monitoring_freshness(session),
        ingestion_lag=_lag(settings.kafka_bootstrap_servers),
        spark_progress=_spark(session),
        latest_failure=_failure(session),
        model_serving=None if (served := _served(session)) is None else served.serving_strategy,
        training_publication_id=None if served is None else served.warehouse_publication_id,
        admin_auth=_admin_auth(),
    )


def monitoring_runs(session: Session, limit: int) -> RunsResponse:
    _backfill_durations(session)
    rows = session.scalars(select(PipelineRun).order_by(PipelineRun.id.desc()).limit(limit)).all()
    return RunsResponse(
        returned=len(rows),
        runs=[_run_view(row) for row in rows],
    )


def monitoring_jobs(session: Session) -> JobStatusResponse:
    """Latest recorded attempt for each job. This is not the execution history."""
    latest = select(func.max(PipelineRun.id).label("id")).group_by(PipelineRun.job_name).subquery()
    rows = session.scalars(
        select(PipelineRun)
        .join(latest, PipelineRun.id == latest.c.id)
        .order_by(PipelineRun.job_name)
    ).all()
    return JobStatusResponse(returned=len(rows), jobs=[_run_view(row) for row in rows])


def monitoring_freshness(session: Session) -> list[StageFreshness]:
    ingested = session.scalar(
        select(CommerceEvent.ingested_at).order_by(CommerceEvent.ingested_at.desc()).limit(1)
    )
    silver = session.scalar(
        select(LakePublication.finished_at)
        .where(LakePublication.status == "published")
        .order_by(LakePublication.finished_at.desc())
        .limit(1)
    )
    warehouse = session.scalar(
        select(WarehouseLoad.finished_at)
        .where(WarehouseLoad.status == "published")
        .order_by(WarehouseLoad.finished_at.desc())
        .limit(1)
    )
    return [
        _stage("ingestion", ingested, "most recent ingested_at, not occurred_at"),
        _stage("silver", silver, "latest published Silver finished_at"),
        _stage("warehouse", warehouse, "latest published warehouse finished_at"),
    ]


def _stage(stage: str, observed: datetime | None, detail: str) -> StageFreshness:
    if observed is None:
        return StageFreshness(stage=stage, observed_at=None, age_seconds=None, detail=detail)
    moment = observed if observed.tzinfo is not None else observed.replace(tzinfo=UTC)
    return StageFreshness(
        stage=stage,
        observed_at=moment,
        age_seconds=max(0, int((datetime.now(UTC) - moment).total_seconds())),
        detail=detail,
    )


def _lag(bootstrap: str) -> LagView:
    consumer = None
    try:
        from confluent_kafka.admin import AdminClient

        admin = AdminClient({"bootstrap.servers": bootstrap})
        request = ConsumerGroupTopicPartitions(CONSUMER_GROUP)
        listed = admin.list_consumer_group_offsets([request])
        description = next(iter(listed.values())).result(timeout=5)
        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": "titan-monitoring-readonly",
                "enable.auto.commit": False,
            }
        )
        total = 0
        measured = 0
        for part in description.topic_partitions:
            if part.offset is None or part.offset < 0:
                continue
            _low, high = consumer.get_watermark_offsets(
                TopicPartition(part.topic, part.partition),
                timeout=5,
            )
            total += max(0, high - part.offset)
            measured += 1
        if measured == 0:
            return LagView(
                consumer_group=CONSUMER_GROUP,
                available=False,
                total_lag=None,
                detail="the ingestion group has no committed offsets",
            )
        return LagView(
            consumer_group=CONSUMER_GROUP,
            available=True,
            total_lag=total,
            detail="sum of high watermark minus committed offset; not Spark checkpoint lag",
        )
    except Exception as exc:
        return LagView(
            consumer_group=CONSUMER_GROUP,
            available=False,
            total_lag=None,
            detail=f"lag was not measured: {exc.__class__.__name__}",
        )
    finally:
        if consumer is not None:
            consumer.close()


def _spark(session: Session) -> str | None:
    row = session.scalar(select(SparkJobStatus).where(SparkJobStatus.job_name == "bronze_stream"))
    if row is None:
        return None
    return f"{row.state} at {row.updated_at.isoformat()} offsets {row.last_offsets or 'unreported'}"


def _run_view(row: PipelineRun) -> RunView:
    return RunView(
        id=row.id,
        job_name=row.job_name,
        state=row.state,
        started_at=row.started_at,
        finished_at=row.finished_at,
        duration_ms=row.duration_ms,
        detail=row.detail,
        error_detail=row.error_detail,
        scope=_publication_scope(row.metadata_json),
    )


def _publication_scope(metadata_json: str | None) -> str | None:
    if not metadata_json:
        return None
    try:
        parsed = json.loads(metadata_json)
    except json.JSONDecodeError:
        return _invalid_scope(metadata_json)
    if not isinstance(parsed, dict):
        return _invalid_scope(metadata_json)
    for key in ("warehouse_publication_id", "publication_id"):
        value = parsed.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return None


def _invalid_scope(metadata_json: str) -> str:
    digest = hashlib.sha256(metadata_json.encode()).hexdigest()[:12]
    return f"invalid-metadata-{digest}"


def _failure(session: Session) -> str | None:
    row = session.scalar(
        select(PipelineRun)
        .where(PipelineRun.state == "failed")
        .order_by(PipelineRun.id.desc())
        .limit(1)
    )
    if row is None:
        return None
    return f"{row.job_name}: {(row.error_detail or row.detail or 'failed')[:300]}"


def _served(session: Session) -> MlRun | None:
    serving = session.scalar(
        select(MlRun).where(MlRun.status == "serving").order_by(MlRun.trained_at.desc()).limit(1)
    )
    if serving is not None:
        return serving
    return session.scalar(
        select(MlRun).where(MlRun.status == "promoted").order_by(MlRun.trained_at.desc()).limit(1)
    )


def _promoted(session: Session) -> MlRun | None:
    return _served(session)


def _admin_auth() -> str:
    import os

    if os.environ.get("TITAN_ADMIN_TOKEN"):
        return "token"
    return "closed"


def _model(row: MlRun) -> ModelView:
    metrics = None
    if row.metrics_json:
        parsed = json.loads(row.metrics_json)
        if isinstance(parsed, dict):
            metrics = parsed
    return ModelView(
        run_id=row.run_id,
        status=row.status,
        warehouse_publication_id=row.warehouse_publication_id,
        algorithm=row.algorithm,
        serving_strategy=row.serving_strategy,
        decision_rationale=row.decision_rationale,
        feature_version=row.feature_version,
        train_start=row.train_start,
        train_end=row.train_end,
        test_start=row.test_start,
        test_end=row.test_end,
        metrics=metrics,
        artifact_uri=row.artifact_uri,
        trained_at=row.trained_at,
        detail=row.detail,
    )


def _backfill_durations(session: Session) -> None:
    bind = session.get_bind()
    if bind.dialect.name != "postgresql":
        return
    session.execute(
        text(
            """
            update pipeline_runs
            set duration_ms = cast(extract(epoch from (finished_at - started_at)) * 1000 as integer)
            where duration_ms is null
              and finished_at is not null
              and finished_at > started_at
            """
        )
    )
    session.commit()
