"""Prometheus metrics for the API and the operational records it already reads.

Gauges are refreshed when ``/metrics`` is scraped. A failed refresh leaves the
HTTP counters in place and does not change request handling.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Awaitable, Callable

from fastapi import Depends, FastAPI, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from sqlalchemy import inspect, text
from sqlalchemy.orm import Session
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response as StarletteResponse
from starlette.types import ASGIApp

from titan_api.config import ApiSettings
from titan_api.platform import _lag, monitoring_freshness, monitoring_jobs, quality_summary
from titan_api.warehouse import load_status
from titan_store.models import SparkJobStatus

logger = logging.getLogger(__name__)

TRACKED_JOBS = (
    "bronze_stream",
    "silver_publication",
    "clickhouse_load",
    "warehouse_load",
    "warehouse_models",
    "quality_gate",
    "analytics_ready",
    "warehouse_reconcile",
)

REQUESTS = Counter(
    "titan_http_requests_total",
    "HTTP requests handled by the API",
    ("method", "path", "status"),
)
LATENCY = Histogram(
    "titan_http_request_duration_seconds",
    "HTTP request latency in seconds",
    ("method", "path"),
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
)
API_READY = Gauge("titan_api_ready", "1 when PostgreSQL and the migrated schema answer")
WAREHOUSE_READY = Gauge("titan_warehouse_ready", "1 when the serving publication is ready")
WAREHOUSE_FRESHNESS = Gauge(
    "titan_warehouse_freshness_seconds",
    "Age of the published warehouse timestamp in seconds",
)
STAGE_AGE = Gauge(
    "titan_stage_age_seconds",
    "Age of the latest recorded stage timestamp in seconds",
    ("stage",),
)
CONSUMER_LAG = Gauge(
    "titan_kafka_consumer_lag_records",
    "Ingestion consumer lag in records; not the Spark checkpoint",
)
LAG_MEASURED = Gauge("titan_kafka_lag_measured", "1 when the ingestion lag query succeeded")
QUALITY_FAILURES = Gauge(
    "titan_quality_failure_count",
    "Blocking failures on the latest quality run",
)
QUALITY_FAILED = Gauge(
    "titan_quality_latest_failed",
    "1 when the latest quality run failed and was not an injected test",
)
QUALITY_INJECTED = Gauge(
    "titan_quality_latest_injected",
    "1 when the latest quality run is an injected test fault",
)
PIPELINE_FAILED = Gauge(
    "titan_pipeline_latest_failed",
    "1 when the newest recorded attempt for the job failed",
    ("job",),
)
SPARK_FAILED = Gauge(
    "titan_spark_job_failed",
    "1 when the latest Spark status row for the job is failed",
    ("job",),
)
REFRESH_OK = Gauge("titan_metrics_refresh_ok", "1 when the operational gauge refresh succeeded")


class MetricsMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: ASGIApp) -> None:
        super().__init__(app)

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[StarletteResponse]],
    ) -> StarletteResponse:
        started = time.perf_counter()
        status = 500
        try:
            response = await call_next(request)
            status = response.status_code
            return response
        finally:
            route = request.scope.get("route")
            path = getattr(route, "path", "unmatched")
            method = request.method
            REQUESTS.labels(method, path, str(status)).inc()
            LATENCY.labels(method, path).observe(time.perf_counter() - started)


def install_metrics(
    app: FastAPI,
    settings: ApiSettings,
    get_session: Callable[..., object],
) -> None:
    app.add_middleware(MetricsMiddleware)

    @app.get("/metrics", include_in_schema=False)
    def metrics(session: Session = Depends(get_session)) -> Response:
        refresh_operational_metrics(session, settings)
        return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


def refresh_operational_metrics(session: Session, settings: ApiSettings) -> None:
    results = (
        _ready(session),
        _warehouse(session, settings),
        _freshness(session),
        _lag_gauge(settings),
        _quality(session),
        _pipelines(session),
        _spark(session),
    )
    REFRESH_OK.set(1 if all(results) else 0)


def _ready(session: Session) -> bool:
    try:
        from titan_api.main import REQUIRED_TABLES

        session.execute(text("SELECT 1"))
        names = set(inspect(session.get_bind()).get_table_names())
    except Exception:
        logger.exception("api readiness metric failed")
        API_READY.set(0)
        return False
    ready = all(name in names for name in REQUIRED_TABLES)
    API_READY.set(1 if ready else 0)
    return ready


def _warehouse(session: Session, settings: ApiSettings) -> bool:
    try:
        status = load_status(session, settings)
    except Exception:
        logger.exception("warehouse metric failed")
        WAREHOUSE_READY.set(0)
        return False
    WAREHOUSE_READY.set(1 if status.ready else 0)
    if status.freshness_seconds is not None:
        WAREHOUSE_FRESHNESS.set(status.freshness_seconds)
    return True


def _freshness(session: Session) -> bool:
    try:
        stages = monitoring_freshness(session)
    except Exception:
        logger.exception("freshness metric failed")
        return False
    for stage in stages:
        if stage.age_seconds is not None:
            STAGE_AGE.labels(stage.stage).set(stage.age_seconds)
    return True


def _lag_gauge(settings: ApiSettings) -> bool:
    view = _lag(settings.kafka_bootstrap_servers)
    if not view.available or view.total_lag is None:
        LAG_MEASURED.set(0)
        return False
    LAG_MEASURED.set(1)
    CONSUMER_LAG.set(view.total_lag)
    return True


def _quality(session: Session) -> bool:
    try:
        summary = quality_summary(session)
    except Exception:
        logger.exception("quality metric failed")
        return False
    injected = bool(summary.injected)
    QUALITY_INJECTED.set(1 if injected else 0)
    QUALITY_FAILURES.set(summary.failure_count or 0)
    failed = summary.latest_status == "failed" and not injected
    QUALITY_FAILED.set(1 if failed else 0)
    return True


def _pipelines(session: Session) -> bool:
    try:
        jobs = {job.job_name: job.state for job in monitoring_jobs(session).jobs}
    except Exception:
        logger.exception("pipeline metric failed")
        return False
    for name in TRACKED_JOBS:
        PIPELINE_FAILED.labels(name).set(1 if jobs.get(name) == "failed" else 0)
    return True


def _spark(session: Session) -> bool:
    try:
        row = session.get(SparkJobStatus, "bronze_stream")
    except Exception:
        logger.exception("spark metric failed")
        return False
    SPARK_FAILED.labels("bronze_stream").set(1 if row is not None and row.state == "failed" else 0)
    return True
