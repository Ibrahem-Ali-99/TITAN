"""Prometheus metrics for the API and the operational records it already reads.

``/metrics`` only renders counters and the last collected gauges. Warehouse,
PostgreSQL, and Kafka reads run on one background thread. A failed read clears
the ``known`` gauge for that signal and leaves the previous value unchanged.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, TimeoutError

from fastapi import FastAPI
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from sqlalchemy import inspect, text
from sqlalchemy.orm import Session
from starlette.responses import Response
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from titan_api.config import ApiSettings
from titan_api.platform import _lag, monitoring_freshness, monitoring_jobs, quality_summary
from titan_api.warehouse import load_status
from titan_store.models import SparkJobStatus

logger = logging.getLogger(__name__)

COLLECT_INTERVAL_SECONDS = 15.0
FRESH_SECONDS = 45.0
PROBE_TIMEOUT_SECONDS = 5.0

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
API_READY_KNOWN = Gauge("titan_api_ready_known", "1 when the readiness check completed")
WAREHOUSE_READY = Gauge("titan_warehouse_ready", "1 when the serving publication is ready")
WAREHOUSE_KNOWN = Gauge(
    "titan_warehouse_status_known", "1 when the latest warehouse read succeeded"
)
WAREHOUSE_FRESHNESS = Gauge(
    "titan_warehouse_freshness_seconds",
    "Age of the published warehouse timestamp in seconds",
)
FRESHNESS_KNOWN = Gauge(
    "titan_warehouse_freshness_known",
    "1 when the latest warehouse read included a publication age",
)
STAGE_AGE = Gauge(
    "titan_stage_age_seconds",
    "Age of the latest recorded stage timestamp in seconds",
    ("stage",),
)
STAGE_KNOWN = Gauge(
    "titan_stage_freshness_known",
    "1 when the latest stage-age read succeeded",
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
QUALITY_KNOWN = Gauge("titan_quality_status_known", "1 when a quality run was read")
PIPELINE_FAILED = Gauge(
    "titan_pipeline_latest_failed",
    "1 when the newest recorded attempt for the job failed",
    ("job",),
)
PIPELINE_KNOWN = Gauge("titan_pipeline_status_known", "1 when the latest pipeline read succeeded")
SPARK_FAILED = Gauge(
    "titan_spark_job_failed",
    "1 when the latest Spark status row for the job is failed",
    ("job",),
)
SPARK_KNOWN = Gauge("titan_spark_status_known", "1 when the latest Spark status read succeeded")
COLLECTION_OK = Gauge(
    "titan_metrics_collection_ok",
    "1 when the last collection attempt read every operational signal",
)
COLLECTION_DURATION = Gauge(
    "titan_metrics_collection_duration_seconds",
    "Duration of the last operational collection attempt",
)
OPERATIONAL_FRESH = Gauge(
    "titan_operational_metrics_fresh",
    "1 when a collection attempt finished within the freshness window",
)
OPERATIONAL_AGE = Gauge(
    "titan_operational_metrics_age_seconds",
    "Seconds since the last finished collection attempt",
)
SCRAPE_DURATION = Gauge(
    "titan_metrics_scrape_duration_seconds",
    "Duration of the previous /metrics render, excluding collection",
)

_age_lock = threading.Lock()
_last_attempt_monotonic: float | None = None
_last_scrape_seconds: float | None = None


class MetricsMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        started = time.perf_counter()
        status = 500

        async def send_wrapper(message: Message) -> None:
            nonlocal status
            if message["type"] == "http.response.start":
                status = int(message["status"])
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            route = scope.get("route")
            path = getattr(route, "path", "unmatched")
            method = str(scope.get("method", ""))
            REQUESTS.labels(method, path, str(status)).inc()
            LATENCY.labels(method, path).observe(time.perf_counter() - started)


class OperationalCollector:
    """One background collection at a time. A busy attempt is not started again."""

    def __init__(
        self,
        session_factory: Callable[[], Session],
        settings: ApiSettings,
        interval_seconds: float = COLLECT_INTERVAL_SECONDS,
    ) -> None:
        self._session_factory = session_factory
        self._settings = settings
        self._interval = interval_seconds
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._loop,
            name="titan-metrics-collector",
            daemon=True,
        )
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout)

    def collect_once(self) -> None:
        if not self._lock.acquire(blocking=False):
            return
        started = time.perf_counter()
        try:
            ok = False
            try:
                ok = self._collect()
            except Exception:
                logger.exception("operational metrics collection failed")
            COLLECTION_OK.set(1 if ok else 0)
        finally:
            COLLECTION_DURATION.set(time.perf_counter() - started)
            mark_attempt_finished()
            self._lock.release()

    def _loop(self) -> None:
        while not self._stop.is_set():
            self.collect_once()
            if self._stop.wait(self._interval):
                return

    def _collect(self) -> bool:
        probes: tuple[Callable[[], bool], ...] = (
            lambda: self._with_session(_ready),
            lambda: self._with_session(lambda session: _warehouse(session, self._settings)),
            lambda: self._with_session(_freshness),
            lambda: _lag_gauge(self._settings),
            lambda: self._with_session(_quality),
            lambda: self._with_session(_pipelines),
            lambda: self._with_session(_spark),
        )
        executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="titan-metrics-probe")
        futures = [executor.submit(probe) for probe in probes]
        deadline = time.monotonic() + PROBE_TIMEOUT_SECONDS
        results: list[bool] = []
        try:
            for future in futures:
                remaining = max(0.0, deadline - time.monotonic())
                try:
                    results.append(bool(future.result(timeout=remaining)))
                except TimeoutError:
                    logger.warning("operational metrics probe timed out")
                    results.append(False)
                except Exception:
                    logger.exception("operational metrics probe failed")
                    results.append(False)
        finally:
            executor.shutdown(wait=False, cancel_futures=True)
        return all(results)

    def _with_session(self, probe: Callable[[Session], bool]) -> bool:
        session = self._session_factory()
        try:
            return probe(session)
        finally:
            session.close()


def collector_enabled(settings: ApiSettings) -> bool:
    if settings.database_url.startswith("sqlite"):
        return False
    return settings.metrics_collector_enabled


def install_metrics(app: FastAPI) -> None:
    app.add_middleware(MetricsMiddleware)

    @app.get("/metrics", include_in_schema=False)
    def metrics() -> Response:
        started = time.perf_counter()
        publish_collection_age()
        if _last_scrape_seconds is not None:
            SCRAPE_DURATION.set(_last_scrape_seconds)
        body = generate_latest()
        _remember_scrape(time.perf_counter() - started)
        return Response(content=body, media_type=CONTENT_TYPE_LATEST)


def mark_attempt_finished() -> None:
    global _last_attempt_monotonic
    with _age_lock:
        _last_attempt_monotonic = time.monotonic()


def publish_collection_age() -> None:
    with _age_lock:
        last = _last_attempt_monotonic
    if last is None:
        OPERATIONAL_FRESH.set(0)
        return
    age = time.monotonic() - last
    OPERATIONAL_AGE.set(age)
    OPERATIONAL_FRESH.set(1 if age <= FRESH_SECONDS else 0)


def _remember_scrape(seconds: float) -> None:
    global _last_scrape_seconds
    _last_scrape_seconds = seconds


def _ready(session: Session) -> bool:
    try:
        from titan_api.main import REQUIRED_TABLES

        session.execute(text("SELECT 1"))
        names = set(inspect(session.get_bind()).get_table_names())
    except Exception:
        logger.exception("api readiness metric failed")
        API_READY_KNOWN.set(1)
        API_READY.set(0)
        return False
    API_READY_KNOWN.set(1)
    API_READY.set(1 if all(name in names for name in REQUIRED_TABLES) else 0)
    return True


def _warehouse(session: Session, settings: ApiSettings) -> bool:
    try:
        status = load_status(session, settings)
    except Exception:
        logger.exception("warehouse metric failed")
        WAREHOUSE_KNOWN.set(0)
        FRESHNESS_KNOWN.set(0)
        return False
    WAREHOUSE_KNOWN.set(1)
    WAREHOUSE_READY.set(1 if status.ready else 0)
    if status.freshness_seconds is None:
        FRESHNESS_KNOWN.set(0)
        return True
    FRESHNESS_KNOWN.set(1)
    WAREHOUSE_FRESHNESS.set(status.freshness_seconds)
    return True


def _freshness(session: Session) -> bool:
    try:
        stages = monitoring_freshness(session)
    except Exception:
        logger.exception("freshness metric failed")
        STAGE_KNOWN.set(0)
        return False
    STAGE_KNOWN.set(1)
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
        QUALITY_KNOWN.set(0)
        return False
    if not summary.available or summary.failure_count is None:
        QUALITY_KNOWN.set(0)
        return True
    QUALITY_KNOWN.set(1)
    injected = bool(summary.injected)
    QUALITY_INJECTED.set(1 if injected else 0)
    QUALITY_FAILURES.set(summary.failure_count)
    failed = summary.latest_status == "failed" and not injected
    QUALITY_FAILED.set(1 if failed else 0)
    return True


def _pipelines(session: Session) -> bool:
    try:
        jobs = {job.job_name: job.state for job in monitoring_jobs(session).jobs}
    except Exception:
        logger.exception("pipeline metric failed")
        PIPELINE_KNOWN.set(0)
        return False
    PIPELINE_KNOWN.set(1)
    for name in TRACKED_JOBS:
        PIPELINE_FAILED.labels(name).set(1 if jobs.get(name) == "failed" else 0)
    return True


def _spark(session: Session) -> bool:
    try:
        row = session.get(SparkJobStatus, "bronze_stream")
    except Exception:
        logger.exception("spark metric failed")
        SPARK_KNOWN.set(0)
        return False
    SPARK_KNOWN.set(1)
    SPARK_FAILED.labels("bronze_stream").set(1 if row is not None and row.state == "failed" else 0)
    return True
