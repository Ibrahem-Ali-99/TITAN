"""TITAN read API.

``/health/live`` only reports that the process is running.
``/health/ready`` reports whether PostgreSQL and the migrated tables are usable.
Stats come from those tables. A failed query returns an error, not zeros.
"""

import faulthandler
import logging
import os
import sys
from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import inspect, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from titan_api.auth import AuthenticationMiddleware
from titan_api.config import ApiSettings
from titan_api.lake import LakeStatus, load_lake_status
from titan_api.logging_config import configure_logging
from titan_api.metrics import OperationalCollector, collector_enabled, install_metrics
from titan_api.platform import (
    AnomaliesResponse,
    FindingsResponse,
    ForecastsResponse,
    JobStatusResponse,
    ModelsResponse,
    MonitoringOverview,
    QualityRuns,
    QualitySummary,
    RunsResponse,
    StageFreshness,
    ml_anomalies,
    ml_forecasts,
    ml_models,
    monitoring_freshness,
    monitoring_jobs,
    monitoring_overview,
    monitoring_runs,
    quality_findings,
    quality_runs,
    quality_summary,
)
from titan_api.queries import StatsUnavailable, load_ingestion_stats, load_recent_events
from titan_api.routes import register_routes
from titan_api.schemas import HealthLive, HealthReady, IngestionStats, RecentEventsResponse
from titan_api.tracing import maybe_enable_tracing, shutdown_tracing
from titan_api.warehouse import (
    AnalyticsOverview,
    OrdersResponse,
    ProductsResponse,
    RevenueResponse,
    WarehouseStatus,
    WarehouseUnavailable,
    load_orders,
    load_overview,
    load_products,
    load_revenue,
    load_status,
)
from titan_contracts.events import CommerceEvent
from titan_messaging.admin import ensure_commerce_topics
from titan_messaging.producer import PublishError, PublishResult, connect_producer, publish_record
from titan_store.db import create_db_engine, create_session_factory

logger = logging.getLogger(__name__)

REQUIRED_TABLES = (
    "commerce_events",
    "ingestion_quarantine",
    "ingestion_observations",
    "source_submissions",
    "csv_imports",
    "csv_import_rows",
    "replay_attempts",
    "spark_job_status",
    "lake_publications",
    "bronze_batch_commits",
    "pipeline_runs",
    "warehouse_loads",
    "quality_runs",
    "quality_findings",
    "ml_runs",
    "ml_predictions",
    "ml_anomalies",
)


def _read[T](loader: Callable[[], T]) -> T:
    try:
        return loader()
    except SQLAlchemyError as exc:
        logger.exception("platform query failed")
        raise HTTPException(status_code=503, detail="platform data is unavailable") from exc


def _analytics[T](loader: Callable[[], T]) -> T:
    try:
        return loader()
    except WarehouseUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("analytics query failed")
        raise HTTPException(status_code=503, detail="analytics query failed") from exc


def get_session(request: Request) -> Iterator[Session]:
    session = request.app.state.session_factory()
    try:
        yield session
    finally:
        session.close()


def create_app(settings: ApiSettings | None = None) -> FastAPI:
    resolved = settings or ApiSettings()
    configure_logging(resolved.log_level)
    if os.getenv("TITAN_MONITORING_DIAGNOSTICS") == "1":
        logging.getLogger(__name__).warning("[titan-monitoring-diagnostic] self-test=loaded")
        faulthandler.dump_traceback(file=sys.stderr, all_threads=True)
    engine = create_db_engine(resolved.database_url)
    session_factory = create_session_factory(engine)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        collector: OperationalCollector | None = None
        if collector_enabled(resolved):
            collector = OperationalCollector(session_factory, resolved)
            collector.start()
            app.state.metrics_collector = collector
        try:
            yield
        finally:
            if collector is not None:
                collector.stop()
            shutdown_tracing()

    app = FastAPI(title="TITAN API", version="0.1.0", lifespan=lifespan)
    app.add_middleware(AuthenticationMiddleware, settings=resolved)
    origins = [origin.strip() for origin in resolved.cors_origins.split(",") if origin.strip()]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=False,
        allow_methods=["GET", "POST", "OPTIONS"],
        allow_headers=["Accept", "Content-Type", "traceparent", "tracestate"],
    )
    app.state.settings = resolved
    app.state.engine = engine
    app.state.session_factory = session_factory

    def publish_event(event: CommerceEvent) -> PublishResult:
        try:
            ensure_commerce_topics(resolved.kafka_bootstrap_servers)
            producer = connect_producer(resolved.kafka_bootstrap_servers, client_id="titan-api")
        except PublishError:
            raise
        except Exception as exc:
            raise PublishError(f"broker was not ready: {exc}") from exc
        try:
            return publish_record(producer, event)
        finally:
            producer.flush(10)

    app.state.publish = publish_event
    register_routes(app, get_session)

    @app.get("/health/live", response_model=HealthLive)
    def live() -> HealthLive:
        return HealthLive(status="live")

    @app.get("/health/ready", response_model=HealthReady)
    def ready(session: Session = Depends(get_session)) -> HealthReady:
        try:
            session.execute(text("SELECT 1"))
            names = set(inspect(session.get_bind()).get_table_names())
        except SQLAlchemyError as exc:
            logger.exception("readiness check failed")
            raise HTTPException(
                status_code=503,
                detail="postgres is not ready: database connection failed",
            ) from exc
        missing = [name for name in REQUIRED_TABLES if name not in names]
        if missing:
            raise HTTPException(
                status_code=503,
                detail="postgres is not ready: schema is not migrated",
            )
        return HealthReady(status="ready", checks={"postgres": "ok"})

    install_metrics(app)

    @app.get("/api/v1/ingestion/stats", response_model=IngestionStats)
    def ingestion_stats(session: Session = Depends(get_session)) -> IngestionStats:
        try:
            return load_ingestion_stats(session)
        except StatsUnavailable as exc:
            logger.exception("ingestion stats query failed")
            raise HTTPException(
                status_code=503,
                detail=f"ingestion stats are unavailable: {exc}",
            ) from exc

    @app.get("/api/v1/lake/status", response_model=LakeStatus)
    def lake_status(session: Session = Depends(get_session)) -> LakeStatus:
        try:
            return load_lake_status(session)
        except SQLAlchemyError as exc:
            logger.exception("lake status query failed")
            raise HTTPException(status_code=503, detail="lake status is unavailable") from exc

    @app.get("/api/v1/warehouse/status", response_model=WarehouseStatus)
    def warehouse_status(session: Session = Depends(get_session)) -> WarehouseStatus:
        try:
            return load_status(session, resolved)
        except SQLAlchemyError as exc:
            logger.exception("warehouse status query failed")
            raise HTTPException(status_code=503, detail="warehouse status is unavailable") from exc

    @app.get("/api/v1/quality/summary", response_model=QualitySummary)
    def quality_summary_route(session: Session = Depends(get_session)) -> QualitySummary:
        return _read(lambda: quality_summary(session))

    @app.get("/api/v1/quality/runs", response_model=QualityRuns)
    def quality_runs_route(
        limit: int = Query(default=20, ge=1, le=100),
        session: Session = Depends(get_session),
    ) -> QualityRuns:
        return _read(lambda: quality_runs(session, limit))

    @app.get("/api/v1/quality/findings", response_model=FindingsResponse)
    def quality_findings_route(
        limit: int = Query(default=50, ge=1, le=200),
        run_id: str | None = None,
        failed_only: bool = False,
        session: Session = Depends(get_session),
    ) -> FindingsResponse:
        return _read(lambda: quality_findings(session, limit, run_id, failed_only))

    @app.get("/api/v1/ml/models", response_model=ModelsResponse)
    def ml_models_route(
        limit: int = Query(default=10, ge=1, le=50),
        session: Session = Depends(get_session),
    ) -> ModelsResponse:
        return _read(lambda: ml_models(session, limit))

    @app.get("/api/v1/ml/evaluations", response_model=ModelsResponse)
    def ml_evaluations_route(session: Session = Depends(get_session)) -> ModelsResponse:
        return _read(lambda: ml_models(session, 10))

    @app.get("/api/v1/ml/forecasts", response_model=ForecastsResponse)
    def ml_forecasts_route(
        limit: int = Query(default=50, ge=1, le=200),
        kind: str | None = None,
        session: Session = Depends(get_session),
    ) -> ForecastsResponse:
        return _read(lambda: ml_forecasts(session, limit, kind))

    @app.get("/api/v1/ml/anomalies", response_model=AnomaliesResponse)
    def ml_anomalies_route(
        limit: int = Query(default=50, ge=1, le=200),
        flagged_only: bool = False,
        session: Session = Depends(get_session),
    ) -> AnomaliesResponse:
        return _read(lambda: ml_anomalies(session, limit, flagged_only))

    @app.get("/api/v1/monitoring/overview", response_model=MonitoringOverview)
    def monitoring_overview_route(session: Session = Depends(get_session)) -> MonitoringOverview:
        return _read(lambda: monitoring_overview(session))

    @app.get("/api/v1/monitoring/runs", response_model=RunsResponse)
    def monitoring_runs_route(
        limit: int = Query(default=30, ge=1, le=100),
        session: Session = Depends(get_session),
    ) -> RunsResponse:
        return _read(lambda: monitoring_runs(session, limit))

    @app.get("/api/v1/monitoring/jobs", response_model=JobStatusResponse)
    def monitoring_jobs_route(session: Session = Depends(get_session)) -> JobStatusResponse:
        return _read(lambda: monitoring_jobs(session))

    @app.get("/api/v1/monitoring/freshness", response_model=list[StageFreshness])
    def monitoring_freshness_route(session: Session = Depends(get_session)) -> list[StageFreshness]:
        return _read(lambda: monitoring_freshness(session))

    @app.get("/api/v1/analytics/overview", response_model=AnalyticsOverview)
    def analytics_overview(session: Session = Depends(get_session)) -> AnalyticsOverview:
        return _analytics(lambda: load_overview(session, resolved))

    @app.get("/api/v1/analytics/revenue", response_model=RevenueResponse)
    def analytics_revenue(
        grain: str = Query(default="hour"),
        currency: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = Query(default=168, ge=1, le=500),
        session: Session = Depends(get_session),
    ) -> RevenueResponse:
        return _analytics(
            lambda: load_revenue(session, resolved, grain, currency, start, end, limit)
        )

    @app.get("/api/v1/analytics/orders", response_model=OrdersResponse)
    def analytics_orders(
        currency: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = Query(default=50, ge=1, le=200),
        session: Session = Depends(get_session),
    ) -> OrdersResponse:
        return _analytics(lambda: load_orders(session, resolved, currency, start, end, limit))

    @app.get("/api/v1/analytics/products", response_model=ProductsResponse)
    def analytics_products(
        currency: str | None = None,
        limit: int = Query(default=20, ge=1, le=100),
        session: Session = Depends(get_session),
    ) -> ProductsResponse:
        return _analytics(lambda: load_products(session, resolved, currency, limit))

    @app.get("/api/v1/events/recent", response_model=RecentEventsResponse)
    def recent_events(
        limit: int = Query(default=20, ge=1, le=100),
        session: Session = Depends(get_session),
    ) -> RecentEventsResponse:
        try:
            return load_recent_events(session, limit)
        except StatsUnavailable as exc:
            logger.exception("recent events query failed")
            raise HTTPException(
                status_code=503,
                detail=f"recent events are unavailable: {exc}",
            ) from exc

    maybe_enable_tracing(app, resolved)
    return app
