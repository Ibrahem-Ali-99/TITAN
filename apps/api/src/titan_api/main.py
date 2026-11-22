"""TITAN read API.

``/health/live`` only reports that the process is running.
``/health/ready`` reports whether PostgreSQL and the migrated tables are usable.
Stats come from those tables. A failed query returns an error, not zeros.
"""

import logging
from collections.abc import Iterator

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import inspect, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from titan_api.config import ApiSettings
from titan_api.logging_config import configure_logging
from titan_api.queries import StatsUnavailable, load_ingestion_stats, load_recent_events
from titan_api.schemas import HealthLive, HealthReady, IngestionStats, RecentEventsResponse
from titan_store.db import create_db_engine, create_session_factory

logger = logging.getLogger(__name__)

REQUIRED_TABLES = ("commerce_events", "ingestion_quarantine")


def get_session(request: Request) -> Iterator[Session]:
    session = request.app.state.session_factory()
    try:
        yield session
    finally:
        session.close()


def create_app(settings: ApiSettings | None = None) -> FastAPI:
    resolved = settings or ApiSettings()
    configure_logging(resolved.log_level)
    app = FastAPI(title="TITAN API", version="0.1.0")
    origins = [origin.strip() for origin in resolved.cors_origins.split(",") if origin.strip()]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=False,
        allow_methods=["GET", "OPTIONS"],
        allow_headers=["Accept", "Content-Type"],
    )
    engine = create_db_engine(resolved.database_url)
    app.state.settings = resolved
    app.state.engine = engine
    app.state.session_factory = create_session_factory(engine)

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

    return app
