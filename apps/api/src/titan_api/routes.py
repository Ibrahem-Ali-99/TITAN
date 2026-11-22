"""REST publication, bounded CSV import, and operator replay.

Kafka acknowledgement is not PostgreSQL persistence. Business rows are written
only by the ingestion consumer.
"""

import os
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from fastapi import (
    APIRouter,
    BackgroundTasks,
    Depends,
    FastAPI,
    HTTPException,
    Query,
    Request,
    UploadFile,
)
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from titan_api.csv_import import MAX_IMPORT_BYTES, CsvFileError, parse_csv_bytes
from titan_api.replay import downstream_state, run_replay
from titan_contracts.events import CommerceEvent
from titan_contracts.parsing import CommerceParseError, parse_commerce_object
from titan_messaging.producer import PublishError, PublishResult
from titan_store.models import (
    CsvImport,
    CsvImportRow,
    QuarantineRecord,
    ReplayAttempt,
    SourceSubmission,
)


class PublishAck(BaseModel):
    status: str
    persisted: bool
    event_id: UUID | None = None
    event_type: str | None = None
    topic: str | None = None
    partition: int | None = None
    offset: int | None = None
    detail: str


class ImportAccepted(BaseModel):
    import_id: UUID
    status: str
    accepted_rows: int
    rejected_rows: int
    detail: str


class ReplayBody(BaseModel):
    quarantine_ids: list[int] = Field(default_factory=list, max_length=20)
    event_ids: list[UUID] = Field(default_factory=list, max_length=20)
    replacement: dict[str, Any] | None = None


def register_routes(app: FastAPI, get_session: Callable[..., Iterator[Session]]) -> None:
    routes = APIRouter()

    @routes.post("/api/v1/ingestion/events", response_model=PublishAck, status_code=202)
    def publish_event(
        payload: dict[str, Any],
        session: Session = Depends(get_session),
    ) -> PublishAck:
        try:
            event = parse_commerce_object(payload)
        except CommerceParseError as exc:
            _submission(
                session,
                channel="rest",
                status="rejected",
                event_id=None,
                event_type=_text(payload.get("event_type")),
                source_name=_text(payload.get("source")),
                detail=f"{exc.code}: {exc.detail}"[:2000],
            )
            raise HTTPException(status_code=422, detail=f"{exc.code}: {exc.detail}") from exc
        try:
            result = app.state.publish(event)
        except PublishError as exc:
            _submission(
                session,
                channel="rest",
                status="publish_failed",
                event_id=event.event_id,
                event_type=event.event_type,
                source_name=event.source,
                detail=str(exc)[:2000],
            )
            raise HTTPException(
                status_code=503,
                detail="broker did not acknowledge the event; it was not persisted",
            ) from exc
        _submission(
            session,
            channel="rest",
            status="published",
            event_id=event.event_id,
            event_type=event.event_type,
            source_name=event.source,
            detail="Kafka acknowledged the record. PostgreSQL persistence is separate.",
            result=result,
        )
        return PublishAck(
            status="published_to_kafka",
            persisted=False,
            event_id=result.event_id,
            event_type=event.event_type,
            topic=result.topic,
            partition=result.partition,
            offset=result.offset,
            detail=(
                "Kafka acknowledged the record. This response does not mean it is in PostgreSQL."
            ),
        )

    @routes.post("/api/v1/ingestion/imports", response_model=ImportAccepted, status_code=202)
    async def start_import(
        file: UploadFile,
        background: BackgroundTasks,
        session: Session = Depends(get_session),
    ) -> ImportAccepted:
        raw = await file.read(MAX_IMPORT_BYTES + 1)
        produced_at = datetime.now(UTC)
        import_id = uuid4()
        try:
            parsed = parse_csv_bytes(raw, source=f"csv.import.{import_id}", produced_at=produced_at)
        except CsvFileError as exc:
            session.add(
                CsvImport(
                    id=import_id,
                    filename=(file.filename or "upload.csv")[:255],
                    received_at=produced_at,
                    completed_at=produced_at,
                    status="failed",
                    detail=exc.detail,
                )
            )
            session.commit()
            raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc
        rejected = sum(1 for row in parsed if row.error is not None)
        publishing = sum(1 for row in parsed if row.event is not None)
        session.add(
            CsvImport(
                id=import_id,
                filename=(file.filename or "upload.csv")[:255],
                received_at=produced_at,
                status="queued" if publishing else "completed",
                completed_at=None if publishing else produced_at,
                accepted_rows=0,
                rejected_rows=rejected,
                detail=(
                    "Rows are published to Kafka. They are not written directly to commerce_events."
                ),
            )
        )
        for row in parsed:
            session.add(
                CsvImportRow(
                    import_id=import_id,
                    row_number=row.row_number,
                    status="publishing" if row.event is not None else "rejected",
                    event_id=None if row.event is None else row.event.event_id,
                    event_type=None if row.event is None else row.event.event_type,
                    event_envelope=None if row.event is None else row.event.model_dump(mode="json"),
                    error_detail=row.error,
                )
            )
        session.commit()
        if publishing:
            background.add_task(_publish_import, app, import_id)
        return ImportAccepted(
            import_id=import_id,
            status="queued" if publishing else "completed",
            accepted_rows=0,
            rejected_rows=rejected,
            detail="Validation finished. Kafka publication continues after this response.",
        )

    @routes.post("/api/v1/ingestion/imports/{import_id}/publish", response_model=ImportAccepted)
    def resume_import(
        import_id: UUID,
        request: Request,
        background: BackgroundTasks,
        session: Session = Depends(get_session),
    ) -> ImportAccepted:
        _require_admin(request)
        record = session.get(CsvImport, import_id)
        if record is None:
            raise HTTPException(status_code=404, detail="import does not exist")
        background.add_task(_publish_import, app, import_id)
        return ImportAccepted(
            import_id=import_id,
            status=record.status,
            accepted_rows=record.accepted_rows,
            rejected_rows=record.rejected_rows,
            detail="Publication of rows still marked publishing was requested.",
        )

    @routes.get("/api/v1/ingestion/imports/{import_id}")
    def read_import(import_id: UUID, session: Session = Depends(get_session)) -> dict[str, Any]:
        record = session.get(CsvImport, import_id)
        if record is None:
            raise HTTPException(status_code=404, detail="import does not exist")
        rows = session.scalars(
            select(CsvImportRow)
            .where(CsvImportRow.import_id == import_id)
            .order_by(CsvImportRow.row_number)
        ).all()
        return {
            "import_id": str(record.id),
            "filename": record.filename,
            "status": record.status,
            "accepted_rows": record.accepted_rows,
            "rejected_rows": record.rejected_rows,
            "publish_failed_rows": record.publish_failed_rows,
            "detail": record.detail,
            "rows": [
                {
                    "row_number": row.row_number,
                    "status": row.status,
                    "event_id": None if row.event_id is None else str(row.event_id),
                    "event_type": row.event_type,
                    "error_detail": row.error_detail,
                    "kafka_topic": row.kafka_topic,
                    "kafka_partition": row.kafka_partition,
                    "kafka_offset": row.kafka_offset,
                }
                for row in rows
            ],
        }

    @routes.get("/api/v1/ingestion/submissions")
    def list_submissions(
        limit: int = Query(default=20, ge=1, le=100),
        session: Session = Depends(get_session),
    ) -> dict[str, Any]:
        rows = session.scalars(
            select(SourceSubmission).order_by(SourceSubmission.received_at.desc()).limit(limit)
        ).all()
        return {"items": [_submission_item(row) for row in rows], "returned": len(rows)}

    @routes.get("/api/v1/ingestion/imports")
    def list_imports(
        limit: int = Query(default=20, ge=1, le=100),
        session: Session = Depends(get_session),
    ) -> dict[str, Any]:
        rows = session.scalars(
            select(CsvImport).order_by(CsvImport.received_at.desc()).limit(limit)
        ).all()
        return {
            "items": [
                {
                    "import_id": str(row.id),
                    "filename": row.filename,
                    "status": row.status,
                    "accepted_rows": row.accepted_rows,
                    "rejected_rows": row.rejected_rows,
                    "publish_failed_rows": row.publish_failed_rows,
                    "received_at": row.received_at.isoformat(),
                }
                for row in rows
            ],
            "returned": len(rows),
        }

    @routes.get("/api/v1/ingestion/quarantine")
    def list_quarantine(
        limit: int = Query(default=20, ge=1, le=100),
        session: Session = Depends(get_session),
    ) -> dict[str, Any]:
        rows = session.scalars(
            select(QuarantineRecord).order_by(QuarantineRecord.received_at.desc()).limit(limit)
        ).all()
        return {
            "items": [
                {
                    "id": row.id,
                    "received_at": row.received_at.isoformat(),
                    "reason_code": row.error_type,
                    "error_detail": row.error_detail,
                    "event_id": None if row.event_id is None else str(row.event_id),
                    "event_type": row.event_type,
                    "source": row.source,
                    "replay_status": row.replay_status,
                    "kafka_topic": row.kafka_topic,
                    "kafka_partition": row.kafka_partition,
                    "kafka_offset": row.kafka_offset,
                }
                for row in rows
            ],
            "returned": len(rows),
        }

    @routes.post("/api/v1/ingestion/replays")
    def create_replays(
        body: ReplayBody,
        request: Request,
        session: Session = Depends(get_session),
    ) -> dict[str, Any]:
        _require_admin(request)
        try:
            attempts = run_replay(
                session,
                app.state.publish,
                quarantine_ids=body.quarantine_ids,
                event_ids=body.event_ids,
                replacement=body.replacement,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return {"items": [_replay_item(session, attempt) for attempt in attempts]}

    @routes.get("/api/v1/ingestion/replays")
    def list_replays(
        limit: int = Query(default=20, ge=1, le=100),
        session: Session = Depends(get_session),
    ) -> dict[str, Any]:
        rows = session.scalars(
            select(ReplayAttempt).order_by(ReplayAttempt.requested_at.desc()).limit(limit)
        ).all()
        return {"items": [_replay_item(session, row) for row in rows], "returned": len(rows)}

    app.include_router(routes)


def _text(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _submission(
    session: Session,
    *,
    channel: str,
    status: str,
    event_id: UUID | None,
    event_type: str | None,
    source_name: str | None,
    detail: str,
    result: PublishResult | None = None,
) -> None:
    session.add(
        SourceSubmission(
            id=uuid4(),
            channel=channel,
            received_at=datetime.now(UTC),
            status=status,
            event_id=event_id,
            event_type=event_type,
            source_name=source_name,
            detail=detail,
            kafka_topic=None if result is None else result.topic,
            kafka_partition=None if result is None else result.partition,
            kafka_offset=None if result is None else result.offset,
        )
    )
    session.commit()


def _submission_item(row: SourceSubmission) -> dict[str, object]:
    return {
        "id": str(row.id),
        "channel": row.channel,
        "received_at": row.received_at.isoformat(),
        "status": row.status,
        "event_id": None if row.event_id is None else str(row.event_id),
        "event_type": row.event_type,
        "source_name": row.source_name,
        "detail": row.detail,
        "kafka_topic": row.kafka_topic,
        "kafka_partition": row.kafka_partition,
        "kafka_offset": row.kafka_offset,
        "persisted": False,
    }


def _require_admin(request: Request) -> None:
    """Fail closed. An unset token does not leave replay or resume open."""
    token = os.environ.get("TITAN_ADMIN_TOKEN")
    header = request.headers.get("authorization")
    if not token or header != f"Bearer {token}":
        raise HTTPException(status_code=401, detail="admin token required")


def _replay_item(session: Session, attempt: ReplayAttempt) -> dict[str, object]:
    return {
        "id": str(attempt.id),
        "requested_at": attempt.requested_at.isoformat(),
        "mode": attempt.mode,
        "outcome": attempt.outcome,
        "downstream_state": downstream_state(session, attempt),
        "detail": attempt.detail,
        "quarantine_id": attempt.quarantine_id,
        "source_event_id": None
        if attempt.source_event_id is None
        else str(attempt.source_event_id),
        "published_event_id": None
        if attempt.published_event_id is None
        else str(attempt.published_event_id),
        "kafka_topic": attempt.kafka_topic,
        "kafka_partition": attempt.kafka_partition,
        "kafka_offset": attempt.kafka_offset,
    }


def _publish_import(app: FastAPI, import_id: UUID) -> None:
    session_factory: sessionmaker[Session] = app.state.session_factory
    session = session_factory()
    try:
        record = session.get(CsvImport, import_id)
        if record is None:
            return
        record.status = "running"
        session.commit()
        pending = session.scalars(
            select(CsvImportRow)
            .where(CsvImportRow.import_id == import_id, CsvImportRow.status == "publishing")
            .order_by(CsvImportRow.row_number)
        ).all()
        publish: Callable[[CommerceEvent], PublishResult] = app.state.publish
        for row in pending:
            envelope = row.event_envelope
            if envelope is None:
                row.status = "rejected"
                row.error_detail = "row has no canonical event to publish"
                session.commit()
                continue
            try:
                event = parse_commerce_object(envelope)
                result = publish(event)
            except CommerceParseError as exc:
                row.status = "rejected"
                row.error_detail = f"{exc.code}: {exc.detail}"[:2000]
            except PublishError as exc:
                row.status = "publish_failed"
                row.error_detail = str(exc)[:2000]
            else:
                row.status = "published"
                row.kafka_topic = result.topic
                row.kafka_partition = result.partition
                row.kafka_offset = result.offset
            session.commit()
        rows = session.scalars(
            select(CsvImportRow).where(CsvImportRow.import_id == import_id)
        ).all()
        record.accepted_rows = sum(1 for row in rows if row.status == "published")
        record.rejected_rows = sum(1 for row in rows if row.status == "rejected")
        record.publish_failed_rows = sum(1 for row in rows if row.status == "publish_failed")
        record.status = "completed"
        record.completed_at = datetime.now(UTC)
        session.commit()
    finally:
        session.close()
