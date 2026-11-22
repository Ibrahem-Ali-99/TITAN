"""Stats are computed from persisted rows. A failed query must not become zeros."""

from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import Executable, func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from titan_api.schemas import ActivityPoint, IngestionStats, RecentEvent, RecentEventsResponse
from titan_store.models import (
    CommerceEvent,
    CsvImportRow,
    IngestionObservation,
    QuarantineRecord,
    ReplayAttempt,
    SourceSubmission,
)

ACTIVITY_WINDOW_MINUTES = 60

METRIC_DEFINITIONS = {
    "persisted_events": "Count of rows in commerce_events. This is not Kafka broker throughput.",
    "persisted_order_created_events": (
        "Count of commerce_events rows whose event_type is order_created, "
        "including conflicts and duplicate operations."
    ),
    "quarantined_records": (
        "Invalid Kafka records stored in ingestion_quarantine. "
        "They were not applied as business events."
    ),
    "kafka_consumed_events": (
        "persisted_events + quarantined_records + duplicate_deliveries. "
        "Numerator is durable consumer outcomes. Denominator is not used. "
        "Scope is this PostgreSQL database. This is not broker throughput."
    ),
    "logical_orders": (
        "order_created rows whose disposition is accepted. "
        "Conflicts and duplicate operations are excluded so they are not extra orders."
    ),
    "order_conflicts": "order_created rows stored as evidence with disposition conflict.",
    "duplicate_operations": (
        "Rows whose payload matches an already accepted business key but whose event_id is new."
    ),
    "duplicate_deliveries": (
        "ingestion_observations rows for a repeated event_id. "
        "One row per Kafka position, so a retry of the same position does not increase the count."
    ),
    "accepted_payments": "payment_completed rows with disposition accepted.",
    "payment_conflicts": "payment_completed rows with disposition conflict.",
    "inventory_updates": "inventory_updated rows. Each new event_id is a state change.",
    "rest_events_published": (
        "source_submissions with channel rest and status published. "
        "Kafka acknowledgement, not PostgreSQL persistence."
    ),
    "rest_events_rejected": "source_submissions with channel rest and status rejected.",
    "csv_rows_accepted": "csv_import_rows with status published. Kafka acknowledgement only.",
    "csv_rows_rejected": "csv_import_rows that failed validation and were not published.",
    "csv_rows_publish_failed": "csv_import_rows whose broker acknowledgement failed.",
    "replays_published": "replay_attempts whose outcome is published. Persistence is separate.",
    "replays_refused": "replay_attempts refused because the record cannot be replayed as-is.",
    "most_recent_ingested_at": (
        "Latest ingested_at among persisted commerce events, or null when the table is empty. "
        "This is database write time, not the business occurred_at."
    ),
    "most_recent_error_at": (
        "Latest quarantine received_at or failed source submission time. Null when there are none."
    ),
    "recent_activity": (
        "Persisted commerce events bucketed by the UTC minute of ingested_at "
        "over the last 60 minutes. Minutes with zero events are omitted. "
        "This is not Kafka throughput."
    ),
}


class StatsUnavailable(Exception):
    """The database could not answer a stats or readiness query."""


def _count(session: Session, statement: Executable) -> int:
    value = session.scalar(statement)
    if value is None:
        raise StatsUnavailable("count query returned no value")
    return int(value)


def load_ingestion_stats(session: Session, *, now: datetime | None = None) -> IngestionStats:
    current = now or datetime.now(UTC)
    if current.tzinfo is None:
        current = current.replace(tzinfo=UTC)
    cutoff = current - timedelta(minutes=ACTIVITY_WINDOW_MINUTES)

    def disposition_count(event_type: str, disposition: str) -> int:
        return _count(
            session,
            select(func.count())
            .select_from(CommerceEvent)
            .where(
                CommerceEvent.event_type == event_type,
                CommerceEvent.disposition == disposition,
            ),
        )

    def status_count(model: Any, status: str, *criteria: Any) -> int:
        statement = select(func.count()).select_from(model).where(model.status == status)
        for criterion in criteria:
            statement = statement.where(criterion)
        return _count(session, statement)

    try:
        persisted = _count(session, select(func.count()).select_from(CommerceEvent))
        orders = _count(
            session,
            select(func.count())
            .select_from(CommerceEvent)
            .where(CommerceEvent.event_type == "order_created"),
        )
        quarantined = _count(session, select(func.count()).select_from(QuarantineRecord))
        logical_orders = disposition_count("order_created", "accepted")
        order_conflicts = disposition_count("order_created", "conflict")
        duplicate_operations = _count(
            session,
            select(func.count())
            .select_from(CommerceEvent)
            .where(CommerceEvent.disposition == "duplicate_operation"),
        )
        duplicate_deliveries = _count(
            session,
            select(func.count())
            .select_from(IngestionObservation)
            .where(IngestionObservation.outcome == "duplicate_delivery"),
        )
        accepted_payments = disposition_count("payment_completed", "accepted")
        payment_conflicts = disposition_count("payment_completed", "conflict")
        inventory_updates = disposition_count("inventory_updated", "accepted")
        rest_published = status_count(
            SourceSubmission, "published", SourceSubmission.channel == "rest"
        )
        rest_rejected = status_count(
            SourceSubmission, "rejected", SourceSubmission.channel == "rest"
        )
        csv_accepted = status_count(CsvImportRow, "published")
        csv_rejected = status_count(CsvImportRow, "rejected")
        csv_failed = status_count(CsvImportRow, "publish_failed")
        replays_published = _count(
            session,
            select(func.count())
            .select_from(ReplayAttempt)
            .where(ReplayAttempt.outcome == "published"),
        )
        replays_refused = _count(
            session,
            select(func.count())
            .select_from(ReplayAttempt)
            .where(ReplayAttempt.outcome == "refused"),
        )
        latest = session.scalar(select(func.max(CommerceEvent.ingested_at)))
        quarantine_error = session.scalar(select(func.max(QuarantineRecord.received_at)))
        submission_error = session.scalar(
            select(func.max(SourceSubmission.received_at)).where(
                SourceSubmission.status.in_(("rejected", "publish_failed"))
            )
        )
        timestamps = session.scalars(
            select(CommerceEvent.ingested_at).where(CommerceEvent.ingested_at >= cutoff)
        ).all()
    except SQLAlchemyError as exc:
        raise StatsUnavailable("database query failed") from exc

    buckets: dict[datetime, int] = {}
    for timestamp in timestamps:
        aware = timestamp if timestamp.tzinfo is not None else timestamp.replace(tzinfo=UTC)
        minute = aware.astimezone(UTC).replace(second=0, microsecond=0)
        buckets[minute] = buckets.get(minute, 0) + 1
    activity = [
        ActivityPoint(minute=minute, persisted_events=count)
        for minute, count in sorted(buckets.items())
    ]
    error_times = [value for value in (quarantine_error, submission_error) if value is not None]
    return IngestionStats(
        persisted_events=persisted,
        persisted_order_created_events=orders,
        quarantined_records=quarantined,
        kafka_consumed_events=persisted + quarantined + duplicate_deliveries,
        logical_orders=logical_orders,
        order_conflicts=order_conflicts,
        duplicate_operations=duplicate_operations,
        duplicate_deliveries=duplicate_deliveries,
        accepted_payments=accepted_payments,
        payment_conflicts=payment_conflicts,
        inventory_updates=inventory_updates,
        rest_events_published=rest_published,
        rest_events_rejected=rest_rejected,
        csv_rows_accepted=csv_accepted,
        csv_rows_rejected=csv_rejected,
        csv_rows_publish_failed=csv_failed,
        replays_published=replays_published,
        replays_refused=replays_refused,
        most_recent_ingested_at=latest,
        most_recent_error_at=max(error_times) if error_times else None,
        recent_activity=activity,
        activity_window_minutes=ACTIVITY_WINDOW_MINUTES,
        metric_definitions=METRIC_DEFINITIONS,
    )


def load_recent_events(session: Session, limit: int) -> RecentEventsResponse:
    try:
        rows = session.scalars(
            select(CommerceEvent)
            .order_by(CommerceEvent.ingested_at.desc(), CommerceEvent.event_id.desc())
            .limit(limit)
        ).all()
    except SQLAlchemyError as exc:
        raise StatsUnavailable("database query failed") from exc

    items: list[RecentEvent] = []
    for row in rows:
        payload = row.event_envelope.get("payload")
        currency = payload.get("currency") if isinstance(payload, dict) else None
        total = None
        if isinstance(payload, dict):
            if payload.get("total_amount") is not None:
                total = payload.get("total_amount")
            elif payload.get("amount") is not None:
                total = payload.get("amount")
        items.append(
            RecentEvent(
                event_id=row.event_id,
                event_type=row.event_type,
                schema_version=row.schema_version,
                order_id=row.order_id,
                disposition=row.disposition,
                occurred_at=row.occurred_at,
                ingested_at=row.ingested_at,
                source=row.source,
                currency=str(currency) if currency is not None else None,
                total_amount=str(total) if total is not None else None,
                synthetic=row.source.startswith("synthetic."),
                kafka_topic=row.kafka_topic,
                kafka_partition=row.kafka_partition,
                kafka_offset=row.kafka_offset,
            )
        )
    return RecentEventsResponse(items=items, limit=limit, returned=len(items))
