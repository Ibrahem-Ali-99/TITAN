"""Stats are computed from persisted rows. A failed query must not become zeros."""

from datetime import UTC, datetime, timedelta

from sqlalchemy import Executable, func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from titan_api.schemas import ActivityPoint, IngestionStats, RecentEvent, RecentEventsResponse
from titan_store.models import CommerceEvent, QuarantineRecord

ACTIVITY_WINDOW_MINUTES = 60

METRIC_DEFINITIONS = {
    "persisted_events": ("Count of rows in commerce_events. This is not Kafka broker throughput."),
    "persisted_order_created_events": (
        "Count of commerce_events rows whose event_type is order_created."
    ),
    "quarantined_records": (
        "Invalid Kafka records stored in ingestion_quarantine. "
        "They were not applied as business events."
    ),
    "most_recent_ingested_at": (
        "Latest ingested_at among persisted commerce events, or null when the table is empty. "
        "This is database write time, not the business occurred_at."
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
    try:
        persisted = _count(session, select(func.count()).select_from(CommerceEvent))
        orders = _count(
            session,
            select(func.count())
            .select_from(CommerceEvent)
            .where(CommerceEvent.event_type == "order_created"),
        )
        quarantined = _count(session, select(func.count()).select_from(QuarantineRecord))
        latest = session.scalar(select(func.max(CommerceEvent.ingested_at)))
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
    return IngestionStats(
        persisted_events=persisted,
        persisted_order_created_events=orders,
        quarantined_records=quarantined,
        most_recent_ingested_at=latest,
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
        total = payload.get("total_amount") if isinstance(payload, dict) else None
        items.append(
            RecentEvent(
                event_id=row.event_id,
                event_type=row.event_type,
                schema_version=row.schema_version,
                order_id=row.order_id,
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
