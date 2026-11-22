from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field


class HealthLive(BaseModel):
    status: str


class HealthReady(BaseModel):
    status: str
    checks: dict[str, str]


class ActivityPoint(BaseModel):
    minute: datetime
    persisted_events: int = Field(description="Rows persisted during this UTC minute.")


class IngestionStats(BaseModel):
    persisted_events: int
    persisted_order_created_events: int
    quarantined_records: int
    most_recent_ingested_at: datetime | None
    recent_activity: list[ActivityPoint]
    activity_window_minutes: int
    metric_definitions: dict[str, str]


class RecentEvent(BaseModel):
    event_id: UUID
    event_type: str
    schema_version: int
    order_id: str
    occurred_at: datetime
    ingested_at: datetime
    source: str
    currency: str | None
    total_amount: str | None
    synthetic: bool
    kafka_topic: str
    kafka_partition: int
    kafka_offset: int


class RecentEventsResponse(BaseModel):
    items: list[RecentEvent]
    limit: int
    returned: int = Field(
        description="Number of rows in this response, not the total persisted count."
    )
