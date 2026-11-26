"""Order producer used by the generator CLI and the Day 1 verification script.

Publication itself lives in ``titan_messaging``. This module keeps the
order-specific result shape those callers already use.
"""

from dataclasses import dataclass
from uuid import UUID

from confluent_kafka import Producer

from event_generator.admin import ensure_order_topic
from titan_contracts.events import OrderCreatedEvent
from titan_messaging.producer import (
    PublishError,
    connect_producer,
    publish_raw_record,
    publish_record,
)

__all__ = [
    "PublishError",
    "PublishResult",
    "connect_producer",
    "publish_events",
    "publish_one",
    "publish_raw_record",
]


@dataclass(frozen=True)
class PublishResult:
    event_id: UUID
    order_id: str
    topic: str
    partition: int
    offset: int


def publish_one(
    producer: Producer, event: OrderCreatedEvent, max_attempts: int = 3
) -> PublishResult:
    result = publish_record(producer, event, max_attempts=max_attempts)
    return PublishResult(
        event_id=result.event_id,
        order_id=event.payload.order_id,
        topic=result.topic,
        partition=result.partition,
        offset=result.offset,
    )


def publish_events(
    bootstrap_servers: str,
    events: list[OrderCreatedEvent],
    *,
    max_attempts: int = 3,
) -> list[PublishResult]:
    if not events:
        return []
    ensure_order_topic(bootstrap_servers)
    producer = connect_producer(bootstrap_servers, client_id="titan-event-generator")
    try:
        return [publish_one(producer, event, max_attempts) for event in events]
    finally:
        producer.flush(10)
