"""Kafka topic catalog for commerce events.

``order_created`` stays on the Day 1 topic. Payment and inventory events use
their own topics so a partition key stays stable for that business entity.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int
    retention_ms: int
    event_type: str


RETENTION_MS = 7 * 24 * 60 * 60 * 1000

ORDER_EVENTS_TOPIC = "titan.commerce.order-events"
ORDER_EVENTS_PARTITIONS = 3
ORDER_EVENTS_RETENTION_MS = RETENTION_MS

PAYMENT_EVENTS_TOPIC = "titan.commerce.payment-events"
INVENTORY_EVENTS_TOPIC = "titan.commerce.inventory-events"
CONSUMER_GROUP = "titan-ingestion"

COMMERCE_TOPICS: tuple[TopicSpec, ...] = (
    TopicSpec(ORDER_EVENTS_TOPIC, ORDER_EVENTS_PARTITIONS, RETENTION_MS, "order_created"),
    TopicSpec(PAYMENT_EVENTS_TOPIC, 3, RETENTION_MS, "payment_completed"),
    TopicSpec(INVENTORY_EVENTS_TOPIC, 3, RETENTION_MS, "inventory_updated"),
)


def topic_for_event_type(event_type: str) -> TopicSpec:
    for spec in COMMERCE_TOPICS:
        if spec.event_type == event_type:
            return spec
    raise KeyError(event_type)
