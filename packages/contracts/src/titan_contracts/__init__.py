"""Canonical event contracts shared by producers and consumers."""

from titan_contracts.events import OrderCreatedEvent
from titan_contracts.identifiers import (
    CUSTOMER_ID_PATTERN,
    ORDER_ID_PATTERN,
    PRODUCT_ID_PATTERN,
)
from titan_contracts.orders import OrderCreatedPayload, OrderItem
from titan_contracts.topics import (
    CONSUMER_GROUP,
    ORDER_EVENTS_PARTITIONS,
    ORDER_EVENTS_RETENTION_MS,
    ORDER_EVENTS_TOPIC,
)

__all__ = [
    "CONSUMER_GROUP",
    "CUSTOMER_ID_PATTERN",
    "ORDER_EVENTS_PARTITIONS",
    "ORDER_EVENTS_RETENTION_MS",
    "ORDER_EVENTS_TOPIC",
    "ORDER_ID_PATTERN",
    "OrderCreatedEvent",
    "OrderCreatedPayload",
    "OrderItem",
    "PRODUCT_ID_PATTERN",
]
