"""Canonical event contracts shared by producers and consumers."""

from titan_contracts.events import (
    CommerceEvent,
    InventoryUpdatedEvent,
    OrderCreatedEvent,
    PaymentCompletedEvent,
    business_key,
    order_reference,
)
from titan_contracts.identifiers import (
    CUSTOMER_ID_PATTERN,
    ORDER_ID_PATTERN,
    PAYMENT_ID_PATTERN,
    PRODUCT_ID_PATTERN,
)
from titan_contracts.inventory import InventoryUpdatedPayload
from titan_contracts.orders import OrderCreatedPayload, OrderItem
from titan_contracts.payments import PaymentCompletedPayload
from titan_contracts.topics import (
    COMMERCE_TOPICS,
    CONSUMER_GROUP,
    ORDER_EVENTS_PARTITIONS,
    ORDER_EVENTS_RETENTION_MS,
    ORDER_EVENTS_TOPIC,
)

__all__ = [
    "COMMERCE_TOPICS",
    "CONSUMER_GROUP",
    "CUSTOMER_ID_PATTERN",
    "CommerceEvent",
    "InventoryUpdatedEvent",
    "InventoryUpdatedPayload",
    "ORDER_EVENTS_PARTITIONS",
    "ORDER_EVENTS_RETENTION_MS",
    "ORDER_EVENTS_TOPIC",
    "ORDER_ID_PATTERN",
    "OrderCreatedEvent",
    "OrderCreatedPayload",
    "OrderItem",
    "PAYMENT_ID_PATTERN",
    "PaymentCompletedEvent",
    "PaymentCompletedPayload",
    "PRODUCT_ID_PATTERN",
    "business_key",
    "order_reference",
]
