"""Commerce topics read by Spark.

These names must stay aligned with ``titan_contracts.topics``. Spark does not
import that package; a host test compares the two lists.
"""

COMMERCE_TOPIC_NAMES: tuple[str, ...] = (
    "titan.commerce.order-events",
    "titan.commerce.payment-events",
    "titan.commerce.inventory-events",
)

ORDER_CREATED = "order_created"
PAYMENT_COMPLETED = "payment_completed"
INVENTORY_UPDATED = "inventory_updated"
KNOWN_EVENT_TYPES: frozenset[str] = frozenset({ORDER_CREATED, PAYMENT_COMPLETED, INVENTORY_UPDATED})
