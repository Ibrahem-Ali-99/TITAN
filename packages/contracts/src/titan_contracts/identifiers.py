"""Identifier conventions for commerce events.

Prefixes are stable so later payment, inventory, and customer events can
reuse the same family without colliding:

- ``ord_`` order
- ``cus_`` customer
- ``prd_`` product
- ``pay_`` payment (reserved)
- ``inv_`` inventory (reserved)
"""

from typing import Literal

ORDER_ID_PATTERN = r"^ord_[A-Za-z0-9]{8,32}$"
CUSTOMER_ID_PATTERN = r"^cus_[A-Za-z0-9]{8,32}$"
PRODUCT_ID_PATTERN = r"^prd_[A-Za-z0-9]{8,32}$"

SYNTHETIC_SOURCE = "synthetic.event-generator"
SCHEMA_VERSION: Literal[1] = 1
ORDER_CREATED: Literal["order_created"] = "order_created"
