"""Inventory snapshot. Repeated updates for one product are state changes."""

import re
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from titan_contracts.identifiers import ORDER_ID_PATTERN, PRODUCT_ID_PATTERN, WAREHOUSE_ID_PATTERN

MAX_STOCK = 1_000_000


class InventoryUpdatedPayload(BaseModel):
    """On-hand quantity and a delta are both recorded.

    The event does not include the previous on-hand value, so Day 2 does not
    recompute one field from the other. Both must simply be inside range.
    """

    model_config = ConfigDict(extra="forbid")

    product_id: str = Field(pattern=PRODUCT_ID_PATTERN)
    warehouse_id: str = Field(pattern=WAREHOUSE_ID_PATTERN)
    quantity_on_hand: int = Field(ge=0, le=MAX_STOCK)
    delta: int = Field(ge=-MAX_STOCK, le=MAX_STOCK)
    order_id: str | None = None

    @field_validator("order_id")
    @classmethod
    def order_reference_must_match(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if re.fullmatch(ORDER_ID_PATTERN, value) is None:
            raise ValueError("order_id is invalid")
        return value

    @field_validator("quantity_on_hand", "delta", mode="before")
    @classmethod
    def counters_must_be_ints(cls, value: object) -> object:
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError("inventory counts must be integers")
        return value

    @model_validator(mode="after")
    def empty_order_is_absent(self) -> Self:
        if self.order_id == "":
            raise ValueError("order_id must be omitted rather than blank")
        return self
