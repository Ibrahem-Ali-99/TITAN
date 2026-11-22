"""Order payload for the ``order_created`` event."""

from decimal import Decimal
from typing import Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_serializer,
    field_validator,
    model_validator,
)

from titan_contracts.identifiers import CUSTOMER_ID_PATTERN, ORDER_ID_PATTERN, PRODUCT_ID_PATTERN
from titan_contracts.money import (
    MAX_ITEMS,
    MAX_LINE_QUANTITY,
    SUPPORTED_CURRENCIES,
    line_amount,
    order_total,
    parse_money,
)


class OrderItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    product_id: str = Field(pattern=PRODUCT_ID_PATTERN)
    quantity: int = Field(ge=1, le=MAX_LINE_QUANTITY)
    unit_price: Decimal

    @field_validator("quantity", mode="before")
    @classmethod
    def quantity_must_be_int(cls, value: object) -> object:
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError("quantity must be a positive integer")
        return value

    @field_validator("unit_price", mode="before")
    @classmethod
    def price_must_be_decimal(cls, value: object) -> Decimal:
        return parse_money(value)

    @field_serializer("unit_price", when_used="json")
    def serialize_unit_price(self, value: Decimal) -> str:
        return format(value, "f")

    @property
    def amount(self) -> Decimal:
        return line_amount(self.quantity, self.unit_price)


class OrderCreatedPayload(BaseModel):
    """One currency per order. Totals are the exact sum of line amounts."""

    model_config = ConfigDict(extra="forbid")

    order_id: str = Field(pattern=ORDER_ID_PATTERN)
    customer_id: str = Field(pattern=CUSTOMER_ID_PATTERN)
    items: list[OrderItem] = Field(min_length=1, max_length=MAX_ITEMS)
    currency: Literal["USD", "EUR", "GBP"]
    total_amount: Decimal

    @field_validator("currency", mode="before")
    @classmethod
    def currency_must_be_supported(cls, value: object) -> object:
        if isinstance(value, str) and value not in SUPPORTED_CURRENCIES:
            raise ValueError("currency is not supported")
        return value

    @field_validator("total_amount", mode="before")
    @classmethod
    def total_must_be_decimal(cls, value: object) -> Decimal:
        return parse_money(value)

    @field_serializer("total_amount", when_used="json")
    def serialize_total(self, value: Decimal) -> str:
        return format(value, "f")

    @model_validator(mode="after")
    def total_matches_lines(self) -> Self:
        expected = order_total((item.quantity, item.unit_price) for item in self.items)
        if self.total_amount != expected:
            raise ValueError(
                f"total_amount {self.total_amount} does not equal "
                f"the sum of line amounts {expected}"
            )
        return self
