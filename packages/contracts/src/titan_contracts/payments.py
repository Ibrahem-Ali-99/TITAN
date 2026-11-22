"""Payment payload. Money uses the same decimal policy as orders."""

from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator

from titan_contracts.identifiers import CUSTOMER_ID_PATTERN, ORDER_ID_PATTERN, PAYMENT_ID_PATTERN
from titan_contracts.money import SUPPORTED_CURRENCIES, parse_money


class PaymentCompletedPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    payment_id: str = Field(pattern=PAYMENT_ID_PATTERN)
    order_id: str = Field(pattern=ORDER_ID_PATTERN)
    customer_id: str = Field(pattern=CUSTOMER_ID_PATTERN)
    currency: Literal["USD", "EUR", "GBP"]
    amount: Decimal
    method: Literal["card", "bank_transfer", "wallet"]

    @field_validator("currency", mode="before")
    @classmethod
    def currency_must_be_supported(cls, value: object) -> object:
        if isinstance(value, str) and value not in SUPPORTED_CURRENCIES:
            raise ValueError("currency is not supported")
        return value

    @field_validator("amount", mode="before")
    @classmethod
    def amount_must_be_decimal(cls, value: object) -> Decimal:
        return parse_money(value)

    @field_serializer("amount", when_used="json")
    def serialize_amount(self, value: Decimal) -> str:
        return format(value, "f")
