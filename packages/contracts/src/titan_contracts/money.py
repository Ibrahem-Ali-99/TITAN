"""Decimal money policy for Day 1 commerce events.

Supported currencies are USD, EUR, and GBP. Each has two minor units.
Amounts must already be expressed in those minor units. This module does
not silently round. ``ROUND_HALF_UP`` is the documented commercial rule
for any future component that must derive a minor-unit amount; Day 1
validation rejects values with more than two decimal places instead of
applying that rounding itself.

Line amount is ``unit_price * quantity``. ``unit_price`` is a positive
decimal with at most two places and ``quantity`` is a positive integer,
so the product is exact. ``total_amount`` must equal the sum of line
amounts exactly. There is no tax, discount, or tolerance on Day 1.
Money is serialized as decimal strings, never as JSON floats.
"""

from collections.abc import Iterable
from decimal import Decimal, InvalidOperation

MONEY_QUANTUM = Decimal("0.01")
MINOR_UNITS = 2
MAX_UNIT_PRICE = Decimal("1000000.00")
MAX_LINE_QUANTITY = 10_000
MAX_ITEMS = 50
SUPPORTED_CURRENCIES = ("USD", "EUR", "GBP")


def parse_money(value: object) -> Decimal:
    """Parse a positive money amount and reject floats and extra scale."""
    if isinstance(value, (bool, float)):
        raise ValueError("money must be a decimal string or Decimal, not a float or boolean")
    if isinstance(value, Decimal):
        amount = value
    elif isinstance(value, int):
        amount = Decimal(value)
    elif isinstance(value, str):
        try:
            amount = Decimal(value)
        except InvalidOperation as exc:
            raise ValueError("money is not a valid decimal") from exc
    else:
        raise ValueError("money must be a decimal string or Decimal")
    if not amount.is_finite():
        raise ValueError("money must be finite")
    if amount <= 0:
        raise ValueError("money must be positive")
    exponent = amount.as_tuple().exponent
    if not isinstance(exponent, int) or -exponent > MINOR_UNITS:
        raise ValueError("money must have at most 2 decimal places")
    if amount > MAX_UNIT_PRICE:
        raise ValueError("money exceeds the supported maximum")
    return amount.quantize(MONEY_QUANTUM)


def line_amount(quantity: int, unit_price: Decimal) -> Decimal:
    return unit_price * quantity


def order_total(lines: Iterable[tuple[int, Decimal]]) -> Decimal:
    return sum((line_amount(quantity, price) for quantity, price in lines), Decimal("0"))
