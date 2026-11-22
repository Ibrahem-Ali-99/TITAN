# Money policy

Day 1 supports USD, EUR, and GBP. Each has two minor units.

- Amounts are `Decimal` values. JSON encodes them as strings. Floats are rejected because binary floating point cannot represent minor units exactly.
- `unit_price` must be positive, finite, and already expressed with at most two decimal places. Values with extra scale are rejected. Day 1 does not silently round.
- `quantity` is an integer from 1 through 10000. Zero and negative quantities are rejected.
- A line amount is `unit_price * quantity`. With the constraints above, that product is exact.
- `total_amount` must equal the sum of line amounts exactly. There is no tax, discount, shipping, or tolerance.
- `ROUND_HALF_UP` to two decimal places is the rule if a later milestone has to derive a minor-unit amount. Validation does not apply it today.
- One currency field covers the whole order.

The generator builds totals with the same function the model uses, so a seeded order that fails validation is a generator bug.
