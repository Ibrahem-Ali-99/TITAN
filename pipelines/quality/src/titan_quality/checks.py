"""Quality findings for one warehouse candidate. These functions do not query a database."""

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class Finding:
    check_id: str
    check_type: str
    dataset: str
    severity: str
    blocks_publication: bool
    observed: str
    expected: str
    passed: bool
    business_id: str | None
    detail: str


CURRENCIES = {"USD", "EUR", "GBP"}


def build_findings(
    orders: list[dict[str, object]],
    items: list[dict[str, object]],
    payments: list[dict[str, object]],
    expected_revenue: dict[str, str],
    *,
    injected: str | None = None,
    event_age_days: int | None = None,
    warehouse_age_seconds: int | None = None,
    accepted_order_count: int | None = None,
) -> list[Finding]:
    """Evaluate a candidate. ``injected`` is a labeled test fault, not a data edit."""
    findings = [
        _unique_orders(orders),
        _currencies(orders),
        _quantities(items),
        _line_totals(orders, items, injected),
        _revenue_matches_expected(orders, expected_revenue),
        _payments(payments, orders),
        _event_age(event_age_days),
        _warehouse_age(warehouse_age_seconds),
        _postgres_gap(accepted_order_count, len(orders)),
    ]
    return findings


def _as_int(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int | str | Decimal):
        raise TypeError("expected an integer quantity or flag")
    return int(value)


def blocking_failed(findings: list[Finding]) -> bool:
    return any(item.blocks_publication and not item.passed for item in findings)


def _finding(
    check_id: str,
    check_type: str,
    dataset: str,
    severity: str,
    blocks: bool,
    observed: str,
    expected: str,
    passed: bool,
    detail: str,
    business_id: str | None = None,
) -> Finding:
    return Finding(
        check_id,
        check_type,
        dataset,
        severity,
        blocks,
        observed,
        expected,
        passed,
        business_id,
        detail,
    )


def _unique_orders(orders: list[dict[str, object]]) -> Finding:
    seen: set[str] = set()
    duplicate = None
    for order in orders:
        order_id = str(order["order_id"])
        if order_id in seen and duplicate is None:
            duplicate = order_id
        seen.add(order_id)
    return _finding(
        "orders.unique_order_id",
        "identity",
        "wh_orders",
        "critical",
        True,
        "0" if duplicate is None else "1",
        "0 duplicate accepted order_id values",
        duplicate is None,
        "Accepted orders are one row per order_id in this publication.",
        duplicate,
    )


def _currencies(orders: list[dict[str, object]]) -> Finding:
    bad = [str(order["order_id"]) for order in orders if str(order["currency"]) not in CURRENCIES]
    return _finding(
        "orders.currency",
        "contract",
        "wh_orders",
        "critical",
        True,
        str(len(bad)),
        "currency in USD, EUR, GBP",
        not bad,
        "Currencies are not converted or added together.",
        None if not bad else bad[0],
    )


def _quantities(items: list[dict[str, object]]) -> Finding:
    bad = [str(item["order_id"]) for item in items if _as_int(item["quantity"]) < 1]
    return _finding(
        "items.positive_quantity",
        "business",
        "wh_order_items",
        "major",
        True,
        str(len(bad)),
        "quantity >= 1",
        not bad,
        "A non-positive line does not represent a sale.",
        None if not bad else bad[0],
    )


def _line_totals(
    orders: list[dict[str, object]],
    items: list[dict[str, object]],
    injected: str | None,
) -> Finding:
    if injected == "revenue_mismatch":
        return _finding(
            "orders.revenue_matches_lines",
            "reconciliation",
            "wh_orders",
            "critical",
            True,
            "injected mismatch",
            "sum of line_amount equals total_amount",
            False,
            "Intentional test injection. Accepted rows were not modified.",
        )
    totals: dict[tuple[str, str], Decimal] = {}
    for item in items:
        key = (str(item["order_id"]), str(item["currency"]))
        totals[key] = totals.get(key, Decimal("0")) + Decimal(str(item["line_amount"]))
    mismatch = None
    for order in orders:
        key = (str(order["order_id"]), str(order["currency"]))
        if totals.get(key, Decimal("0")) != Decimal(str(order["total_amount"])):
            mismatch = key[0]
            break
    return _finding(
        "orders.revenue_matches_lines",
        "reconciliation",
        "wh_orders",
        "critical",
        True,
        "0" if mismatch is None else "1",
        "sum of line_amount equals total_amount",
        mismatch is None,
        "Compared inside each order and currency.",
        mismatch,
    )


def _revenue_matches_expected(
    orders: list[dict[str, object]],
    expected_revenue: dict[str, str],
) -> Finding:
    totals: dict[str, Decimal] = {}
    for order in orders:
        currency = str(order["currency"])
        totals[currency] = totals.get(currency, Decimal("0")) + Decimal(str(order["total_amount"]))
    observed = {
        key: format(value.quantize(Decimal("0.01")), "f") for key, value in sorted(totals.items())
    }
    expected = {
        key: format(Decimal(value).quantize(Decimal("0.01")), "f")
        for key, value in sorted(expected_revenue.items())
    }
    return _finding(
        "orders.silver_clickhouse_revenue",
        "reconciliation",
        "wh_orders",
        "critical",
        True,
        str(observed),
        str(expected),
        observed == expected,
        "Candidate ClickHouse sums against the Silver revenue stored on the load.",
    )


def _payments(payments: list[dict[str, object]], orders: list[dict[str, object]]) -> Finding:
    """A payment with order_matched=1 must find that order. Unmatched payments do not fail."""
    order_ids = {str(order["order_id"]) for order in orders}
    broken = None
    unmatched = 0
    for payment in payments:
        matched = _as_int(payment["order_matched"]) == 1
        if not matched:
            unmatched += 1
            continue
        if str(payment["order_id"]) not in order_ids and broken is None:
            broken = str(payment["payment_id"])
    return _finding(
        "payments.matched_order_exists",
        "relationship",
        "wh_payments",
        "major",
        True,
        "0" if broken is None else "1",
        "order_matched payments reference an accepted order; unmatched payments are allowed",
        broken is None,
        f"Unmatched payments in this publication: {unmatched}. Partial payments are not summed.",
        broken,
    )


def _event_age(event_age_days: int | None) -> Finding:
    observed = "unknown" if event_age_days is None else str(event_age_days)
    return _finding(
        "freshness.event_age_days",
        "freshness",
        "wh_orders",
        "informational",
        False,
        observed,
        "recorded only; backdated synthetic occurred_at is not an ingestion failure",
        True,
        "event age is now minus max(occurred_at). It is not pipeline delay.",
    )


def _warehouse_age(warehouse_age_seconds: int | None) -> Finding:
    stale = warehouse_age_seconds is not None and warehouse_age_seconds > 6 * 3600
    observed = "unknown" if warehouse_age_seconds is None else str(warehouse_age_seconds)
    return _finding(
        "freshness.warehouse_publication_age_seconds",
        "freshness",
        "wh_active",
        "major" if stale else "informational",
        False,
        observed,
        "alert when the active publication is older than 6 hours; does not block",
        not stale,
        "Uses published_at, not occurred_at.",
    )


def _postgres_gap(accepted_order_count: int | None, warehouse_orders: int) -> Finding:
    if accepted_order_count is None:
        return _finding(
            "reconciliation.postgres_accepted_orders",
            "reconciliation",
            "commerce_events",
            "informational",
            False,
            "unknown",
            "warehouse orders equal accepted PostgreSQL orders when Silver is current",
            True,
            "PostgreSQL count was not supplied.",
        )
    gap = accepted_order_count - warehouse_orders
    return _finding(
        "reconciliation.postgres_accepted_orders",
        "reconciliation",
        "commerce_events",
        "informational",
        False,
        str(gap),
        "0 when this publication includes every currently accepted order",
        True,
        "A positive gap means Silver has not been republished. It does not block this candidate.",
    )
