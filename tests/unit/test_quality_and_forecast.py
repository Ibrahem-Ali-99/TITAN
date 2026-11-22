from datetime import date
from decimal import Decimal

from titan_ml.features import anomaly_scores, complete_panel, feature_rows, split_rows
from titan_quality.checks import blocking_failed, build_findings


def _order(order_id: str, total: str, currency: str = "USD") -> dict[str, object]:
    return {"order_id": order_id, "currency": currency, "total_amount": Decimal(total)}


def _item(
    order_id: str,
    amount: str,
    quantity: int = 1,
    currency: str = "USD",
) -> dict[str, object]:
    return {
        "order_id": order_id,
        "currency": currency,
        "quantity": quantity,
        "line_amount": Decimal(amount),
    }


def test_matching_lines_pass_and_an_injection_blocks_without_editing_rows() -> None:
    orders = [_order("ord_10000001", "10.00")]
    items = [_item("ord_10000001", "10.00")]
    passed = build_findings(orders, items, [], {"USD": "10.00"})
    assert not blocking_failed(passed)
    injected = build_findings(orders, items, [], {"USD": "10.00"}, injected="revenue_mismatch")
    assert blocking_failed(injected)
    failed = [item for item in injected if not item.passed]
    assert failed[0].check_id == "orders.revenue_matches_lines"
    assert "not modified" in failed[0].detail
    assert orders[0]["total_amount"] == Decimal("10.00")


def test_unmatched_payment_does_not_fail_and_a_matched_missing_order_does() -> None:
    orders = [_order("ord_10000001", "10.00")]
    items = [_item("ord_10000001", "10.00")]
    unmatched = [{"payment_id": "pay_10000001", "order_id": "ord_missing1", "order_matched": 0}]
    ok = build_findings(orders, items, unmatched, {"USD": "10.00"})
    payment = next(item for item in ok if item.check_id == "payments.matched_order_exists")
    assert payment.passed
    matched = [{"payment_id": "pay_10000002", "order_id": "ord_missing1", "order_matched": 1}]
    blocked = build_findings(orders, items, matched, {"USD": "10.00"})
    assert blocking_failed(blocked)


def test_features_use_only_earlier_days_and_the_split_is_chronological() -> None:
    raw = [("prd_100000000001", date(2026, 1, day), day) for day in range(1, 21)]
    panel = complete_panel(raw)
    rows = feature_rows(panel)
    by_date = {row["activity_date"]: row for row in rows}
    current = by_date[date(2026, 1, 10)]
    assert current["target"] == 10
    assert current["lag_1"] == 9
    assert current["lag_7"] == 3
    assert "target" not in {key for key in current if key.startswith("lag")}
    train, test = split_rows(rows)
    train_dates = [row["activity_date"] for row in train if isinstance(row["activity_date"], date)]
    test_dates = [row["activity_date"] for row in test if isinstance(row["activity_date"], date)]
    assert max(train_dates) < min(test_dates)


def test_anomaly_score_uses_earlier_days_and_skips_a_zero_mad() -> None:
    raw = [("prd_100000000001", date(2026, 1, day), 1 if day % 2 else 2) for day in range(1, 15)]
    raw.append(("prd_100000000001", date(2026, 1, 15), 50))
    scored = anomaly_scores(complete_panel(raw))
    flagged = [row for row in scored if row["flagged"]]
    assert flagged[-1]["activity_date"] == "2026-01-15"
    flat = [("prd_100000000001", date(2026, 2, day), 2) for day in range(1, 16)]
    quiet = anomaly_scores(complete_panel(flat))
    assert quiet
    assert not any(row["flagged"] for row in quiet)
