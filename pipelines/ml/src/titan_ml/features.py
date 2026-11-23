"""One-step demand features. Lag and rolling values use only earlier days."""

from collections import defaultdict
from datetime import date, timedelta
from statistics import median

FEATURE_VERSION = "demand-daily-v1"
TEST_DAYS = 7
MINIMUM_DATES = 14


def complete_panel(rows: list[tuple[str, date, int]]) -> list[tuple[str, date, int]]:
    """Fill missing product-days with zero quantity. Dates stay inside the observed span."""
    if not rows:
        return []
    by_product: dict[str, dict[date, int]] = defaultdict(dict)
    for product_id, activity_date, quantity in rows:
        current = by_product[product_id].get(activity_date, 0)
        by_product[product_id][activity_date] = current + quantity
    start = min(activity_date for _, activity_date, _ in rows)
    end = max(activity_date for _, activity_date, _ in rows)
    panel: list[tuple[str, date, int]] = []
    for product_id, quantities in sorted(by_product.items()):
        cursor = start
        while cursor <= end:
            panel.append((product_id, cursor, quantities.get(cursor, 0)))
            cursor += timedelta(days=1)
    return panel


def feature_rows(panel: list[tuple[str, date, int]]) -> list[dict[str, object]]:
    """Target is quantity on day t. Features are computed from days before t."""
    grouped: dict[str, list[tuple[date, int]]] = defaultdict(list)
    for product_id, activity_date, quantity in panel:
        grouped[product_id].append((activity_date, quantity))
    rows: list[dict[str, object]] = []
    for product_id, series in grouped.items():
        series.sort()
        quantities = [quantity for _, quantity in series]
        rolling_sum = 0
        for index, (activity_date, quantity) in enumerate(series):
            # Keep the feature window bounded.  Rebuilding ``quantities[:index]``
            # for every row made this path quadratic for long product histories.
            lag_1 = quantities[index - 1] if index else 0
            lag_7 = quantities[index - 7] if index >= 7 else 0
            window_size = min(index, 7)
            rolling = rolling_sum / window_size if window_size else 0.0
            rows.append(
                {
                    "product_id": product_id,
                    "activity_date": activity_date,
                    "target": quantity,
                    "lag_1": lag_1,
                    "lag_7": lag_7,
                    "rolling_7": rolling,
                    "day_of_week": activity_date.weekday(),
                }
            )
            rolling_sum += quantity
            if index >= 7:
                rolling_sum -= quantities[index - 7]
    return rows


def split_rows(
    rows: list[dict[str, object]],
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """Last seven observed dates are the test window. Earlier rows with a real lag_7 train."""
    dates = sorted({row["activity_date"] for row in rows if isinstance(row["activity_date"], date)})
    if len(dates) < MINIMUM_DATES:
        raise ValueError(f"need at least {MINIMUM_DATES} distinct dates, found {len(dates)}")
    test_start = dates[-TEST_DAYS]
    train = [
        row
        for row in rows
        if isinstance(row["activity_date"], date)
        and row["activity_date"] < test_start
        and _has_history(row)
    ]
    test = [
        row
        for row in rows
        if isinstance(row["activity_date"], date) and row["activity_date"] >= test_start
    ]
    if not train or not test:
        raise ValueError("chronological split produced an empty train or test window")
    return train, test


def _has_history(row: dict[str, object]) -> bool:
    return (
        _number(row["lag_1"]) != 0 or _number(row["lag_7"]) != 0 or _number(row["rolling_7"]) != 0.0
    )


def _number(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise TypeError("feature is not numeric")
    return float(value)


def mae(actual: list[float], predicted: list[float]) -> float:
    if not actual:
        raise ValueError("mae requires at least one row")
    total = sum(abs(left - right) for left, right in zip(actual, predicted, strict=True))
    return total / len(actual)


def wape(actual: list[float], predicted: list[float]) -> float | None:
    denom = sum(abs(value) for value in actual)
    if denom == 0:
        return None
    numer = sum(abs(left - right) for left, right in zip(actual, predicted, strict=True))
    return numer / denom


def seasonal_naive(rows: list[dict[str, object]]) -> list[float]:
    return [_number(row["lag_7"]) for row in rows]


def anomaly_scores(
    panel: list[tuple[str, date, int]],
    *,
    window: int = 14,
    threshold: float = 3.5,
) -> list[dict[str, object]]:
    """Robust z-score from the previous window only. A zero MAD does not invent a flag."""
    grouped: dict[str, list[tuple[date, int]]] = defaultdict(list)
    for product_id, activity_date, quantity in panel:
        grouped[product_id].append((activity_date, quantity))
    scored: list[dict[str, object]] = []
    for product_id, series in grouped.items():
        series.sort()
        quantities = [quantity for _, quantity in series]
        for index, (activity_date, quantity) in enumerate(series):
            history = quantities[max(0, index - window) : index]
            if len(history) < window:
                continue
            baseline = float(median(history))
            deviations = [abs(value - baseline) for value in history]
            mad = float(median(deviations))
            score: float | None
            flagged = False
            detail = f"rolling median of the previous {window} days"
            if mad == 0:
                score = None
                detail = "MAD is zero, so no scale is available and the day is not flagged"
            else:
                score = 0.6745 * (quantity - baseline) / mad
                flagged = abs(score) > threshold
            scored.append(
                {
                    "product_id": product_id,
                    "activity_date": activity_date.isoformat(),
                    "observed": str(quantity),
                    "baseline": format(baseline, ".3f"),
                    "score": None if score is None else format(score, ".3f"),
                    "flagged": flagged,
                    "detail": detail,
                }
            )
    return scored
