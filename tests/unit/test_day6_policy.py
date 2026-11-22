from typing import Any

from fastapi.testclient import TestClient

from titan_api.config import ApiSettings
from titan_api.main import create_app
from titan_messaging.producer import PublishError, acknowledgement_is_uncertain
from titan_ml.selection import choose_strategy, recursive_seasonal
from titan_warehouse.recovery import PublicationEvidence, decide_recovery, fact_count, is_verified


def test_a_missing_fact_group_counts_as_zero() -> None:
    assert fact_count({}, "payments") == 0
    assert fact_count({"orders": 11}, "orders") == 11
    orders_only = PublicationEvidence(
        publication_id="orders-only",
        published=True,
        dbt_passed=True,
        quality_passed=True,
        silver_published=True,
        order_count=11,
        payment_count=0,
        inventory_count=0,
        item_count=24,
        clickhouse_orders=11,
        clickhouse_payments=fact_count({}, "payments"),
        clickhouse_inventory=fact_count({}, "inventory"),
        clickhouse_items=24,
    )
    assert is_verified(orders_only)


def test_a_worse_candidate_is_not_the_served_method() -> None:
    choice = choose_strategy(2.3, 1.4)
    assert choice.strategy == "seasonal_naive"
    assert "not served" in choice.rationale
    better = choose_strategy(1.0, 1.4)
    assert better.strategy == "hist_gradient_boosting"


def test_recursive_seasonal_does_not_read_a_future_actual() -> None:
    history = [float(value) for value in range(10)]
    forecasts = recursive_seasonal(history, 8)
    assert forecasts[0] == history[-7]
    assert forecasts[7] == forecasts[0]


def _evidence(
    publication_id: str,
    *,
    published: bool = True,
    dbt_passed: bool = True,
    quality_passed: bool = True,
    silver_published: bool = True,
    clickhouse_payments: int = 8,
) -> PublicationEvidence:
    return PublicationEvidence(
        publication_id=publication_id,
        published=published,
        dbt_passed=dbt_passed,
        quality_passed=quality_passed,
        silver_published=silver_published,
        order_count=10,
        payment_count=8,
        inventory_count=6,
        item_count=10,
        clickhouse_orders=10,
        clickhouse_payments=clickhouse_payments,
        clickhouse_inventory=6,
        clickhouse_items=10,
    )


def test_recovery_reverts_an_unrecorded_pointer_and_ignores_a_match() -> None:
    published = [_evidence("good"), _evidence("older")]
    reverted = decide_recovery("candidate", published)
    assert reverted.action == "exchange"
    assert reverted.publication_id == "good"
    assert decide_recovery("good", published).action == "none"
    assert decide_recovery("candidate", []).action == "refuse"


def test_recovery_skips_incomplete_publications() -> None:
    newer_orders_only = _evidence("partial", clickhouse_payments=0, quality_passed=False)
    missing_dbt = _evidence("no-dbt", dbt_passed=False)
    missing_quality = _evidence("no-quality", quality_passed=False)
    missing_silver = _evidence("no-silver", silver_published=False)
    verified = _evidence("complete")
    assert not is_verified(newer_orders_only)
    assert not is_verified(missing_dbt)
    assert not is_verified(missing_quality)
    assert not is_verified(missing_silver)
    decision = decide_recovery(
        "partial",
        [newer_orders_only, missing_dbt, missing_quality, missing_silver, verified],
    )
    assert decision.action == "exchange"
    assert decision.publication_id == "complete"
    assert decide_recovery("partial", [newer_orders_only]).action == "refuse"


def test_a_buffered_produce_is_not_a_definite_rejection() -> None:
    assert acknowledgement_is_uncertain(None, 1)
    assert not acknowledgement_is_uncertain("unknown topic", 0)
    error = PublishError("not acknowledged", uncertain=True)
    assert error.uncertain


def test_replay_is_closed_when_the_admin_token_is_unset(monkeypatch: Any) -> None:
    monkeypatch.delenv("TITAN_ADMIN_TOKEN", raising=False)
    app = create_app(ApiSettings(database_url="sqlite://", cors_origins="http://localhost:3000"))
    denied = TestClient(app).post("/api/v1/ingestion/replays", json={})
    assert denied.status_code == 401


def test_replay_requires_a_configured_admin_token() -> None:
    import os

    os.environ["TITAN_ADMIN_TOKEN"] = "local-secret"
    try:
        app = create_app(
            ApiSettings(database_url="sqlite://", cors_origins="http://localhost:3000")
        )
        client = TestClient(app)
        denied = client.post("/api/v1/ingestion/replays", json={})
        assert denied.status_code == 401
        allowed = client.post(
            "/api/v1/ingestion/replays",
            json={},
            headers={"Authorization": "Bearer local-secret"},
        )
        assert allowed.status_code != 401
    finally:
        os.environ.pop("TITAN_ADMIN_TOKEN", None)
