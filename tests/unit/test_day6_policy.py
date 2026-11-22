from fastapi.testclient import TestClient

from titan_api.config import ApiSettings
from titan_api.main import create_app
from titan_messaging.producer import PublishError, acknowledgement_is_uncertain
from titan_ml.selection import choose_strategy, recursive_seasonal
from titan_warehouse.recovery import PublishedPointer, decide_recovery


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


def test_recovery_reverts_an_unrecorded_pointer_and_ignores_a_match() -> None:
    published = [PublishedPointer("good", True), PublishedPointer("older", True)]
    reverted = decide_recovery("candidate", published)
    assert reverted.action == "exchange"
    assert reverted.publication_id == "good"
    assert decide_recovery("good", published).action == "none"
    assert decide_recovery("candidate", []).action == "refuse"


def test_a_buffered_produce_is_not_a_definite_rejection() -> None:
    assert acknowledgement_is_uncertain(None, 1)
    assert not acknowledgement_is_uncertain("unknown topic", 0)
    error = PublishError("not acknowledged", uncertain=True)
    assert error.uncertain


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
