from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from titan_api.config import ApiSettings
from titan_api.main import create_app
from titan_api.platform import LagView

ROOT = Path(__file__).resolve().parents[2]


def test_metrics_use_the_route_template(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "titan_api.metrics._lag",
        lambda _bootstrap: LagView(
            consumer_group="titan-ingestion",
            available=False,
            total_lag=None,
            detail="unit test",
        ),
    )
    app = create_app(ApiSettings(database_url="sqlite://", cors_origins="http://localhost:3000"))
    client = TestClient(app)
    assert client.get("/health/live").status_code == 200
    response = client.get("/metrics")
    assert response.status_code == 200
    assert 'path="/health/live"' in response.text
    assert "titan_http_requests_total" in response.text
    assert "titan_http_request_duration_seconds" in response.text


def test_alert_rules_name_the_operational_conditions() -> None:
    rules = (ROOT / "observability" / "prometheus" / "alerts.yml").read_text(encoding="utf-8")
    for name in (
        "TitanApiDown",
        "TitanWarehouseNotReady",
        "TitanDataStale",
        "TitanConsumerLag",
        "TitanPublicationFailed",
        "TitanQualityFailed",
        "TitanPostgresExporterDown",
    ):
        assert name in rules
    assert "21600" in rules
    assert "titan_kafka_lag_measured == 1" in rules
