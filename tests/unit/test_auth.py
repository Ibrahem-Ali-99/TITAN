from fastapi.testclient import TestClient

from titan_api.config import ApiSettings
from titan_api.main import create_app
from titan_store.models import Base


def _client() -> TestClient:
    app = create_app(ApiSettings(
        database_url="sqlite://", cors_origins="http://localhost:3000",
        read_token="read-token", write_token="write-token", admin_token="admin-token",
    ))
    Base.metadata.create_all(app.state.engine)
    return TestClient(app)


def test_health_is_public_but_business_reads_require_auth(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("TITAN_RAW_AUTH_TEST", "1")
    client = _client()
    assert client.get("/health/live").status_code == 200
    assert client.get("/api/v1/ingestion/stats").status_code == 401
    assert client.get(
        "/api/v1/ingestion/stats", headers={"Authorization": "Bearer read-token"}
    ).status_code == 200


def test_read_scope_cannot_publish(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("TITAN_RAW_AUTH_TEST", "1")
    response = _client().post(
        "/api/v1/ingestion/events",
        json={"event_type": "invalid"},
        headers={"Authorization": "Bearer read-token"},
    )
    assert response.status_code == 403


def test_write_scope_can_publish_but_not_admin_replay(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("TITAN_RAW_AUTH_TEST", "1")
    client = _client()
    assert client.post(
        "/api/v1/ingestion/events",
        json={"event_type": "invalid"},
        headers={"Authorization": "Bearer write-token"},
    ).status_code == 422
    assert client.post(
        "/api/v1/ingestion/replays",
        json={},
        headers={"Authorization": "Bearer write-token"},
    ).status_code == 403


def test_missing_configured_tokens_fail_closed(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("TITAN_RAW_AUTH_TEST", "1")
    monkeypatch.delenv("TITAN_READ_TOKEN", raising=False)
    monkeypatch.delenv("TITAN_WRITE_TOKEN", raising=False)
    monkeypatch.delenv("TITAN_ADMIN_TOKEN", raising=False)
    client = TestClient(create_app(ApiSettings(database_url="sqlite://", cors_origins="http://localhost:3000")))
    assert client.get("/api/v1/ingestion/stats").status_code == 503
