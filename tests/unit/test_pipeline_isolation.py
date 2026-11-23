import pytest

from pipeline_flow import _has_port, assert_state_change_is_isolated


def test_development_ports_are_not_confused_with_isolated_ports() -> None:
    assert _has_port("postgresql://titan@127.0.0.1:5435/titan", 5435)
    assert not _has_port("postgresql://titan@127.0.0.1:15435/titan", 5435)
    assert _has_port("127.0.0.1:9092", 9092)
    assert not _has_port("127.0.0.1:19092", 9092)
    assert _has_port("http://127.0.0.1:8000", 8000)
    assert not _has_port("http://127.0.0.1:18000", 8000)


def test_state_changing_check_refuses_the_development_project(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("COMPOSE_PROJECT_NAME", raising=False)
    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql+psycopg://titan:titan_dev_password@127.0.0.1:15435/titan",
    )
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:19092")
    monkeypatch.setenv("API_BASE_URL", "http://127.0.0.1:18000")
    with pytest.raises(RuntimeError, match="isolated project"):
        assert_state_change_is_isolated()


def test_state_changing_check_refuses_development_ports(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("COMPOSE_PROJECT_NAME", "titan-e2e")
    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql+psycopg://titan:titan_dev_password@127.0.0.1:5435/titan",
    )
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:19092")
    monkeypatch.setenv("API_BASE_URL", "http://127.0.0.1:18000")
    with pytest.raises(RuntimeError, match="5435"):
        assert_state_change_is_isolated()
