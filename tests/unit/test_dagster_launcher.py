import os

import pytest

from titan_dagster.launcher import compose_command  # type: ignore[import-not-found]


def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TITAN_PROTECTED_STAGING", "true")
    monkeypatch.setenv("COMPOSE_PROJECT_NAME", "titan-staging-test")
    monkeypatch.setenv(
        "COMPOSE_FILE",
        os.pathsep.join(
            [
                "docker-compose.yml",
                "docker-compose.staging.yml",
                "docker-compose.staging-postgres-tls.yml",
            ]
        ),
    )


def test_launcher_allows_only_staging_spark_services(monkeypatch: pytest.MonkeyPatch) -> None:
    _env(monkeypatch)
    command = compose_command("spark-bronze")
    assert command[-1] == "spark-bronze"
    assert "--project-name" in command
    assert "titan-staging-test" in command


@pytest.mark.parametrize("service", ["postgres", "api", "spark-tests"])
def test_launcher_rejects_unapproved_services(
    monkeypatch: pytest.MonkeyPatch, service: str
) -> None:
    _env(monkeypatch)
    with pytest.raises(RuntimeError, match="rejected service"):
        compose_command(service)


def test_launcher_rejects_wrong_project(monkeypatch: pytest.MonkeyPatch) -> None:
    _env(monkeypatch)
    monkeypatch.setenv("COMPOSE_PROJECT_NAME", "titan")
    with pytest.raises(RuntimeError, match="isolated"):
        compose_command("spark-silver")


def test_launcher_rejects_unapproved_compose_file(monkeypatch: pytest.MonkeyPatch) -> None:
    _env(monkeypatch)
    monkeypatch.setenv(
        "COMPOSE_FILE", os.pathsep.join(["docker-compose.yml", "docker-compose.e2e.yml"])
    )
    with pytest.raises(RuntimeError, match="unapproved"):
        compose_command("spark-silver")
