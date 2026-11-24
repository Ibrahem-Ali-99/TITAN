from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _dagster_service_block() -> str:
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    start = compose.index("  dagster-daemon:")
    end = compose.index("\n  event-generator:", start)
    return compose[start:end]


def test_protected_compose_does_not_mount_docker_socket() -> None:
    assert "/var/run/docker.sock" not in _dagster_service_block()


def test_dagster_has_reproducible_image_and_build_contract() -> None:
    service = _dagster_service_block()
    assert "dockerfile: infrastructure/docker/orchestration.Dockerfile" in service
    staging = (ROOT / "docker-compose.staging.yml").read_text(encoding="utf-8")
    assert "TITAN_ORCHESTRATION_IMAGE:?staging orchestration image digest is required" in staging


def test_socket_requires_explicit_local_development_override() -> None:
    override = (ROOT / "docker-compose.dagster-local.yml").read_text(encoding="utf-8")
    assert "/var/run/docker.sock:/var/run/docker.sock" in override
    assert "protected or" in override
