"""Validated host-side launcher boundary for protected staging Spark jobs."""

from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
ALLOWED_SERVICES = frozenset({"spark-bronze", "spark-silver"})
ALLOWED_COMPOSE_FILES = frozenset(
    {
        "docker-compose.yml",
        "docker-compose.staging.yml",
        "docker-compose.staging-postgres-tls.yml",
        "docker-compose.staging-kafka-tls.yml",
        "docker-compose.staging-clickhouse-tls.yml",
        "docker-compose.staging-minio-tls.yml",
        "docker-compose.staging-ingress.yml",
    }
)


def _compose_files() -> list[Path]:
    raw = os.environ.get("COMPOSE_FILE", "")
    files = [Path(item).resolve() for item in raw.split(os.pathsep) if item]
    if not files or files[0] != (ROOT / "docker-compose.yml").resolve():
        raise RuntimeError("protected Dagster launcher requires docker-compose.yml first")
    if any(path.parent != ROOT or path.name not in ALLOWED_COMPOSE_FILES for path in files):
        raise RuntimeError("protected Dagster launcher rejected an unapproved Compose file")
    return files


def compose_command(service: str) -> list[str]:
    """Return a fixed Docker Compose command for one approved staging service."""
    if service not in ALLOWED_SERVICES:
        raise RuntimeError(f"protected Dagster launcher rejected service: {service}")
    if os.environ.get("TITAN_PROTECTED_STAGING", "").lower() != "true":
        raise RuntimeError("protected Dagster launcher requires TITAN_PROTECTED_STAGING=true")
    project = os.environ.get("COMPOSE_PROJECT_NAME", "")
    if not project.startswith("titan-staging-"):
        raise RuntimeError(
            "protected Dagster launcher requires an isolated titan-staging-* project"
        )
    command = ["docker", "compose", "--project-name", project]
    for path in _compose_files():
        command.extend(("-f", str(path)))
    command.extend(("--profile", "processing", "run", "--rm", service))
    return command
