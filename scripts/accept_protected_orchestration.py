"""Bounded disposable protected-staging Dagster acceptance runner.

This composes the repository's existing provisioning and acceptance commands; it
does not contain fixture, TLS, or warehouse business logic.
"""

from __future__ import annotations

import argparse
import os
import secrets
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
COMPOSE_FILES = (
    "docker-compose.yml",
    "docker-compose.staging.yml",
    "docker-compose.staging-postgres-tls.yml",
    "docker-compose.staging-kafka-tls.yml",
    "docker-compose.staging-clickhouse-tls.yml",
    "docker-compose.staging-minio-tls.yml",
)
STAGES = ("preflight", "services", "fixture", "bronze", "silver", "warehouse")


def project_name() -> str:
    return f"titan-staging-accept-{secrets.token_hex(4)}"


def require_project(value: str) -> str:
    if not value.startswith("titan-staging-"):
        raise ValueError("protected acceptance requires a titan-staging-* project")
    return value


class Runner:
    def __init__(self, root: Path, project: str, timeout: int) -> None:
        self.root = root
        self.project = require_project(project)
        self.timeout = timeout
        self.evidence = root / "benchmarks" / f"protected-orchestration-{project}.log"
        self.env = os.environ.copy()
        self.env.update(
            {
                "COMPOSE_PROJECT_NAME": self.project,
                "TITAN_PROTECTED_STAGING": "true",
                "TITAN_STAGING_VOLUME_PREFIX": self.project,
                "COMPOSE_FILE": os.pathsep.join(COMPOSE_FILES),
            }
        )

    def run(self, stage: str, command: list[str]) -> str:
        started = time.monotonic()
        completed = subprocess.run(
            command,
            cwd=self.root,
            env=self.env,
            capture_output=True,
            text=True,
            timeout=self.timeout,
            check=False,
        )
        detail = (completed.stdout + completed.stderr)[-4000:]
        with self.evidence.open("a", encoding="utf-8") as stream:
            stream.write(
                f"{stage} exit={completed.returncode} seconds={time.monotonic() - started:.2f}\n"
            )
            stream.write(detail.replace("password", "[redacted]").replace("token", "[redacted]"))
            stream.write("\n")
        if completed.returncode:
            raise RuntimeError(
                f"stage {stage} failed with exit {completed.returncode}; see {self.evidence}"
            )
        return completed.stdout

    def compose(self, *args: str) -> list[str]:
        command = ["docker", "compose", "--project-name", self.project]
        for compose_file in COMPOSE_FILES:
            command.extend(("-f", compose_file))
        return command + list(args)

    def cleanup(self) -> None:
        try:
            subprocess.run(
                self.compose("down", "--volumes", "--remove-orphans"),
                cwd=self.root,
                env=self.env,
                capture_output=True,
                text=True,
                timeout=self.timeout,
                check=False,
            )
        finally:
            if self.evidence.exists():
                self.evidence.write_text(
                    self.evidence.read_text(encoding="utf-8") + "cleanup attempted\n",
                    encoding="utf-8",
                )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-name", default=None)
    parser.add_argument("--timeout", type=int, default=300)
    args = parser.parse_args(argv)
    project = require_project(args.project_name or project_name())
    runner = Runner(ROOT, project, args.timeout)
    try:
        runner.run("docker", ["docker", "info", "--format", "{{.ServerVersion}}"])
        runner.run(
            "preflight",
            [
                sys.executable,
                "scripts/staging_preflight.py",
                "--secrets-dir",
                ".staging-secrets",
                "--project-name",
                project,
                "--compose-file",
                Path(COMPOSE_FILES[0]).resolve(),
                "--compose-file",
                Path(COMPOSE_FILES[1]).resolve(),
            ],
        )
        runner.run(
            "services",
            runner.compose(
                "--profile",
                "processing",
                "up",
                "-d",
                "--wait",
                "postgres",
                "kafka",
                "clickhouse",
                "minio",
                "migrate",
                "ingestion",
                "api",
            ),
        )
        runner.run("fixture", [sys.executable, "scripts/full_pipeline_ci.py"])
        for stage, asset in (
            ("bronze", "bronze_evidence"),
            ("silver", "silver_publication"),
            ("warehouse", "clickhouse_load"),
        ):
            runner.run(stage, [sys.executable, "-m", "titan_dagster", asset])
        return 0
    finally:
        runner.cleanup()


if __name__ == "__main__":
    raise SystemExit(main())
