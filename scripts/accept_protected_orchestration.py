"""Bounded disposable protected-staging Dagster acceptance runner.

This composes the repository's existing provisioning and acceptance commands; it
does not contain fixture, TLS, or warehouse business logic.
"""
# ruff: noqa: E501

from __future__ import annotations

import argparse
import os
import secrets
import shutil
import subprocess
import sys
import tempfile
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

    def bootstrap(self, work: Path) -> None:
        secrets_dir = work / "secrets"
        tls_dir = work / "tls"
        kafka_dir = work / "kafka"
        secrets_dir.mkdir()
        for name in (
            "POSTGRES_PASSWORD",
            "CLICKHOUSE_PASSWORD",
            "MINIO_ROOT_PASSWORD",
            "TITAN_READ_TOKEN",
            "TITAN_WRITE_TOKEN",
            "TITAN_ADMIN_TOKEN",
        ):
            value = secrets.token_urlsafe(32)
            (secrets_dir / name).write_text(value + "\n", encoding="utf-8")
            self.env[name] = value
        self.env.update(
            {
                "TITAN_ORCHESTRATION_IMAGE": os.environ.get(
                    "TITAN_ORCHESTRATION_IMAGE", "titan-orchestration:6e-acceptance"
                ),
                "TITAN_SPARK_IMAGE": os.environ.get(
                    "TITAN_SPARK_IMAGE",
                    "titan-spark:protected-f3952feb443145ed90c19e64a6ef433b502f68ca",
                ),
                "KAFKA_TLS_STORE_PASSWORD": secrets.token_urlsafe(24),
                "KAFKA_SASL_USERNAME": f"{self.project}-client",
                "KAFKA_SASL_PASSWORD": secrets.token_urlsafe(32),
                "TITAN_OIDC_ISSUER": "https://oidc.invalid/",
                "TITAN_OIDC_CLIENT_ID": "staging",
                "TITAN_OIDC_CLIENT_SECRET": secrets.token_urlsafe(24),
                "TITAN_OIDC_REDIRECT_URI": "https://localhost/oidc/callback",
                "TITAN_OIDC_AUDIENCE": "titan-staging",
                "TITAN_OIDC_SESSION_SECRET": secrets.token_urlsafe(32),
            }
        )
        self.run(
            "tls",
            [
                sys.executable,
                "scripts/staging_tls.py",
                "provision",
                "--tls-dir",
                str(tls_dir),
                "--days",
                "2",
                "--windows-acl-verified",
            ],
        )
        self.run(
            "tls-verify",
            [
                sys.executable,
                "scripts/staging_tls.py",
                "verify",
                "--tls-dir",
                str(tls_dir),
                "--windows-acl-verified",
            ],
        )
        shutil.copyfile(
            self.root / "docs/security/clickhouse-tls-config.xml.example",
            tls_dir / "config.xml",
        )
        volumes = {
            "postgres": "postgres-tls",
            "clickhouse": "clickhouse-tls",
            "minio": "minio-tls",
        }
        for service, suffix in volumes.items():
            self.run(
                f"{service}-tls",
                [
                    sys.executable,
                    f"scripts/staging_{service}_tls.py",
                    "--source",
                    str(tls_dir),
                    "--volume",
                    f"{self.project}-{suffix}",
                ],
            )
        from staging_postgres_tls import PG_HBA

        (tls_dir / "pg_hba.conf").write_text(PG_HBA, encoding="ascii")
        self.run(
            "kafka-stores",
            [
                sys.executable,
                "scripts/staging_kafka_tls.py",
                "--source",
                str(tls_dir),
                "--destination",
                str(kafka_dir),
                "--password",
                self.env["KAFKA_TLS_STORE_PASSWORD"],
                "--username",
                self.env["KAFKA_SASL_USERNAME"],
                "--sasl-password",
                self.env["KAFKA_SASL_PASSWORD"],
            ],
        )
        self.run(
            "kafka-tls",
            [
                sys.executable,
                "scripts/staging_kafka_tls.py",
                "--source",
                str(kafka_dir),
                "--volume",
                f"{self.project}-kafka-tls",
                "--password",
                self.env["KAFKA_TLS_STORE_PASSWORD"],
                "--username",
                self.env["KAFKA_SASL_USERNAME"],
                "--sasl-password",
                self.env["KAFKA_SASL_PASSWORD"],
            ],
        )
        preflight = [
            sys.executable,
            "scripts/staging_preflight.py",
            "--secrets-dir",
            str(secrets_dir),
            "--project-name",
            self.project,
            "--windows-acl-verified",
            "--postgres-tls",
            "--postgres-tls-dir",
            str(tls_dir),
            "--kafka-tls",
            "--kafka-tls-dir",
            str(kafka_dir),
            "--clickhouse-tls",
            "--clickhouse-tls-dir",
            str(tls_dir),
            "--minio-tls",
            "--minio-tls-dir",
            str(tls_dir),
        ]
        for file in COMPOSE_FILES:
            preflight.extend(("--compose-file", str((self.root / file).resolve())))
        self.run("preflight", preflight)

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
    parser.add_argument("--bootstrap-only", action="store_true")
    args = parser.parse_args(argv)
    project = require_project(args.project_name or project_name())
    runner = Runner(ROOT, project, args.timeout)
    try:
        runner.run("docker", ["docker", "info", "--format", "{{.ServerVersion}}"])
        with tempfile.TemporaryDirectory(prefix="titan-protected-accept-") as temporary:
            runner.bootstrap(Path(temporary))
            if args.bootstrap_only:
                print(f"bootstrap succeeded for {project}")
                return 0
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
