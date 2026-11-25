"""Bounded disposable protected-staging Dagster acceptance runner.

This composes the repository's existing provisioning and acceptance commands; it
does not contain fixture, TLS, or warehouse business logic.
"""
# ruff: noqa: E501

from __future__ import annotations

import argparse
import json
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
        source_roots = [
            str(self.root / "pipelines" / "dagster" / "src"),
            str(self.root / "pipelines" / "warehouse" / "src"),
            str(self.root / "pipelines" / "quality" / "src"),
        ]
        existing_pythonpath = self.env.get("PYTHONPATH", "")
        if existing_pythonpath:
            source_roots.append(existing_pythonpath)
        self.env.update(
            {
                "COMPOSE_PROJECT_NAME": self.project,
                "TITAN_PROTECTED_STAGING": "true",
                "TITAN_STAGING_VOLUME_PREFIX": self.project,
                "COMPOSE_FILE": os.pathsep.join(COMPOSE_FILES),
                "PYTHONPATH": os.pathsep.join(source_roots),
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
            value = secrets.token_hex(32)
            (secrets_dir / name).write_text(value + "\n", encoding="utf-8")
            self.env[name] = value
        metrics_token = secrets_dir / "metrics_read_token"
        metrics_token.write_text(self.env["TITAN_READ_TOKEN"] + "\n", encoding="utf-8")
        metrics_token.chmod(0o600)
        self.env["TITAN_METRICS_TOKEN_FILE"] = str(metrics_token)
        self.env.update(
            {
                "TITAN_ORCHESTRATION_IMAGE": os.environ.get(
                    "TITAN_ORCHESTRATION_IMAGE", "titan-orchestration:6e-acceptance"
                ),
                "TITAN_SPARK_IMAGE": os.environ.get(
                    "TITAN_SPARK_IMAGE",
                    "titan-spark:protected-f3952feb443145ed90c19e64a6ef433b502f68ca",
                ),
                "KAFKA_TLS_STORE_PASSWORD": secrets.token_hex(24),
                "KAFKA_SASL_USERNAME": f"{self.project}-client",
                "KAFKA_SASL_PASSWORD": secrets.token_hex(32),
                "TITAN_OIDC_ISSUER": "https://oidc.invalid/",
                "TITAN_OIDC_CLIENT_ID": "staging",
                "TITAN_OIDC_CLIENT_SECRET": secrets.token_hex(24),
                "TITAN_OIDC_REDIRECT_URI": "https://localhost/oidc/callback",
                "TITAN_OIDC_AUDIENCE": "titan-staging",
                "TITAN_OIDC_SESSION_SECRET": secrets.token_hex(32),
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

    def expected_failure(self, stage: str, command: list[str]) -> str:
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
                f"{stage} expected_exit={completed.returncode} "
                f"seconds={time.monotonic() - started:.2f}\n"
            )
            stream.write(detail.replace("password", "[redacted]").replace("token", "[redacted]"))
            stream.write("\n")
        if completed.returncode == 0:
            raise RuntimeError(f"stage {stage} unexpectedly succeeded; see {self.evidence}")
        return detail

    def compose(self, *args: str) -> list[str]:
        command = ["docker", "compose", "--project-name", self.project]
        for compose_file in COMPOSE_FILES:
            command.extend(("-f", compose_file))
        return command + list(args)

    def cleanup(self) -> None:
        try:
            listed = subprocess.run(
                self.compose("ps", "-aq", "clickhouse"),
                cwd=self.root,
                env=self.env,
                capture_output=True,
                text=True,
                timeout=self.timeout,
                check=False,
            )
            for container in listed.stdout.splitlines():
                diagnostics = [
                    ("clickhouse-docker-log", ["docker", "logs", "--timestamps", container]),
                    ("clickhouse-inspect", ["docker", "inspect", container]),
                    (
                        "clickhouse-error-log",
                        [
                            "docker",
                            "exec",
                            container,
                            "sh",
                            "-lc",
                            "tail -200 /var/log/clickhouse-server/clickhouse-server.err.log",
                        ],
                    ),
                ]
                for label, command in diagnostics:
                    result = subprocess.run(
                        command, capture_output=True, text=True, timeout=30, check=False
                    )
                    with self.evidence.open("a", encoding="utf-8") as stream:
                        stream.write(f"{label} exit={result.returncode}\n")
                        stream.write((result.stdout + result.stderr)[-12000:])
                        stream.write("\n")
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
        temporary = Path(tempfile.mkdtemp(prefix="titan-protected-accept-"))
        runner.bootstrap(temporary)
        if args.bootstrap_only:
            print(f"bootstrap succeeded for {project}")
            return 0
        runner.run(
            "services",
            runner.compose(
                "--profile",
                "processing",
                "--profile",
                "observability",
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
                "prometheus",
                "grafana",
            ),
        )
        runner.expected_failure(
            "metrics-unauthenticated",
            runner.compose(
                "exec", "-T", "prometheus", "wget", "-q", "-O", "-", "http://api:8000/metrics"
            ),
        )
        runner.expected_failure(
            "metrics-invalid-token",
            runner.compose(
                "exec",
                "-T",
                "prometheus",
                "wget",
                "--header=Authorization: Bearer invalid-disposable-token",
                "-q",
                "-O",
                "-",
                "http://api:8000/metrics",
            ),
        )
        runner.run(
            "metrics-scrape",
            runner.compose(
                "exec",
                "-T",
                "prometheus",
                "sh",
                "-lc",
                "wget --header=\"Authorization:Bearer $(tr -d '\\r\\n' < /run/secrets/metrics_read_token)\" "
                "-q -O - http://api:8000/metrics",
            ),
        )
        runner.run(
            "prometheus-target",
            runner.compose(
                "exec",
                "-T",
                "prometheus",
                "sh",
                "-lc",
                "sleep 20; wget -q -O - 'http://127.0.0.1:9090/api/v1/query?query=up%7Bjob%3D%22titan-api%22%7D'",
            ),
        )
        runner.run(
            "grafana-health",
            runner.compose(
                "exec", "-T", "grafana", "wget", "-q", "-O", "-", "http://grafana:3000/api/health"
            ),
        )
        runner.run(
            "grafana-query",
            runner.compose(
                "exec",
                "-T",
                "grafana",
                "sh",
                "-lc",
                "auth=$(printf 'titan:%s' \"$GF_SECURITY_ADMIN_PASSWORD\" | base64 | tr -d '\\r\\n'); "
                'wget --header="Authorization: Basic $auth" -q -O - '
                "'http://grafana:3000/api/datasources/proxy/uid/prometheus/api/v1/query?query=titan_warehouse_ready'",
            ),
        )
        runner.run(
            "clickhouse-db",
            runner.compose(
                "exec",
                "-T",
                "clickhouse",
                "sh",
                "-lc",
                "clickhouse-client --config-file=/tls/config.xml --secure "
                '--host clickhouse --port 9440 --user "$CLICKHOUSE_USER" '
                '--password "$CLICKHOUSE_PASSWORD" '
                "--query 'CREATE DATABASE IF NOT EXISTS titan'",
            ),
        )
        fixture_mount = str((ROOT / "scripts" / "full_pipeline_ci.py").resolve())
        benchmark_mount = str((ROOT / "benchmarks").resolve())
        fixture_database = (
            "postgresql://titan:"
            f"{runner.env['POSTGRES_PASSWORD']}"
            "@postgres:5432/titan?sslmode=verify-full&sslrootcert=/tls/ca.crt"
        )
        runner.env["DATABASE_URL"] = fixture_database
        runner.run(
            "fixture",
            runner.compose(
                "run",
                "--rm",
                "--no-deps",
                "-v",
                f"{fixture_mount}:/tmp/full_pipeline_ci.py:ro",
                "-v",
                f"{benchmark_mount}:/benchmarks",
                "-e",
                "KAFKA_BOOTSTRAP_SERVERS=kafka:19092",
                "-e",
                "DATABASE_URL",
                "-e",
                f"COMPOSE_PROJECT_NAME={runner.project}",
                "ingestion",
                "python",
                "/tmp/full_pipeline_ci.py",
            ),
        )
        for stage, asset in (
            ("bronze", "bronze_evidence"),
            ("silver", "silver_publication"),
            ("warehouse", "clickhouse_load"),
        ):
            runner.run(stage, [sys.executable, "-m", "titan_dagster", asset])
        reconcile = (
            "import json; from titan_warehouse.load import _clickhouse, _database_url, "
            "_money, _published_loads; c=_clickhouse(); "
            "p=_published_loads(_database_url())[0]; "
            "a=c.query('select warehouse_publication_id from wh_active limit 1').result_rows[0][0]; "
            "counts={t:int(c.query(f'select count() from {t} where warehouse_publication_id = {repr(p[\"id\"])}').result_rows[0][0]) for t in ['wh_orders','wh_payments','wh_inventory','wh_order_items']}; "
            "revenue=_money(c,p['id']); print(json.dumps({'load_id':p['load_id'],'publication_id':p['id'],'active':str(a),'counts':counts,'revenue':revenue})); "
            "assert str(a)==p['id'] and counts=={'wh_orders':86,'wh_payments':86,'wh_inventory':84,'wh_order_items':86} and revenue=={'EUR':'2308.38','GBP':'2171.39','USD':'1995.90'}"
        )
        before_output = runner.run(
            "warehouse-reconcile",
            runner.compose(
                "run", "--rm", "--no-deps", "warehouse-publish", "python", "-c", reconcile
            ),
        )
        before = json.loads(
            next(line for line in before_output.splitlines() if line.startswith("{"))
        )
        failed = runner.expected_failure(
            "warehouse-failure",
            runner.compose(
                "run",
                "--rm",
                "--no-deps",
                "warehouse-publish",
                "python",
                "-m",
                "titan_warehouse.load",
                "--crash-after",
                "tested",
            ),
        )
        if "crash after dbt tests and before publication" not in failed:
            raise RuntimeError("warehouse failure injection did not reach its controlled boundary")
        failure_output = runner.run(
            "warehouse-reconcile-after-failure",
            runner.compose(
                "run", "--rm", "--no-deps", "warehouse-publish", "python", "-c", reconcile
            ),
        )
        failure_state = json.loads(
            next(line for line in failure_output.splitlines() if line.startswith("{"))
        )
        if failure_state["active"] != before["active"]:
            raise RuntimeError("failed warehouse candidate changed the active publication")
        runner.run(
            "warehouse-retry",
            runner.compose("run", "--rm", "--no-deps", "warehouse-publish"),
        )
        after_output = runner.run(
            "warehouse-reconcile-retry",
            runner.compose(
                "run", "--rm", "--no-deps", "warehouse-publish", "python", "-c", reconcile
            ),
        )
        after = json.loads(next(line for line in after_output.splitlines() if line.startswith("{")))
        if after["active"] == before["active"]:
            raise RuntimeError("warehouse retry did not create a new active publication")
        return 0
    finally:
        runner.cleanup()
        if "temporary" in locals():
            shutil.rmtree(temporary, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
