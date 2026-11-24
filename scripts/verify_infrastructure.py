"""Check Kafka, PostgreSQL, ClickHouse, and MinIO. This creates no business data."""

import os
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from envfile import load_env_file

ROOT = Path(__file__).resolve().parents[1]


def check_postgres(url: str) -> str:
    engine = create_engine(
        url,
        pool_pre_ping=True,
        connect_args={"connect_timeout": 15},
    )
    try:
        with engine.connect() as connection:
            value = connection.execute(text("SELECT 1")).scalar_one()
        return f"ok SELECT 1 -> {value}"
    except SQLAlchemyError as exc:
        detail = str(getattr(exc, "orig", exc)).splitlines()[0]
        return f"FAIL {exc.__class__.__name__}: {detail}"
    finally:
        engine.dispose()


def check_kafka(servers: str) -> str:
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": servers})
    metadata = admin.list_topics(timeout=10)
    broker_ids = sorted(metadata.brokers)
    if not broker_ids:
        return "FAIL no brokers in metadata"
    return f"ok brokers={broker_ids}"


def check_clickhouse(url: str, user: str, password: str) -> str:
    request = urllib.request.Request(
        f"{url.rstrip('/')}/?query=SELECT%201",
        headers={"X-ClickHouse-User": user, "X-ClickHouse-Key": password},
    )
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            body = response.read().decode("utf-8").strip()
    except urllib.error.HTTPError as exc:
        return f"FAIL HTTP {exc.code}"
    except urllib.error.URLError as exc:
        return f"FAIL {exc.reason}"
    if body != "1":
        return f"FAIL unexpected body {body!r}"
    return "ok authenticated SELECT 1"


def check_minio(endpoint: str, access_key: str, secret_key: str) -> str:
    from minio import Minio
    from minio.error import S3Error

    secure = os.environ.get("MINIO_SECURE", "false").lower() == "true"
    client = Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
        cert_check=True,
    )
    try:
        buckets = client.list_buckets()
    except S3Error as exc:
        return f"FAIL {exc.code}"
    except Exception as exc:
        return f"FAIL {exc.__class__.__name__}: {exc}"
    return f"ok authenticated ListBuckets count={len(buckets)}"


def assess_volume_mount(project: str, mounts: str) -> str:
    """A non-development project must use its own Postgres volume."""
    if project in {"", "titan"}:
        return "ok development project"
    if f"{project}_postgres_data" not in mounts:
        return f"FAIL postgres is not on {project}_postgres_data: {mounts!r}"
    if "titan_postgres_data" in mounts or "titan-e2e_postgres_data" in mounts:
        return f"FAIL postgres mounts another project's volume: {mounts!r}"
    return f"ok {mounts.strip()}"


def check_project_volume(project: str) -> str:
    if project in {"", "titan"}:
        return "ok development project"
    completed = subprocess.run(
        [
            "docker",
            "inspect",
            f"{project}-postgres-1",
            "--format",
            "{{range .Mounts}}{{.Name}} {{end}}",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip().splitlines()
        reason = detail[-1] if detail else "unknown"
        return f"FAIL postgres container {project}-postgres-1 is not inspectable: {reason}"
    return assess_volume_mount(project, completed.stdout)


def main() -> None:
    load_env_file(ROOT / ".env")
    checks = {
        "postgres": check_postgres(
            os.environ.get(
                "DATABASE_URL",
                "postgresql+psycopg://titan:titan_dev_password@localhost:5435/titan",
            )
        ),
        "kafka": check_kafka(os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")),
        "clickhouse": check_clickhouse(
            os.environ.get("CLICKHOUSE_HTTP_URL", "http://localhost:8123"),
            os.environ.get("CLICKHOUSE_USER", "titan"),
            os.environ.get("CLICKHOUSE_PASSWORD", "titan_dev_password"),
        ),
        "minio": check_minio(
            os.environ.get("MINIO_ENDPOINT", "localhost:9000"),
            os.environ.get("MINIO_ROOT_USER", "titan"),
            os.environ.get("MINIO_ROOT_PASSWORD", "titan_dev_password"),
        ),
        "volume": check_project_volume(os.environ.get("COMPOSE_PROJECT_NAME", "titan")),
    }
    failed = False
    for name, result in checks.items():
        print(f"{name}: {result}")
        if result.startswith("FAIL") or "Traceback" in result:
            failed = True
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"infrastructure check crashed: {exc}", file=sys.stderr)
        sys.exit(1)
