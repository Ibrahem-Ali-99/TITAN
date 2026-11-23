import os
import socket

import pytest

pytestmark = pytest.mark.integration

ISOLATED_DATABASE = os.environ.get(
    "DATABASE_URL",
    "postgresql+psycopg://titan:titan_ci_password@127.0.0.1:15435/titan",
)
ISOLATED_BROKERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:19092")
ISOLATED_API = os.environ.get("API_BASE_URL", "http://127.0.0.1:18000")


def _open(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=1):
            return True
    except OSError:
        return False


def test_kafka_postgres_api_pipeline() -> None:
    project = os.environ.get("COMPOSE_PROJECT_NAME", "").strip()
    if project in {"", "titan"}:
        pytest.skip("requires an isolated COMPOSE_PROJECT_NAME, not the development project")
    if not (_open(19092) and _open(15435) and _open(18000)):
        pytest.skip("isolated Kafka, PostgreSQL, and API are not reachable")
    os.environ["DATABASE_URL"] = ISOLATED_DATABASE
    os.environ["KAFKA_BOOTSTRAP_SERVERS"] = ISOLATED_BROKERS
    os.environ["API_BASE_URL"] = ISOLATED_API
    from pipeline_flow import run_pipeline_check

    report = run_pipeline_check()
    assert report.ok, report.detail
