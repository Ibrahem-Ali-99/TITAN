import socket

import pytest

pytestmark = pytest.mark.integration


def _open(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=1):
            return True
    except OSError:
        return False


def test_kafka_postgres_api_pipeline() -> None:
    if not (_open(9092) and _open(5435) and _open(8000)):
        pytest.skip("Kafka, PostgreSQL, and the API are not all reachable on localhost")
    from pipeline_flow import run_pipeline_check

    report = run_pipeline_check()
    assert report.ok, report.detail
