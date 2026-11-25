import threading
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from opentelemetry import trace
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from prometheus_client import generate_latest
from sqlalchemy.orm import Session

from titan_api.config import ApiSettings
from titan_api.main import create_app
from titan_api.metrics import (
    CONSUMER_LAG,
    LAG_MEASURED,
    PIPELINE_FAILED,
    PIPELINE_KNOWN,
    WAREHOUSE_KNOWN,
    WAREHOUSE_READY,
    OperationalCollector,
    _lag_gauge,
    _pipelines,
    _warehouse,
)
from titan_api.platform import LagView
from titan_api.tracing import enable_tracing, shutdown_tracing

ROOT = Path(__file__).resolve().parents[2]


def _settings() -> ApiSettings:
    return ApiSettings(
        database_url="sqlite://",
        cors_origins="http://localhost:3000",
        otel_enabled=False,
        metrics_collector_enabled=False,
    )


def _gauge(name: str) -> float | None:
    prefix = name + " "
    for line in generate_latest().decode().splitlines():
        if line.startswith(prefix):
            return float(line.split()[-1])
    return None


def test_metrics_use_the_route_template() -> None:
    app = create_app(_settings())
    client = TestClient(app)
    assert client.get("/health/live").status_code == 200
    response = client.get("/metrics")
    assert response.status_code == 200
    assert 'path="/health/live"' in response.text
    assert "titan_http_requests_total" in response.text
    assert "titan_http_request_duration_seconds" in response.text


def test_scrape_does_not_query_kafka_or_the_database(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("scrape touched an operational dependency")

    monkeypatch.setattr("titan_api.metrics._lag", boom)
    monkeypatch.setattr("titan_api.metrics._warehouse", boom)
    app = create_app(_settings())
    with TestClient(app) as client:
        assert client.get("/metrics").status_code == 200


def test_unavailable_lag_does_not_replace_the_last_value(monkeypatch: pytest.MonkeyPatch) -> None:
    CONSUMER_LAG.set(7)
    LAG_MEASURED.set(1)
    monkeypatch.setattr(
        "titan_api.metrics._lag",
        lambda _bootstrap: LagView(
            consumer_group="titan-ingestion",
            available=False,
            total_lag=None,
            detail="down",
        ),
    )
    assert _lag_gauge(_settings()) is False
    assert _gauge("titan_kafka_consumer_lag_records") == 7
    assert _gauge("titan_kafka_lag_measured") == 0


def test_warehouse_read_failure_does_not_invent_not_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    WAREHOUSE_READY.set(1)
    WAREHOUSE_KNOWN.set(1)

    def broken(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("clickhouse down")

    monkeypatch.setattr("titan_api.metrics.load_status", broken)
    assert _warehouse(object(), _settings()) is False  # type: ignore[arg-type]
    assert _gauge("titan_warehouse_ready") == 1
    assert _gauge("titan_warehouse_status_known") == 0


def test_pipeline_read_failure_does_not_clear_a_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    PIPELINE_FAILED.labels("quality_gate").set(1)
    PIPELINE_KNOWN.set(1)

    def broken(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("postgres down")

    monkeypatch.setattr("titan_api.metrics.monitoring_jobs", broken)
    assert _pipelines(object()) is False  # type: ignore[arg-type]
    assert _gauge('titan_pipeline_latest_failed{job="quality_gate"}') == 1
    assert _gauge("titan_pipeline_status_known") == 0


def test_collect_once_does_not_overlap() -> None:
    calls = {"n": 0}
    started = threading.Event()
    release = threading.Event()

    class Blocking(OperationalCollector):
        def _collect(self) -> bool:
            calls["n"] += 1
            started.set()
            assert release.wait(2)
            return True

    def unused_session() -> Session:
        raise AssertionError("session was not required")

    collector = Blocking(unused_session, _settings())
    thread = threading.Thread(target=collector.collect_once)
    thread.start()
    assert started.wait(2)
    collector.collect_once()
    release.set()
    thread.join(2)
    assert calls["n"] == 1


def test_collection_deadline_does_not_wait_for_a_blocked_probe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from titan_api import metrics as metrics_module

    def blocked(_session: Session) -> bool:
        time.sleep(0.2)
        return True

    monkeypatch.setattr(metrics_module, "_ready", blocked)
    monkeypatch.setattr(metrics_module, "PROBE_TIMEOUT_SECONDS", 0.03)
    collector = OperationalCollector(lambda: Session(), _settings())
    started = time.perf_counter()
    collector.collect_once()
    elapsed = time.perf_counter() - started
    assert elapsed < 0.15
    assert _gauge("titan_metrics_collection_ok") == 0


def test_traceparent_becomes_the_parent_span() -> None:
    exporter = InMemorySpanExporter()
    app = create_app(_settings())
    enable_tracing(app, exporter, batch=False)
    trace_id = "4bf92f3577b34da6a3ce929d0e0e4736"
    parent = "00f067aa0ba902b7"
    try:
        client = TestClient(app)
        response = client.get("/health/live", headers={"traceparent": f"00-{trace_id}-{parent}-01"})
        assert response.status_code == 200
    finally:
        shutdown_tracing()
    spans = [
        span
        for span in exporter.get_finished_spans()
        if span.kind == trace.SpanKind.SERVER and span.parent is not None
    ]
    assert spans
    parent_context = spans[0].parent
    assert parent_context is not None
    assert format(spans[0].context.trace_id, "032x") == trace_id
    assert format(parent_context.span_id, "016x") == parent


def test_alert_rules_name_the_operational_conditions() -> None:
    rules = (ROOT / "observability" / "prometheus" / "alerts.yml").read_text(encoding="utf-8")
    for name in (
        "TitanApiDown",
        "TitanWarehouseNotReady",
        "TitanDataStale",
        "TitanConsumerLag",
        "TitanPublicationFailed",
        "TitanQualityFailed",
        "TitanPostgresExporterDown",
        "TitanMetricsStale",
        "TitanMetricsCollectionFailed",
    ):
        assert name in rules
    assert "21600" in rules
    assert "titan_kafka_lag_measured == 1" in rules
    assert "titan_warehouse_freshness_known == 1" in rules
    assert "titan_warehouse_status_known == 1" in rules
    assert "titan_operational_metrics_fresh == 0" in rules


def test_collection_age_becomes_stale(monkeypatch: pytest.MonkeyPatch) -> None:
    from titan_api import metrics as metrics_module

    monkeypatch.setattr(metrics_module, "_last_attempt_monotonic", time.monotonic() - 60)
    monkeypatch.setattr(metrics_module, "FRESH_SECONDS", 45.0)
    metrics_module.publish_collection_age()
    assert _gauge("titan_operational_metrics_fresh") == 0
    assert (_gauge("titan_operational_metrics_age_seconds") or 0) > 45
