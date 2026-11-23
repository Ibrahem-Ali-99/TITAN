"""Check the local observability profile against the running TITAN stack.

The script reads Prometheus, Grafana, and the API. It stops and starts only the
Postgres exporter so one alert can fire and clear without changing business data.
"""

from __future__ import annotations

import base64
import json
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from base64 import b64encode
from typing import Any

API = "http://127.0.0.1:8000"
PROMETHEUS = "http://127.0.0.1:9090"
GRAFANA = "http://127.0.0.1:3001"
TEMPO = "http://127.0.0.1:3200"
GRAFANA_USER = "titan"
GRAFANA_PASSWORD = "titan_dev_password"
EXPECTED_TARGETS = ("prometheus", "titan-api", "postgres", "kafka")
CONTROLLED_ALERT = "TitanPostgresExporterDown"


def get(url: str, headers: dict[str, str] | None = None, timeout: float = 20) -> tuple[int, bytes]:
    request = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read()


def grafana(path: str) -> tuple[int, bytes]:
    token = b64encode(f"{GRAFANA_USER}:{GRAFANA_PASSWORD}".encode()).decode()
    return get(f"{GRAFANA}{path}", {"Authorization": f"Basic {token}"})


def prom_query(expr: str) -> list[dict[str, Any]]:
    status, body = get(f"{PROMETHEUS}/api/v1/query?query={urllib.parse.quote(expr)}")
    payload = json.loads(body)
    if status != 200 or payload.get("status") != "success":
        raise RuntimeError(f"prometheus query failed for {expr}: {body[:300]!r}")
    result = payload["data"]["result"]
    if not isinstance(result, list):
        raise RuntimeError(f"prometheus result was not a list for {expr}")
    return result


def sample(expr: str) -> float | None:
    rows = prom_query(expr)
    if not rows:
        return None
    return float(rows[0]["value"][1])


def alert_state(name: str) -> str:
    status, body = get(f"{PROMETHEUS}/api/v1/alerts")
    payload = json.loads(body)
    if status != 200:
        raise RuntimeError(body[:300].decode())
    states = [
        item.get("state", "")
        for item in payload.get("data", {}).get("alerts", [])
        if item.get("labels", {}).get("alertname") == name
    ]
    if not states:
        return "inactive"
    if "firing" in states:
        return "firing"
    if "pending" in states:
        return "pending"
    return str(states[0])


def wait_for_alert(name: str, wanted: str, timeout_s: int) -> str:
    deadline = time.monotonic() + timeout_s
    seen = "missing"
    while time.monotonic() < deadline:
        seen = alert_state(name)
        if seen == wanted:
            return seen
        time.sleep(5)
    return seen


def _b64_hex(value: object) -> str:
    if not isinstance(value, str):
        return ""
    try:
        return base64.b64decode(value).hex()
    except ValueError:
        return ""


def trace_has_parent(body: bytes, trace_id: str, parent: str) -> bool:
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return False
    batches = payload.get("batches")
    if not isinstance(batches, list):
        return False
    for batch in batches:
        if not isinstance(batch, dict):
            continue
        scopes = batch.get("scopeSpans")
        if not isinstance(scopes, list):
            continue
        for scope in scopes:
            if not isinstance(scope, dict):
                continue
            spans = scope.get("spans")
            if not isinstance(spans, list):
                continue
            for span in spans:
                if not isinstance(span, dict) or span.get("kind") != "SPAN_KIND_SERVER":
                    continue
                if (
                    _b64_hex(span.get("traceId")) == trace_id
                    and _b64_hex(span.get("parentSpanId")) == parent
                ):
                    return True
    return False


def counter(path: str) -> float:
    quoted = path.replace("\\", "\\\\").replace('"', '\\"')
    value = sample(f'sum(titan_http_requests_total{{path="{quoted}",status="200"}})')
    return 0 if value is None else value


def wait_for_targets(timeout_s: int) -> dict[str, str]:
    deadline = time.monotonic() + timeout_s
    health: dict[str, str] = {}
    while time.monotonic() < deadline:
        status, body = get(f"{PROMETHEUS}/api/v1/targets")
        if status == 200:
            targets = json.loads(body)["data"]["activeTargets"]
            health = {
                item["labels"].get("job", ""): str(item.get("health", ""))
                for item in targets
                if item["labels"].get("job") in EXPECTED_TARGETS
            }
            if all(health.get(job) == "up" for job in EXPECTED_TARGETS):
                return health
        time.sleep(5)
    return health


def main() -> None:
    evidence: dict[str, Any] = {}
    health = wait_for_targets(90)
    evidence["targets"] = health
    evidence["targets_ok"] = all(health.get(job) == "up" for job in EXPECTED_TARGETS)

    code, health_body = grafana("/api/health")
    evidence["grafana_health"] = json.loads(health_body) if code == 200 else code
    code, search_body = grafana("/api/search?type=dash-db")
    dashboards = json.loads(search_body) if code == 200 else []
    evidence["dashboards"] = [item.get("title") for item in dashboards]
    code, query_body = grafana(
        "/api/datasources/proxy/uid/prometheus/api/v1/query?query="
        + urllib.parse.quote("titan_warehouse_ready")
    )
    evidence["grafana_query_status"] = code
    evidence["grafana_warehouse_ready"] = json.loads(query_body) if code == 200 else None

    before = counter("/health/ready")
    for _ in range(3):
        get(f"{API}/health/ready")
    deadline = time.monotonic() + 45
    after = before
    while time.monotonic() < deadline:
        time.sleep(5)
        after = counter("/health/ready")
        if after >= before + 3:
            break
    evidence["ready_requests_before"] = before
    evidence["ready_requests_after"] = after
    evidence["request_counter_increased"] = after >= before + 3

    api_status, api_body = get(f"{API}/api/v1/monitoring/overview")
    overview = json.loads(api_body)
    lag_metric = sample("titan_kafka_consumer_lag_records")
    evidence["api_lag"] = overview["ingestion_lag"]["total_lag"] if api_status == 200 else None
    evidence["metric_lag"] = lag_metric
    evidence["lag_matches"] = evidence["api_lag"] == lag_metric

    wh_status, wh_body = get(f"{API}/api/v1/warehouse/status")
    warehouse = json.loads(wh_body)
    ready_metric = sample("titan_warehouse_ready")
    evidence["api_warehouse_ready"] = warehouse.get("ready") if wh_status == 200 else None
    evidence["metric_warehouse_ready"] = ready_metric
    evidence["warehouse_matches"] = bool(warehouse.get("ready")) == (ready_metric == 1)

    before_alert = alert_state(CONTROLLED_ALERT)
    firing = "not-run"
    resolved = "not-run"
    try:
        subprocess.run(
            ["docker", "compose", "--profile", "observability", "stop", "postgres-exporter"],
            check=True,
        )
        firing = wait_for_alert(CONTROLLED_ALERT, "firing", 150)
    finally:
        subprocess.run(
            ["docker", "compose", "--profile", "observability", "start", "postgres-exporter"],
            check=True,
        )
        resolved = wait_for_alert(CONTROLLED_ALERT, "inactive", 120)
    evidence["controlled_alert"] = {
        "name": CONTROLLED_ALERT,
        "before": before_alert,
        "after_stop": firing,
        "after_start": resolved,
    }

    trace_id = "4bf92f3577b34da6a3ce929d0e0e4736"
    parent = "00f067aa0ba902b7"
    trace_status, _trace_body = get(
        f"{API}/health/live",
        {"traceparent": f"00-{trace_id}-{parent}-01"},
    )
    evidence["trace_response"] = trace_status
    time.sleep(8)
    tempo_status, tempo_body = get(
        f"{TEMPO}/api/traces/{trace_id}",
        {"Accept": "application/json"},
    )
    evidence["tempo_status"] = tempo_status
    evidence["tempo_body"] = tempo_body[:500].decode(errors="replace")
    evidence["trace_found"] = tempo_status == 200 and trace_has_parent(tempo_body, trace_id, parent)

    timings: list[float] = []
    for _ in range(10):
        started = time.perf_counter()
        get(f"{API}/health/ready")
        timings.append((time.perf_counter() - started) * 1000)
    timings.sort()
    evidence["ready_latency_ms"] = {
        "min": round(timings[0], 1),
        "p50": round(timings[len(timings) // 2], 1),
        "max": round(timings[-1], 1),
    }
    scrape_ms: list[float] = []
    for _ in range(10):
        started = time.perf_counter()
        get(f"{API}/metrics", timeout=30)
        scrape_ms.append((time.perf_counter() - started) * 1000)
    scrape_ms.sort()
    evidence["metrics_latency_ms"] = {
        "min": round(scrape_ms[0], 1),
        "p50": round(scrape_ms[len(scrape_ms) // 2], 1),
        "max": round(scrape_ms[-1], 1),
    }
    evidence["collection_duration_seconds"] = sample("titan_metrics_collection_duration_seconds")
    evidence["operational_fresh"] = sample("titan_operational_metrics_fresh")
    evidence["warehouse_known"] = sample("titan_warehouse_status_known")
    evidence["freshness_known"] = sample("titan_warehouse_freshness_known")
    evidence["freshness_seconds"] = sample("titan_warehouse_freshness_seconds")
    evidence["data_stale_alert"] = alert_state("TitanDataStale")
    print(json.dumps(evidence, indent=2, default=str))

    required = (
        evidence["targets_ok"]
        and evidence["request_counter_increased"]
        and evidence["lag_matches"]
        and evidence["warehouse_matches"]
        and before_alert == "inactive"
        and firing == "firing"
        and resolved == "inactive"
        and len(evidence["dashboards"]) >= 7
        and evidence["grafana_query_status"] == 200
        and evidence["trace_found"]
        and evidence["operational_fresh"] == 1
        and evidence["warehouse_known"] == 1
        and evidence["freshness_known"] == 1
        and evidence["metrics_latency_ms"]["p50"] < 40
        and evidence["data_stale_alert"] in {"pending", "firing"}
    )
    if not required:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
