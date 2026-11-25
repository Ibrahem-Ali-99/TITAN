"""Exercise Day 6 recovery without replacing a valid warehouse on success.

The exchange crash is real. Reconciliation must put the previous pointer back.
A browser was not used.
"""

import json
import os
import subprocess
import time
import urllib.request
from pathlib import Path
from typing import Any

from scripts.envfile import load_env_file
from event_generator.generator import generate_orders

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    load_env_file(ROOT / ".env")
    os.environ.pop("TITAN_QUALITY_INJECT", None)
    os.environ.pop("TITAN_ADMIN_TOKEN", None)
    base = os.environ.get("API_BASE_URL", "http://localhost:8000")
    _wait_ready(base)
    before = _overview(base)
    pointer = before["warehouse_publication_id"]
    revenue = before["accepted_revenue"]
    _duplicate_delivery(base)
    _restart("ingestion")
    _postgres_outage(base)
    _crash_and_reconcile(pointer, revenue, base)
    _train_and_check(base)
    _heartbeat()
    _benchmark()
    after = _overview(base)
    if after["warehouse_publication_id"] != pointer or after["accepted_revenue"] != revenue:
        raise SystemExit("recovery changed the serving warehouse")
    print(
        json.dumps(
            {
                "publication": pointer,
                "revenue": revenue,
                "serving": _json(base, "/api/v1/ml/models?limit=3"),
            },
            indent=2,
        )
    )


def _duplicate_delivery(base: str) -> None:
    before = int(_json(base, "/api/v1/ingestion/stats")["logical_orders"])
    event = generate_orders(1, seed=61023, deterministic_event_ids=True)[0]
    body = event.model_dump(mode="json")
    _post(base, "/api/v1/ingestion/events", body)
    _wait_event(base, str(event.event_id))
    mid = int(_json(base, "/api/v1/ingestion/stats")["logical_orders"])
    if mid != before + 1:
        raise SystemExit(f"first delivery did not add one order: {before} -> {mid}")
    duplicates = int(_json(base, "/api/v1/ingestion/stats")["duplicate_deliveries"])
    _post(base, "/api/v1/ingestion/events", body)
    time.sleep(3)
    after = int(_json(base, "/api/v1/ingestion/stats")["logical_orders"])
    if after != mid:
        raise SystemExit(f"duplicate delivery added an order: {mid} -> {after}")
    if int(_json(base, "/api/v1/ingestion/stats")["duplicate_deliveries"]) <= duplicates:
        raise SystemExit("duplicate delivery was not recorded")


def _postgres_outage(base: str) -> None:
    subprocess.run(["docker", "stop", "titan-postgres-1"], cwd=ROOT, check=True)
    try:
        deadline = time.monotonic() + 30
        saw_down = False
        while time.monotonic() < deadline:
            try:
                _json(base, "/health/ready")
            except Exception:
                saw_down = True
                break
            time.sleep(1)
        if not saw_down:
            raise SystemExit("postgres stop did not make the API unready")
    finally:
        subprocess.run(["docker", "start", "titan-postgres-1"], cwd=ROOT, check=True)
    _wait_ready(base)


def _crash_and_reconcile(pointer: str, revenue: dict[str, Any], base: str) -> None:
    completed = _python("titan_warehouse", "--crash-after", "exchanged")
    if completed.returncode == 0:
        raise SystemExit("exchange crash returned success")
    if _active_publication() == pointer:
        raise SystemExit("exchange crash did not move the pointer")
    restored = _python("titan_warehouse", "--reconcile")
    if restored.returncode != 0:
        raise SystemExit(restored.stderr[-2000:])
    if _active_publication() != pointer:
        raise SystemExit("reconcile did not restore the published pointer")
    if _overview(base)["accepted_revenue"] != revenue:
        raise SystemExit("reconcile changed serving revenue")


def _train_and_check(base: str) -> None:
    completed = _python("titan_ml")
    if completed.returncode != 0:
        raise SystemExit(completed.stderr[-2000:] or completed.stdout[-2000:])
    models = _json(base, "/api/v1/ml/models?limit=5")
    serving = next(row for row in models["models"] if row["status"] == "serving")
    if serving["serving_strategy"] != "seasonal_naive":
        raise SystemExit(f"serving strategy was not the baseline: {serving}")
    if not any(row["status"] == "evaluated" for row in models["models"]):
        raise SystemExit("the previous training run was not retained")


def _heartbeat() -> None:
    code = """
from datetime import UTC, datetime
from dagster import build_schedule_context, materialize
from titan_dagster.definitions import heartbeat_schedule, reliability_heartbeat
context = build_schedule_context(
    scheduled_execution_time=datetime(2026, 11, 22, 18, 5, tzinfo=UTC)
)
evaluation = heartbeat_schedule.evaluate_tick(context)
if not list(evaluation.run_requests or []):
    raise SystemExit("heartbeat schedule tick returned no run")
result = materialize([reliability_heartbeat])
if not result.success:
    raise SystemExit("heartbeat materialization failed")
print("heartbeat")
"""
    completed = subprocess.run(
        ["uv", "run", "--group", "orchestration", "python", "-c", code],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        env=_python_env(),
    )
    if completed.returncode != 0:
        raise SystemExit(completed.stderr[-2000:] or completed.stdout[-2000:])


def _benchmark() -> None:
    completed = subprocess.run(
        ["uv", "run", "python", "scripts/benchmark_ingestion.py"],
        cwd=ROOT,
        check=False,
        env={**os.environ, "TITAN_BENCH_COUNT": "100", "TITAN_BENCH_SEED": "61022"},
    )
    if completed.returncode != 0:
        raise SystemExit("benchmark failed")


def _restart(service: str) -> None:
    subprocess.run(["docker", "compose", "restart", service], cwd=ROOT, check=True)
    time.sleep(5)


def _overview(base: str) -> dict[str, Any]:
    body = _json(base, "/api/v1/analytics/overview")
    if not isinstance(body, dict) or not body.get("warehouse_publication_id"):
        raise SystemExit(f"warehouse overview is unavailable: {body}")
    return body


def _active_publication() -> str:
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "titan"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", "titan_dev_password"),
        database=os.environ.get("CLICKHOUSE_DATABASE", "titan"),
    )
    try:
        return str(client.query("select warehouse_publication_id from wh_active").result_rows[0][0])
    finally:
        client.close()


def _post(base: str, path: str, payload: dict[str, Any]) -> None:
    request = urllib.request.Request(
        base + path,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        if response.status not in {200, 202}:
            raise SystemExit(f"{path} returned {response.status}")


def _wait_event(base: str, event_id: str) -> None:
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        recent = _json(base, "/api/v1/events/recent?limit=50")
        if any(item["event_id"] == event_id for item in recent["items"]):
            return
        time.sleep(1)
    raise SystemExit(f"event {event_id} was not stored")


def _wait_ready(base: str) -> None:
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        try:
            body = _json(base, "/health/ready")
        except Exception:
            time.sleep(2)
            continue
        if isinstance(body, dict) and body.get("status") == "ready":
            return
        time.sleep(2)
    raise SystemExit("API did not become ready")


def _json(base: str, path: str) -> Any:
    with urllib.request.urlopen(base + path, timeout=30) as response:
        return json.load(response)


def _python(module: str, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["uv", "run", "--group", "orchestration", "python", "-m", module, *args],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        env=_python_env(),
    )


def _python_env() -> dict[str, str]:
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(
        [
            str(ROOT / "pipelines" / "warehouse" / "src"),
            str(ROOT / "pipelines" / "quality" / "src"),
            str(ROOT / "pipelines" / "ml" / "src"),
            str(ROOT / "pipelines" / "dagster" / "src"),
            env.get("PYTHONPATH", ""),
        ]
    )
    env.setdefault("DATABASE_URL", "postgresql://titan:titan_dev_password@localhost:5435/titan")
    return env


if __name__ == "__main__":
    main()
