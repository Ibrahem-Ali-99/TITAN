"""Check the quality gate, a historical forecast, and monitoring reads.

Requires the Compose stack. The script publishes the 28-day backfill only when the
active warehouse does not already cover 21 distinct item dates.
"""

import json
import os
import subprocess
import time
import urllib.request
from pathlib import Path
from typing import Any, cast

from scripts.envfile import load_env_file
from event_generator.generator import generate_history
from titan_contracts.events import OrderCreatedEvent

ROOT = Path(__file__).resolve().parents[1]
HISTORY_DAYS = 28
HISTORY_SEED = 51022


def main() -> None:
    load_env_file(ROOT / ".env")
    os.environ.pop("TITAN_QUALITY_INJECT", None)
    started = time.perf_counter()
    # The API stays up while its schema is behind. Compose treats that healthcheck
    # as a failed start, so this script migrates first and then polls /health/ready.
    _compose("up", "-d", "kafka", "postgres", "clickhouse", "minio", "ingestion")
    _compose("run", "--rm", "--build", "migrate")
    base = os.environ.get("API_BASE_URL", "http://localhost:8000")
    _wait_ready(base)
    before_dates = _distinct_item_dates()
    expected_orders = _history_order_count()
    if _accepted_history_orders() < expected_orders:
        _publish_history()
        _wait_history(expected_orders)
    else:
        print(f"historical orders already accepted: {expected_orders}")
    if before_dates < 21:
        _compose("--profile", "processing", "run", "--rm", "spark-bronze")
        _compose("--profile", "processing", "run", "--rm", "spark-silver")
    else:
        print(f"warehouse already has {before_dates} item dates; Spark refresh skipped")
    _warehouse()
    dates = _distinct_item_dates()
    if dates < 21:
        raise SystemExit(f"published item dates are {dates}, need at least 21")
    pointer = _active_publication()
    revenue = _overview(base)["accepted_revenue"]
    summary = _json(base, "/api/v1/quality/summary")
    if summary["latest_status"] != "passed":
        raise SystemExit(f"quality gate did not pass: {summary}")
    os.environ["TITAN_QUALITY_INJECT"] = "revenue_mismatch"
    _warehouse(expect_ok=False)
    os.environ.pop("TITAN_QUALITY_INJECT", None)
    if _active_publication() != pointer:
        raise SystemExit("injected failure replaced the active publication")
    if _overview(base)["accepted_revenue"] != revenue:
        raise SystemExit("injected failure changed serving revenue")
    failed = _json(base, "/api/v1/quality/runs?limit=5")
    injected = any(
        run["injected"] == "revenue_mismatch" and run["status"] == "failed"
        for run in failed["runs"]
    )
    if not injected:
        raise SystemExit(f"injected failure was not stored: {failed}")
    findings = _json(base, "/api/v1/quality/findings?failed_only=true&limit=20")
    if not any(item["check_id"] == "orders.revenue_matches_lines" for item in findings["findings"]):
        raise SystemExit("injected finding was not stored")
    first = _train()
    second = _train()
    models = _json(base, "/api/v1/ml/models?limit=5")
    by_id = {row["run_id"]: row for row in models["models"]}
    if first not in by_id or second not in by_id:
        raise SystemExit(f"trained runs were not stored: {first} {second}")
    if by_id[first]["metrics"] != by_id[second]["metrics"]:
        raise SystemExit("repeated training produced different metrics")
    if by_id[second]["status"] != "promoted" or by_id[first]["status"] != "archived":
        raise SystemExit("the newer run did not replace the promoted model")
    forecasts = _json(base, "/api/v1/ml/forecasts?kind=one_step_evaluation&limit=5")
    if forecasts["run_id"] != second or forecasts["returned"] < 1:
        raise SystemExit(f"forecasts are not the promoted run: {forecasts}")
    freshness = _json(base, "/api/v1/monitoring/freshness")
    warehouse = next(item for item in freshness if item["stage"] == "warehouse")
    if warehouse["age_seconds"] is None or "finished_at" not in warehouse["detail"]:
        raise SystemExit(f"warehouse freshness is not a publication timestamp: {warehouse}")
    runs = _json(base, "/api/v1/monitoring/runs?limit=30")
    timed = any(
        run["job_name"] == "quality_gate" and run["duration_ms"] is not None for run in runs["runs"]
    )
    if not timed:
        raise SystemExit("quality gate duration was not stored")
    if not any(run["state"] == "failed" for run in runs["runs"]) or not any(
        run["state"] == "succeeded" for run in runs["runs"]
    ):
        raise SystemExit("run history does not contain both a failure and a success")
    _schedule_tick()
    _compose("restart", "web")
    for path in ("/data-quality", "/ml", "/monitoring"):
        _page(path)
    elapsed = time.perf_counter() - started
    print(
        json.dumps(
            {
                "publication": pointer,
                "revenue": revenue,
                "item_dates": dates,
                "orders": _overview(base)["order_count"],
                "model": second,
                "metrics": by_id[second]["metrics"],
                "elapsed_seconds": round(elapsed, 1),
                "docker": _docker_stats(),
            },
            indent=2,
        )
    )


def _history_order_count() -> int:
    history = generate_history(HISTORY_DAYS, HISTORY_SEED)
    return sum(isinstance(event, OrderCreatedEvent) for event in history)


def _publish_history() -> None:
    completed = subprocess.run(
        [
            "uv",
            "run",
            "python",
            "-m",
            "event_generator",
            "--history-days",
            str(HISTORY_DAYS),
            "--seed",
            str(HISTORY_SEED),
            "--rate",
            "0",
        ],
        cwd=ROOT,
        check=False,
        env=_env(),
    )
    if completed.returncode != 0:
        raise SystemExit("history publish failed")


def _wait_history(expected: int) -> None:
    deadline = time.monotonic() + 240
    while time.monotonic() < deadline:
        if _accepted_history_orders() >= expected:
            return
        time.sleep(2)
    raise SystemExit(f"accepted historical orders did not reach {expected}")


def _accepted_history_orders() -> int:
    import psycopg

    prefix = f"ord_h{HISTORY_SEED % 10000:04d}%"
    with psycopg.connect(_database_url()) as connection:
        row = connection.execute(
            """
            select count(*)
            from commerce_events
            where event_type = 'order_created'
              and disposition = 'accepted'
              and order_id like %s
            """,
            (prefix,),
        ).fetchone()
    return 0 if row is None else int(row[0])


def _database_url() -> str:
    url = os.environ.get(
        "DATABASE_URL",
        "postgresql://titan:titan_dev_password@localhost:5435/titan",
    )
    return url.replace("postgresql+psycopg://", "postgresql://", 1)


def _train() -> str:
    completed = subprocess.run(
        ["uv", "run", "--group", "orchestration", "python", "-m", "titan_ml"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        env=_python_env(),
    )
    if completed.returncode != 0:
        raise SystemExit(completed.stderr[-4000:] or completed.stdout[-4000:])
    return completed.stdout.strip().splitlines()[-1]


def _schedule_tick() -> None:
    code = """
from datetime import UTC, datetime
from dagster import build_schedule_context
from titan_dagster.definitions import hourly_analytics
context = build_schedule_context(
    scheduled_execution_time=datetime(2026, 11, 22, 15, 15, tzinfo=UTC)
)
evaluation = hourly_analytics.evaluate_tick(context)
requests = list(evaluation.run_requests or [])
if not requests:
    raise SystemExit("hourly schedule tick returned no run request")
print(requests[0].run_key or "tick")
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
        raise SystemExit(completed.stderr[-4000:] or completed.stdout[-4000:])


def _warehouse(*, expect_ok: bool = True) -> None:
    completed = subprocess.run(
        ["uv", "run", "--group", "orchestration", "python", "-m", "titan_warehouse"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        env=_python_env(),
    )
    if expect_ok and completed.returncode != 0:
        raise SystemExit(completed.stderr[-4000:] or completed.stdout[-4000:])
    if not expect_ok and completed.returncode == 0:
        raise SystemExit("injected quality gate published a candidate")


def _distinct_item_dates() -> int:

    client = _clickhouse()
    try:
        row = client.query("select uniqExact(toDate(occurred_at)) from fct_order_items").result_rows
    except Exception:
        return 0
    finally:
        client.close()
    return int(row[0][0]) if row else 0


def _active_publication() -> str:
    client = _clickhouse()
    try:
        return str(client.query("select warehouse_publication_id from wh_active").result_rows[0][0])
    finally:
        client.close()


def _clickhouse() -> Any:
    import clickhouse_connect

    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "titan"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", "titan_dev_password"),
        database=os.environ.get("CLICKHOUSE_DATABASE", "titan"),
    )


def _overview(base: str) -> dict[str, Any]:
    body = _json(base, "/api/v1/analytics/overview")
    if not isinstance(body, dict) or not body.get("warehouse_publication_id"):
        raise SystemExit(f"warehouse overview is unavailable: {body}")
    return cast(dict[str, Any], body)


def _json(base: str, path: str) -> Any:
    with urllib.request.urlopen(base + path, timeout=30) as response:
        return json.load(response)


def _page(path: str) -> None:
    deadline = time.monotonic() + 45
    last = "not requested"
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen("http://localhost:3000" + path, timeout=10) as response:
                html = response.read().decode("utf-8", errors="replace")
        except Exception as exc:
            last = str(exc)
            time.sleep(2)
            continue
        if response.status == 200 and ("Loading" in html or "TITAN" in html):
            return
        last = f"status {response.status}"
        time.sleep(2)
    raise SystemExit(f"{path} did not render: {last}")


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


def _compose(*args: str) -> None:
    completed = subprocess.run(["docker", "compose", *args], cwd=ROOT, check=False)
    if completed.returncode != 0:
        raise SystemExit(f"docker compose {' '.join(args)} failed")


def _docker_stats() -> str:
    completed = subprocess.run(
        ["docker", "stats", "--no-stream", "--format", "{{.Name}} {{.MemUsage}}"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _env() -> dict[str, str]:
    return dict(os.environ)


def _python_env() -> dict[str, str]:
    env = _env()
    root = str(ROOT)
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
    del root
    return env


if __name__ == "__main__":
    main()
