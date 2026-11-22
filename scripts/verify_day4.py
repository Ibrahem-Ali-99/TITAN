"""Publish Silver into ClickHouse and check the warehouse failure gates.

Requires the Compose stack. The script runs Spark, dbt, and one Dagster materialization.
"""

import json
import os
import subprocess
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import uuid4

from envfile import load_env_file
from event_generator.generator import _occurred_at, generate_orders
from titan_contracts.money import order_total, parse_money

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    load_env_file(ROOT / ".env")
    _compose("run", "--rm", "--build", "migrate")
    _compose("up", "-d", "--build", "api")
    _compose("restart", "web")
    base = os.environ.get("API_BASE_URL", "http://localhost:8000")
    _wait_ready(base)

    before = _json(base, "/api/v1/ingestion/stats")
    span = timedelta(hours=48)
    late_by = timedelta(hours=20)
    events = generate_orders(
        6,
        seed=_seed(),
        deterministic_event_ids=False,
        occurred_span=span,
        late_every=3,
        late_by=late_by,
    )
    late = events[2]
    late_at = _occurred_at(2, 6, span, 3, late_by)
    for event in events:
        _send(base, event.model_dump(mode="json"))
    _send(base, events[0].model_dump(mode="json"))
    conflict = events[0].model_dump(mode="json")
    conflict["event_id"] = str(uuid4())
    item = conflict["payload"]["items"][0]
    item["quantity"] = 3 if item["quantity"] == 2 else 2
    conflict["payload"]["total_amount"] = format(
        order_total(
            (line["quantity"], parse_money(line["unit_price"]))
            for line in conflict["payload"]["items"]
        ),
        "f",
    )
    _send(base, conflict)
    for event in events:
        _wait_event(base, str(event.event_id))
    _wait_event(base, conflict["event_id"])
    after = _json(base, "/api/v1/ingestion/stats")
    if after["logical_orders"] != before["logical_orders"] + len(events):
        raise SystemExit(
            "logical orders moved by more than the new orders: "
            f"{before['logical_orders']} -> {after['logical_orders']}"
        )

    _compose("--profile", "processing", "run", "--rm", "spark-bronze")
    _compose("--profile", "processing", "run", "--rm", "spark-silver")
    started = time.perf_counter()
    first = _warehouse()
    first_seconds = time.perf_counter() - started
    stats = _docker_stats()
    _assert_reconciled(base, late.payload.order_id, late_at)
    reloaded = _warehouse()
    if _active_orders() != _published_order_count():
        raise SystemExit("reloading the same Silver snapshot changed the serving order count")
    if reloaded == first:
        raise SystemExit("reload reused a warehouse publication id")

    extra = generate_orders(1, seed=_seed(), deterministic_event_ids=False)[0]
    _send(base, extra.model_dump(mode="json"))
    _wait_event(base, str(extra.event_id))
    _compose("--profile", "processing", "run", "--rm", "spark-silver")
    before_new = _active_orders()
    _warehouse()
    if _active_orders() != before_new + 1:
        raise SystemExit(f"newer Silver did not add one order: {before_new} -> {_active_orders()}")

    pointer = _active_publication()
    failed = _warehouse("--crash-after", "inserted", expect_ok=False)
    if _active_publication() != pointer:
        raise SystemExit(f"partial load replaced the active publication: {failed}")
    os.environ["TITAN_DBT_GATE"] = "fail"
    gated = _warehouse(expect_ok=False)
    os.environ.pop("TITAN_DBT_GATE", None)
    if _active_publication() != pointer:
        raise SystemExit(f"failed dbt test replaced the active publication: {gated}")
    stale = _older_silver()
    refused = _warehouse("--silver", stale, expect_ok=False)
    if _active_publication() != pointer:
        raise SystemExit(f"stale Silver replaced the active publication: {refused}")

    dagster_started = time.perf_counter()
    _dagster("clickhouse_load", "warehouse_models", "analytics_ready")
    dagster_seconds = time.perf_counter() - dagster_started
    if _active_orders() != _published_order_count():
        raise SystemExit("Dagster promotion does not match the serving view")
    for job_name, state in (
        ("clickhouse_load", "succeeded"),
        ("warehouse_models", "succeeded"),
        ("analytics_ready", "succeeded"),
        ("warehouse_load", "failed"),
    ):
        if not _has_run(job_name, state):
            raise SystemExit(f"pipeline history is missing {job_name} {state}")

    status = _json(base, "/api/v1/warehouse/status")
    overview = _json(base, "/api/v1/analytics/overview")
    revenue = _json(base, "/api/v1/analytics/revenue?grain=day&limit=30")
    products = _json(base, "/api/v1/analytics/products?limit=20")
    publication = status["warehouse_publication_id"]
    if not status["ready"] or publication != overview["warehouse_publication_id"]:
        raise SystemExit(f"API publication is not ready: {status}")
    if overview["order_count"] != _active_orders():
        raise SystemExit("overview order count does not match ClickHouse")
    if len({point["currency"] for point in revenue["points"]}) < 2:
        raise SystemExit(f"revenue response has one currency: {revenue}")
    pages = {path: _page(path) for path in ("/", "/warehouse", "/pipelines")}
    print(
        json.dumps(
            {
                "warehouse_publication_id": status["warehouse_publication_id"],
                "silver_publication_id": status["silver_publication_id"],
                "orders": overview["order_count"],
                "revenue": overview["accepted_revenue"],
                "products": products["returned"],
                "freshness_seconds": status["freshness_seconds"],
                "first_load_seconds": round(first_seconds, 1),
                "dagster_seconds": round(dagster_seconds, 1),
                "docker_stats": stats,
                "pages": pages,
            },
            indent=2,
        )
    )


def _assert_reconciled(base: str, late_order_id: str, late_at: datetime) -> None:
    import clickhouse_connect

    published = _published()
    client = clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "titan"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", "titan_dev_password"),
        database=os.environ.get("CLICKHOUSE_DATABASE", "titan"),
    )
    row = client.query("select warehouse_publication_id from wh_active limit 1").result_rows
    active = row[0][0]
    if active != published["warehouse_publication_id"]:
        raise SystemExit(f"pointer {active} is not the published load {published}")
    orders = int(client.query("select count() from fct_orders").result_rows[0][0])
    if orders != published["order_count"]:
        raise SystemExit(f"ClickHouse orders {orders} != published {published['order_count']}")
    revenue = {
        str(row[0]): format(Decimal(str(row[1])).quantize(Decimal("0.01")), "f")
        for row in client.query(
            "select currency, sum(total_amount) from fct_orders group by currency"
        ).result_rows
    }
    if revenue != published["revenue"]:
        raise SystemExit(f"revenue {revenue} != silver {published['revenue']}")
    if len(revenue) < 2:
        raise SystemExit(f"expected at least two currencies: {revenue}")
    start = late_at.isoformat().replace("+00:00", "Z")
    end = (late_at + timedelta(seconds=1)).isoformat().replace("+00:00", "Z")
    window = _json(base, f"/api/v1/analytics/orders?start={start}&end={end}&limit=50")
    if late_order_id not in {row["order_id"] for row in window["orders"]}:
        raise SystemExit(f"late order {late_order_id} was not in {start}: {window}")
    client.close()


def _published() -> dict[str, Any]:
    import psycopg

    with psycopg.connect(_database_url()) as connection:
        row = connection.execute(
            """
            select warehouse_publication_id, silver_publication_id,
                   order_count, accepted_revenue_json
            from warehouse_loads
            where status = 'published' and finished_at is not null
            order by finished_at desc
            limit 1
            """
        ).fetchone()
    if row is None:
        raise SystemExit("no published warehouse load")
    return {
        "warehouse_publication_id": str(row[0]),
        "silver_publication_id": str(row[1]),
        "order_count": int(row[2]),
        "revenue": json.loads(row[3]),
    }


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
        rows = client.query("select warehouse_publication_id from wh_active limit 1").result_rows
        return str(rows[0][0])
    finally:
        client.close()


def _active_orders() -> int:
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "titan"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", "titan_dev_password"),
        database=os.environ.get("CLICKHOUSE_DATABASE", "titan"),
    )
    try:
        return int(client.query("select count() from fct_orders").result_rows[0][0])
    finally:
        client.close()


def _published_order_count() -> int:
    return int(_published()["order_count"])


def _older_silver() -> str:
    import psycopg

    with psycopg.connect(_database_url()) as connection:
        rows = connection.execute(
            """
            select publication_id
            from lake_publications
            where status = 'published' and finished_at is not null
            order by finished_at desc
            limit 2
            """
        ).fetchall()
    if len(rows) < 2:
        raise SystemExit("need two Silver publications to test a stale load")
    return str(rows[1][0])


def _has_run(job_name: str, state: str) -> bool:
    import psycopg

    with psycopg.connect(_database_url()) as connection:
        row = connection.execute(
            "select 1 from pipeline_runs where job_name = %s and state = %s limit 1",
            (job_name, state),
        ).fetchone()
    return row is not None


def _database_url() -> str:
    url = os.environ.get(
        "DATABASE_URL", "postgresql://titan:titan_dev_password@localhost:5435/titan"
    )
    return url.replace("postgresql+psycopg://", "postgresql://", 1)


def _warehouse(*args: str, expect_ok: bool = True) -> str:
    completed = _python("titan_warehouse", *args)
    if expect_ok and completed.returncode != 0:
        raise SystemExit(completed.stderr[-4000:] or completed.stdout[-4000:])
    if not expect_ok and completed.returncode == 0:
        raise SystemExit("warehouse command succeeded when a failure was required")
    return (completed.stdout or completed.stderr)[-500:]


def _dagster(*assets: str) -> None:
    completed = _python("titan_dagster", *assets)
    if completed.returncode != 0:
        raise SystemExit(completed.stderr[-4000:] or completed.stdout[-4000:])


def _python(module: str, *args: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(
        [
            str(ROOT / "pipelines" / "warehouse" / "src"),
            str(ROOT / "pipelines" / "dagster" / "src"),
            env.get("PYTHONPATH", ""),
        ]
    )
    env.setdefault("CLICKHOUSE_HOST", "localhost")
    env.setdefault("S3_ENDPOINT", "http://localhost:9000")
    return subprocess.run(
        ["uv", "run", "--group", "orchestration", "python", "-m", module, *args],
        cwd=ROOT,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )


def _docker_stats() -> list[str]:
    completed = subprocess.run(
        ["docker", "stats", "--no-stream", "--format", "{{.Name}} {{.MemUsage}} {{.CPUPerc}}"],
        check=False,
        capture_output=True,
        text=True,
    )
    return [line for line in completed.stdout.splitlines() if line.startswith("titan-")]


def _page(path: str) -> int:
    with urllib.request.urlopen(f"http://localhost:3000{path}", timeout=30) as response:
        response.read()
        return int(response.status)


def _compose(*args: str) -> str:
    completed = subprocess.run(
        ["docker", "compose", *args],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise SystemExit(completed.stderr[-4000:] or completed.stdout[-4000:])
    return completed.stdout


def _send(base: str, payload: dict[str, Any]) -> None:
    request = urllib.request.Request(
        f"{base}/api/v1/ingestion/events",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        body = json.loads(response.read().decode())
    if response.status != 202 or body.get("persisted") is not False:
        raise SystemExit(f"unexpected publish response: {response.status} {body}")


def _json(base: str, path: str) -> dict[str, Any]:
    try:
        with urllib.request.urlopen(base + path, timeout=30) as response:
            raw = response.read().decode()
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode()[:2000]
        raise SystemExit(f"{path} returned {exc.code}: {detail}") from exc
    body = json.loads(raw)
    if not isinstance(body, dict):
        raise RuntimeError("response was not an object")
    return body


def _wait_ready(base: str) -> None:
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        try:
            if _json(base, "/health/ready")["status"] == "ready":
                return
        except Exception:
            time.sleep(2)
            continue
        time.sleep(2)
    raise SystemExit("API did not become ready")


def _wait_event(base: str, event_id: str) -> None:
    deadline = time.monotonic() + 40
    while time.monotonic() < deadline:
        recent = _json(base, "/api/v1/events/recent?limit=100")
        if any(item["event_id"] == event_id for item in recent["items"]):
            return
        time.sleep(0.5)
    raise SystemExit(f"event {event_id} was not persisted")


def _seed() -> int:
    import random

    return random.SystemRandom().randrange(1, 2**31 - 1)


if __name__ == "__main__":
    main()
