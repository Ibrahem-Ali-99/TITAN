"""End-to-end reliability checks on the disposable titan-e2e Compose project.

This process refuses to continue unless Postgres is mounted from
``titan-e2e_postgres_data``. It does not remove volumes. Dagster is skipped
unless an environment-only ``docker compose`` command also resolves to
``titan-e2e``, because the Dagster assets call Compose without ``-p``.
"""

from __future__ import annotations

import json
import os
import subprocess
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
PROJECT = "titan-e2e"
COMPOSE = [
    "docker",
    "compose",
    "-p",
    PROJECT,
    "-f",
    str(ROOT / "docker-compose.yml"),
    "-f",
    str(ROOT / "docker-compose.e2e.yml"),
]
API = "http://localhost:18000"
ADMIN = "e2e-local-admin"
EVIDENCE = ROOT / "docs" / "audits" / "e2e-evidence.json"
CSV_HEADER = (
    "event_type,occurred_at,order_id,customer_id,currency,product_id,quantity,"
    "unit_price,payment_id,amount,payment_method,quantity_on_hand,inventory_delta,warehouse_id"
)


def main() -> None:
    os.environ["COMPOSE_PROJECT_NAME"] = PROJECT
    os.environ["COMPOSE_FILE"] = f"{ROOT / 'docker-compose.yml'};{ROOT / 'docker-compose.e2e.yml'}"
    os.environ["DATABASE_URL"] = "postgresql://titan:titan_dev_password@localhost:15435/titan"
    os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "localhost:19092"
    os.environ["API_BASE_URL"] = API
    os.environ["CLICKHOUSE_HOST"] = "localhost"
    os.environ["CLICKHOUSE_PORT"] = "18123"
    os.environ["CLICKHOUSE_USER"] = "titan"
    os.environ["CLICKHOUSE_PASSWORD"] = "titan_dev_password"
    os.environ["CLICKHOUSE_DATABASE"] = "titan"
    os.environ["S3_ENDPOINT"] = "http://localhost:19000"
    os.environ["S3_ACCESS_KEY"] = "titan"
    os.environ["S3_SECRET_KEY"] = "titan_dev_password"
    phase = os.environ.get("TITAN_E2E_FROM", "start")
    if EVIDENCE.exists() and phase != "start":
        report = json.loads(EVIDENCE.read_text(encoding="utf-8"))
        report.pop("aborted", None)
    else:
        report = {
            "started_at": datetime.now(UTC).isoformat(),
            "project": PROJECT,
            "scenarios": {},
        }
    try:
        report["live_orders_before"] = report.get("live_orders_before", _live_orders())
        print("isolation ok", flush=True)
        _assert_isolated()
        if phase == "remainder":
            status = _get("/api/v1/warehouse/status")
            report["scenarios"]["reconcile_after_empty_facts"] = {
                "ready": status["ready"],
                "reason": status["reason"],
                "warehouse_publication_id": status["warehouse_publication_id"],
                "clickhouse_publication_id": status["clickhouse_publication_id"],
                "order_count": status["order_count"],
                "revenue": status["accepted_revenue"],
            }
            if not status["ready"] or status["order_count"] != 11:
                raise SystemExit(f"reconcile did not restore serving: {status}")
            _monitoring(report)
            print("dagster start", flush=True)
            _dagster(report)
            print("dagster done", flush=True)
            _spark_tests(report)
            _dagster_validate(report)
            report["live_orders_after"] = _live_orders()
            report["live_orders_unchanged"] = (
                report["live_orders_before"] == report["live_orders_after"]
            )
            return
        if phase == "publish":
            _republish(report)
            _failures(report)
            _monitoring(report)
            print("dagster start", flush=True)
            _dagster(report)
            print("dagster done", flush=True)
            _spark_tests(report)
            _dagster_validate(report)
            report["live_orders_after"] = _live_orders()
            report["live_orders_unchanged"] = (
                report["live_orders_before"] == report["live_orders_after"]
            )
            return
        report["isolation"] = {
            "postgres_volume": "titan-e2e_postgres_data",
            "api": API,
            "kafka": "localhost:19092",
            "clickhouse": "localhost:18123",
            "minio": "http://localhost:19000",
        }
        _compose("run", "--rm", "migrate")
        _compose("up", "-d", "ingestion", "api")
        _wait_ready()
        _wait_consumer()
        print("ingestion start", flush=True)
        _ingestion(report)
        print("ingestion done", flush=True)
        _integration(report)
        print("spark start", flush=True)
        _spark_and_warehouse(report)
        print("spark done", flush=True)
        _failures(report)
        _monitoring(report)
        print("dagster start", flush=True)
        _dagster(report)
        print("dagster done", flush=True)
        _spark_tests(report)
        _dagster_validate(report)
        report["live_orders_after"] = _live_orders()
        report["live_orders_unchanged"] = (
            report["live_orders_before"] == report["live_orders_after"]
        )
    except Exception as exc:
        report["aborted"] = f"{exc.__class__.__name__}: {exc}"
        raise
    finally:
        report["finished_at"] = datetime.now(UTC).isoformat()
        EVIDENCE.parent.mkdir(parents=True, exist_ok=True)
        EVIDENCE.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
        print(
            json.dumps(
                {key: report.get(key) for key in ("aborted", "live_orders_unchanged", "scenarios")},
                indent=2,
                default=str,
            )
        )


def _assert_isolated() -> None:
    mounts = subprocess.run(
        [
            "docker",
            "inspect",
            "titan-e2e-postgres-1",
            "--format",
            "{{range .Mounts}}{{.Name}} {{end}}",
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    if "titan-e2e_postgres_data" not in mounts or "titan_postgres_data" in mounts:
        raise SystemExit(f"e2e postgres is not on an isolated volume: {mounts!r}")
    for name in (
        "titan-e2e-kafka-1",
        "titan-e2e-clickhouse-1",
        "titan-e2e-minio-1",
    ):
        volume = subprocess.run(
            ["docker", "inspect", name, "--format", "{{range .Mounts}}{{.Name}} {{end}}"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        if "titan_" in volume.replace("titan-e2e_", ""):
            raise SystemExit(f"{name} mounts a development volume: {volume!r}")


def _ingestion(report: dict[str, Any]) -> None:
    from event_generator.generator import generate_orders
    from titan_contracts.money import order_total, parse_money
    from titan_messaging.producer import connect_producer, publish_record

    events = generate_orders(
        4,
        seed=221122,
        deterministic_event_ids=True,
        occurred_span=timedelta(hours=48),
        late_every=3,
        late_by=timedelta(hours=20),
    )
    late = events[2]
    for event in events:
        _post_json("/api/v1/ingestion/events", event.model_dump(mode="json"))
    _post_json("/api/v1/ingestion/events", events[0].model_dump(mode="json"))
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
    _post_json("/api/v1/ingestion/events", conflict)
    duplicate = generate_orders(1, seed=221123, deterministic_event_ids=True)[0]
    producer = connect_producer("localhost:19092")
    publish_record(producer, duplicate)
    publish_record(producer, duplicate)
    producer.flush(10)
    producer.produce("titan.commerce.order-events", key=b"ord_e2ebadkey1", value=b"not-json")
    producer.flush(10)
    for event in (*events, duplicate):
        _wait_event(str(event.event_id))
    _wait_event(conflict["event_id"])
    _wait_quarantine()
    csv_body = (
        CSV_HEADER
        + "\n"
        + "order_created,2026-01-15T12:00:00+00:00,ord_e2ecsv000001,cus_e2ecsv000001,EUR,"
        + "prd_100000000001,2,12.50,,,,,,\n"
        + "order_created,not-a-date,ord_e2ebad000001,cus_e2ebad000001,EUR,"
        + "prd_100000000001,1,4.00,,,,,,\n"
    )
    imported = _post_csv(csv_body.encode())
    _wait_stat("logical_orders", 6)
    quarantine = _get("/api/v1/ingestion/quarantine?limit=20")
    malformed = next(item for item in quarantine["items"] if item["reason_code"])
    refused = _post_json(
        "/api/v1/ingestion/replays",
        {"quarantine_ids": [malformed["id"]]},
        admin=True,
    )
    replacement = generate_orders(1, seed=221124, deterministic_event_ids=True)[0]
    replaced = _post_json(
        "/api/v1/ingestion/replays",
        {
            "quarantine_ids": [malformed["id"]],
            "replacement": replacement.model_dump(mode="json"),
        },
        admin=True,
    )
    _wait_event(str(replacement.event_id))
    replayed = _post_json(
        "/api/v1/ingestion/replays",
        {"event_ids": [str(events[0].event_id)]},
        admin=True,
    )
    _wait_event(str(events[0].event_id))
    before_restart = _get("/api/v1/ingestion/stats")["logical_orders"]
    _compose("stop", "ingestion")
    held = generate_orders(1, seed=221125, deterministic_event_ids=True)[0]
    _post_json("/api/v1/ingestion/events", held.model_dump(mode="json"))
    time.sleep(2)
    if _event_visible(str(held.event_id)):
        raise SystemExit("event persisted while the consumer was stopped")
    _compose("start", "ingestion")
    _wait_consumer()
    _wait_event(str(held.event_id))
    after = _get("/api/v1/ingestion/stats")
    orders = _psql(
        "select order_id, disposition, count(*) from commerce_events "
        "where event_type = 'order_created' group by 1, 2 order by 1, 2"
    )
    report["scenarios"]["ingestion"] = {
        "late_order_id": late.payload.order_id,
        "late_occurred_at": late.occurred_at.isoformat(),
        "csv_import": imported,
        "malformed_replay": refused,
        "replacement_replay": replaced,
        "persisted_replay": replayed,
        "logical_orders_before_restart": before_restart,
        "stats": {
            "logical_orders": after["logical_orders"],
            "order_conflicts": after["order_conflicts"],
            "duplicate_deliveries": after["duplicate_deliveries"],
            "quarantined_records": after["quarantined_records"],
            "csv_rows_accepted": after["csv_rows_accepted"],
            "csv_rows_rejected": after["csv_rows_rejected"],
            "replays_published": after["replays_published"],
            "replays_refused": after["replays_refused"],
        },
        "order_rows": orders,
    }
    if after["logical_orders"] != before_restart + 1:
        raise SystemExit(
            f"consumer restart changed logical orders by more than the held event: {after}"
        )
    if after["order_conflicts"] < 1 or after["quarantined_records"] < 1:
        raise SystemExit(f"conflict or quarantine was not recorded: {after}")
    if refused["items"][0]["outcome"] != "refused":
        raise SystemExit(f"malformed replay was not refused: {refused}")
    if replaced["items"][0]["outcome"] != "published":
        raise SystemExit(f"replacement replay was not published: {replaced}")


def _integration(report: dict[str, Any]) -> None:
    completed = _run(
        ["uv", "run", "pytest", "-m", "integration", "-q", "--tb=line"],
        timeout=180,
    )
    report["scenarios"]["integration_pytest"] = {
        "returncode": completed.returncode,
        "tail": (completed.stdout + completed.stderr)[-2000:],
    }
    if completed.returncode != 0:
        raise SystemExit("integration pytest failed")


def _spark_and_warehouse(report: dict[str, Any]) -> None:
    _compose("--profile", "processing", "run", "--rm", "lake-init")
    _compose("--profile", "processing", "run", "--rm", "spark-bronze")
    first_commits = _psql("select count(*) from bronze_batch_commits")
    extra = __import__("event_generator.generator", fromlist=["generate_orders"]).generate_orders(
        1, seed=221126, deterministic_event_ids=True
    )[0]
    _post_json("/api/v1/ingestion/events", extra.model_dump(mode="json"))
    _wait_event(str(extra.event_id))
    _compose("--profile", "processing", "run", "--rm", "spark-bronze")
    second_commits = _psql("select count(*) from bronze_batch_commits")
    running = _psql(
        "select count(*) from pipeline_runs where state = 'running' and finished_at is not null"
    )
    _compose("--profile", "processing", "run", "--rm", "spark-silver")
    silver = _psql(
        "select publication_id::text, status, silver_order_count "
        "from lake_publications order by finished_at desc nulls last limit 1"
    )
    publication = _module("titan_warehouse")
    status = _get("/api/v1/warehouse/status")
    revenue = _currency_revenue()
    report["scenarios"]["spark_warehouse"] = {
        "bronze_commits_after_first": first_commits.strip(),
        "bronze_commits_after_second": second_commits.strip(),
        "running_rows_with_finish": running.strip(),
        "silver": silver.strip(),
        "warehouse_command_tail": publication[-1500:],
        "status": {
            "ready": status["ready"],
            "warehouse_publication_id": status["warehouse_publication_id"],
            "order_count": status["order_count"],
            "item_count": status["item_count"],
            "accepted_revenue": status["accepted_revenue"],
        },
        "postgres_revenue": revenue,
    }
    if running.strip() != "0":
        raise SystemExit("a running pipeline row was stored with finished_at")
    if not status["ready"]:
        raise SystemExit(f"warehouse did not become ready: {status}")
    if status["accepted_revenue"] != revenue:
        raise SystemExit(f"revenue diverged: api {status['accepted_revenue']} postgres {revenue}")
    if status["order_count"] != _logical_orders():
        raise SystemExit("serving orders do not match accepted logical orders")


def _republish(report: dict[str, Any]) -> None:
    """A direct warehouse command must record the dbt job the API uses for readiness."""
    publication = _module("titan_warehouse")
    status = _get("/api/v1/warehouse/status")
    models = _psql(
        "select count(*) from pipeline_runs where job_name = 'warehouse_models' "
        "and state = 'succeeded' and detail = ("
        "select load_id::text from warehouse_loads where status = 'published' "
        "order by finished_at desc limit 1)"
    )
    report["scenarios"]["direct_publish_records_dbt"] = {
        "tail": publication[-500:],
        "ready": status["ready"],
        "reason": status["reason"],
        "order_count": status["order_count"],
        "revenue": status["accepted_revenue"],
        "warehouse_models_rows": models.strip(),
    }
    if not status["ready"] or models.strip() != "1":
        raise SystemExit(f"direct publish is not ready with a dbt record: {status} rows={models}")


def _failures(report: dict[str, Any]) -> None:
    pointer = _get("/api/v1/warehouse/status")["warehouse_publication_id"]
    orders = _get("/api/v1/warehouse/status")["order_count"]
    inserted = _module("titan_warehouse", "--crash-after", "inserted", expect_ok=False)
    exchanged = _module("titan_warehouse", "--crash-after", "exchanged", expect_ok=False)
    reconciled = _module("titan_warehouse", "--reconcile")
    os.environ["TITAN_DBT_GATE"] = "fail"
    try:
        gated = _module("titan_warehouse", expect_ok=False)
    finally:
        os.environ.pop("TITAN_DBT_GATE", None)
    os.environ["TITAN_QUALITY_INJECT"] = "revenue_mismatch"
    try:
        quality = _module("titan_warehouse", expect_ok=False)
    finally:
        os.environ.pop("TITAN_QUALITY_INJECT", None)
    status = _get("/api/v1/warehouse/status")
    report["scenarios"]["publication_failures"] = {
        "pointer_before": pointer,
        "orders_before": orders,
        "crash_inserted_tail": inserted[-800:],
        "crash_exchanged_tail": exchanged[-800:],
        "reconcile_tail": reconciled[-800:],
        "dbt_gate_tail": gated[-800:],
        "quality_inject_tail": quality[-800:],
        "pointer_after": status["warehouse_publication_id"],
        "orders_after": status["order_count"],
        "ready": status["ready"],
        "revenue": status["accepted_revenue"],
    }
    if status["warehouse_publication_id"] != pointer or status["order_count"] != orders:
        raise SystemExit(f"failure injection changed the serving publication: {status}")
    if not status["ready"]:
        raise SystemExit(f"serving warehouse was left unready: {status}")


def _monitoring(report: dict[str, Any]) -> None:
    _psql(
        "insert into pipeline_runs ("
        "job_name, state, started_at, finished_at, detail, metadata_json"
        ") values ("
        "'e2e_scope_probe', 'failed', now(), now(), 'scope a', "
        "'{\"publication_id\":\"pub-a\"}'"
        "), ("
        "'e2e_scope_probe', 'succeeded', now(), now(), 'scope b', "
        "'{\"publication_id\":\"pub-b\"}'"
        ")"
    )
    jobs = _get("/api/v1/monitoring/jobs")
    page = _get("/api/v1/monitoring/runs?limit=1")
    scoped = next(job for job in jobs["jobs"] if job["job_name"] == "e2e_scope_probe")
    load_rows = _psql(
        "select state, finished_at is null from pipeline_runs "
        "where job_name = 'warehouse_load' order by id desc limit 6"
    )
    report["scenarios"]["monitoring"] = {
        "latest_scope_probe": scoped,
        "runs_page_limit_1": page["runs"][0]["job_name"] if page["runs"] else None,
        "jobs_returned": jobs["returned"],
        "warehouse_load_recent": load_rows.strip(),
    }
    if scoped["state"] != "succeeded" or scoped["scope"] != "pub-b":
        raise SystemExit(f"latest job ignored publication scope: {scoped}")
    if scoped["id"] in {run["id"] for run in page["runs"]} or page["runs"][0]["id"] != scoped["id"]:
        # The newest row is the probe success, so limit=1 shows it. The failed
        # scope-a row is older and must not be the jobs payload.
        failed = _psql(
            "select id from pipeline_runs where job_name = 'e2e_scope_probe' and state = 'failed'"
        ).strip()
        if page["runs"][0]["id"] == int(failed):
            raise SystemExit("pagination returned the older failure as the only history row")


def _dagster(report: dict[str, Any]) -> None:
    if not _compose_env_is_isolated():
        report["scenarios"]["dagster"] = {
            "status": "NOT VERIFIED",
            "reason": "compose env is not titan-e2e",
        }
        return
    before = _get("/api/v1/warehouse/status")
    completed = _module_raw(
        "titan_dagster",
        "silver_publication",
        "clickhouse_load",
        "warehouse_models",
        "quality_gate",
        "analytics_ready",
    )
    after = _get("/api/v1/warehouse/status")
    report["scenarios"]["dagster"] = {
        "returncode": completed.returncode,
        "tail": (completed.stdout + completed.stderr)[-2000:],
        "orders_before": before["order_count"],
        "orders_after": after["order_count"],
        "ready": after["ready"],
        "publication_changed": before["warehouse_publication_id"]
        != after["warehouse_publication_id"],
        "revenue": after["accepted_revenue"],
    }
    if (
        completed.returncode != 0
        or not after["ready"]
        or after["order_count"] != before["order_count"]
    ):
        raise SystemExit("Dagster publication changed serving orders or failed")
    if after["accepted_revenue"] != before["accepted_revenue"]:
        raise SystemExit("Dagster reload changed currency revenue")


def _spark_tests(report: dict[str, Any]) -> None:
    completed = _compose_raw("--profile", "processing", "run", "--rm", "spark-tests")
    report["scenarios"]["spark_tests"] = {
        "returncode": completed.returncode,
        "tail": (completed.stdout + completed.stderr)[-2000:],
    }
    if completed.returncode != 0:
        raise SystemExit("spark tests failed")


def _dagster_validate(report: dict[str, Any]) -> None:
    home = ROOT / ".dagster-e2e"
    home.mkdir(exist_ok=True)
    env = os.environ.copy()
    env["DAGSTER_HOME"] = str(home)
    env["PYTHONPATH"] = os.pathsep.join(
        [str(ROOT / "pipelines" / "dagster" / "src"), env.get("PYTHONPATH", "")]
    )
    completed = _run(
        [
            "uv",
            "run",
            "--group",
            "orchestration",
            "dagster",
            "definitions",
            "validate",
            "-w",
            str(ROOT / "pipelines" / "dagster" / "workspace.yaml"),
        ],
        timeout=180,
        env=env,
    )
    report["scenarios"]["dagster_validate"] = {
        "returncode": completed.returncode,
        "tail": (completed.stdout + completed.stderr)[-1500:],
    }
    if completed.returncode != 0:
        raise SystemExit("dagster definitions validate failed")


def _compose_env_is_isolated() -> bool:
    completed = subprocess.run(
        ["docker", "compose", "ps", "--format", "{{.Name}}"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        env=os.environ.copy(),
    )
    names = [line for line in completed.stdout.splitlines() if line]
    return bool(names) and all(name.startswith("titan-e2e-") for name in names)


def _currency_revenue() -> dict[str, str]:
    rows = _psql(
        "select event_envelope->'payload'->>'currency', "
        "sum((event_envelope->'payload'->>'total_amount')::numeric) "
        "from commerce_events where event_type = 'order_created' and disposition = 'accepted' "
        "group by 1 order by 1"
    )
    revenue: dict[str, str] = {}
    for line in rows.splitlines():
        currency, amount = line.split("|")
        revenue[currency] = format(Decimal(amount).quantize(Decimal("0.01")), "f")
    return revenue


def _logical_orders() -> int:
    return int(
        _psql(
            "select count(*) from commerce_events "
            "where event_type = 'order_created' and disposition = 'accepted'"
        ).strip()
    )


def _live_orders() -> int | None:
    completed = subprocess.run(
        [
            "docker",
            "exec",
            "titan-postgres-1",
            "psql",
            "-U",
            "titan",
            "-d",
            "titan",
            "-tAc",
            "select count(*) from commerce_events "
            "where event_type = 'order_created' and disposition = 'accepted'",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        return None
    return int(completed.stdout.strip())


def _psql(sql: str) -> str:
    completed = subprocess.run(
        [
            "docker",
            "exec",
            "titan-e2e-postgres-1",
            "psql",
            "-U",
            "titan",
            "-d",
            "titan",
            "-tA",
            "-c",
            sql,
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip("\n")


def _module(module: str, *args: str, expect_ok: bool = True) -> str:
    completed = _module_raw(module, *args)
    if expect_ok and completed.returncode != 0:
        raise SystemExit((completed.stdout + completed.stderr)[-4000:])
    if not expect_ok and completed.returncode == 0:
        raise SystemExit(f"{module} succeeded when a failure was required")
    return (completed.stdout + completed.stderr)[-4000:]


def _module_raw(module: str, *args: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(
        [
            str(ROOT / "pipelines" / "warehouse" / "src"),
            str(ROOT / "pipelines" / "quality" / "src"),
            str(ROOT / "pipelines" / "ml" / "src"),
            str(ROOT / "pipelines" / "dagster" / "src"),
            env.get("PYTHONPATH", ""),
        ]
    )
    return _run(
        ["uv", "run", "--group", "orchestration", "python", "-m", module, *args],
        timeout=1200,
        env=env,
    )


def _compose(*args: str) -> str:
    completed = _compose_raw(*args)
    if completed.returncode != 0:
        raise SystemExit((completed.stdout + completed.stderr)[-4000:])
    return completed.stdout


def _compose_raw(*args: str) -> subprocess.CompletedProcess[str]:
    return _run([*COMPOSE, *args], timeout=1200)


def _run(
    command: list[str],
    *,
    timeout: int,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=ROOT,
        env=env,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def _post_json(path: str, payload: dict[str, Any], *, admin: bool = False) -> dict[str, Any]:
    headers = {"Content-Type": "application/json"}
    if admin:
        headers["Authorization"] = f"Bearer {ADMIN}"
    request = urllib.request.Request(
        API + path,
        data=json.dumps(payload).encode(),
        headers=headers,
        method="POST",
    )
    return _read(request)


def _post_csv(raw: bytes) -> dict[str, Any]:
    boundary = "titan-e2e-boundary"
    body = (
        (
            f"--{boundary}\r\n"
            'Content-Disposition: form-data; name="file"; filename="orders.csv"\r\n'
            "Content-Type: text/csv\r\n\r\n"
        ).encode()
        + raw
        + f"\r\n--{boundary}--\r\n".encode()
    )
    request = urllib.request.Request(
        API + "/api/v1/ingestion/imports",
        data=body,
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        method="POST",
    )
    return _read(request)


def _get(path: str) -> dict[str, Any]:
    return _read(urllib.request.Request(API + path))


def _read(request: urllib.request.Request) -> dict[str, Any]:
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            body = json.loads(response.read().decode())
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode()[:2000]
        raise SystemExit(f"{request.full_url} returned {exc.code}: {detail}") from exc
    if not isinstance(body, dict):
        raise RuntimeError("response was not an object")
    return body


def _wait_ready() -> None:
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        try:
            if _get("/health/ready")["status"] == "ready":
                return
        except Exception:
            time.sleep(2)
    raise SystemExit("isolated API did not become ready")


def _wait_consumer() -> None:
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        completed = subprocess.run(
            ["docker", "inspect", "--format", "{{.State.Health.Status}}", "titan-e2e-ingestion-1"],
            check=False,
            capture_output=True,
            text=True,
        )
        if completed.stdout.strip() == "healthy":
            return
        time.sleep(2)
    raise SystemExit("isolated ingestion did not become healthy")


def _wait_event(event_id: str) -> None:
    deadline = time.monotonic() + 45
    while time.monotonic() < deadline:
        if _event_visible(event_id):
            return
        time.sleep(0.5)
    raise SystemExit(f"event {event_id} was not persisted")


def _event_visible(event_id: str) -> bool:
    recent = _get("/api/v1/events/recent?limit=100")
    return any(item["event_id"] == event_id for item in recent["items"])


def _wait_quarantine() -> None:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if _get("/api/v1/ingestion/stats")["quarantined_records"] >= 1:
            return
        time.sleep(0.5)
    raise SystemExit("malformed record was not quarantined")


def _wait_stat(name: str, minimum: int) -> None:
    deadline = time.monotonic() + 40
    while time.monotonic() < deadline:
        if int(_get("/api/v1/ingestion/stats")[name]) >= minimum:
            return
        time.sleep(0.5)
    raise SystemExit(f"{name} did not reach {minimum}")


if __name__ == "__main__":
    main()
