"""SIGKILL the Spark driver during a Bronze microbatch and restart it.

Uses Compose project ``titan-sparkkill`` and the e2e port override. It refuses
to continue unless Postgres is mounted from ``titan-sparkkill_postgres_data``.
After the Parquet copy, the batch writes a marker and waits. This script then
sends ``docker kill --signal=KILL`` to that container, before
``commit_bronze_batch`` and before Spark writes the checkpoint commit.
Volumes are not deleted.
"""

from __future__ import annotations

import json
import os
import subprocess
import time
import urllib.request
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
PROJECT = "titan-sparkkill"
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
EVIDENCE = ROOT / "docs" / "audits" / "spark-kill-evidence.json"


def main() -> None:
    os.environ["COMPOSE_PROJECT_NAME"] = PROJECT
    os.environ["COMPOSE_FILE"] = f"{ROOT / 'docker-compose.yml'};{ROOT / 'docker-compose.e2e.yml'}"
    for key, value in {
        "DATABASE_URL": "postgresql://titan:titan_dev_password@localhost:15435/titan",
        "KAFKA_BOOTSTRAP_SERVERS": "localhost:19092",
        "API_BASE_URL": API,
        "CLICKHOUSE_HOST": "localhost",
        "CLICKHOUSE_PORT": "18123",
        "CLICKHOUSE_USER": "titan",
        "CLICKHOUSE_PASSWORD": "titan_dev_password",
        "CLICKHOUSE_DATABASE": "titan",
        "S3_ENDPOINT": "http://localhost:19000",
        "S3_ACCESS_KEY": "titan",
        "S3_SECRET_KEY": "titan_dev_password",
    }.items():
        os.environ[key] = value
    report: dict[str, Any] = {"started_at": datetime.now(UTC).isoformat(), "project": PROJECT}
    try:
        report["live_orders_before"] = _live_orders()
        _compose("up", "-d", "postgres", "kafka", "clickhouse", "minio")
        _wait_healthy("titan-sparkkill-postgres-1")
        _assert_isolated()
        report["isolation"] = "titan-sparkkill_postgres_data"
        _compose("run", "--rm", "migrate")
        _compose("up", "-d", "ingestion", "api")
        _wait_ready()
        _wait_healthy("titan-sparkkill-ingestion-1")
        orders = _publish_dataset()
        report["dataset"] = orders
        _compose("--profile", "processing", "run", "--rm", "lake-init")
        report["checkpoint_before"] = _checkpoint_state()
        report["commits_before"] = _psql("select count(*) from bronze_batch_commits").strip()
        killed = _kill_during_batch()
        report["kill_exit_code"] = killed.returncode
        report["kill_log_tail"] = (killed.stdout + killed.stderr)[-1500:]
        report["checkpoint_after_kill"] = _checkpoint_state()
        report["commits_after_kill"] = _psql("select count(*) from bronze_batch_commits").strip()
        if killed.returncode == 0:
            raise SystemExit("the driver exited cleanly; the microbatch was not killed")
        if report["checkpoint_after_kill"].get("marker") != "bronze_staged":
            raise SystemExit(f"kill marker was not written inside the batch: {report}")
        if report["commits_after_kill"] != "0":
            raise SystemExit("bronze metadata was committed before the kill")
        commits = report["checkpoint_after_kill"].get("commits") or []
        if commits:
            raise SystemExit(f"checkpoint commit exists for an unfinished batch: {commits}")
        recovered = _compose_raw(
            "--profile", "processing", "run", "--rm", "--no-deps", "spark-bronze"
        )
        report["recovery_exit_code"] = recovered.returncode
        report["recovery_log_tail"] = (recovered.stdout + recovered.stderr)[-800:]
        if recovered.returncode != 0:
            raise SystemExit("bronze restart failed")
        report["bronze_after_recovery"] = _psql(
            "select batch_id, attempt, record_count, object_prefix from bronze_batch_commits "
            "order by batch_id"
        )
        report["parquet"] = _bronze_parquet()
        again = _compose_raw("--profile", "processing", "run", "--rm", "--no-deps", "spark-bronze")
        report["second_restart_exit_code"] = again.returncode
        report["bronze_after_second_restart"] = _psql(
            "select batch_id, attempt, record_count from bronze_batch_commits order by batch_id"
        )
        if report["bronze_after_second_restart"] != _count_only(report["bronze_after_recovery"]):
            raise SystemExit("a second restart changed the committed bronze batches")
        _compose("--profile", "processing", "run", "--rm", "--no-deps", "spark-silver")
        publication = _module("titan_warehouse")
        status = _get("/api/v1/warehouse/status")
        revenue = _currency_revenue()
        report["warehouse"] = {
            "tail": publication[-400:],
            "ready": status["ready"],
            "reason": status["reason"],
            "order_count": status["order_count"],
            "item_count": status["item_count"],
            "revenue": status["accepted_revenue"],
            "postgres_revenue": revenue,
            "publication_id": status["warehouse_publication_id"],
            "clickhouse_publication_id": status["clickhouse_publication_id"],
        }
        report["live_orders_after"] = _live_orders()
        report["live_orders_unchanged"] = (
            report["live_orders_before"] == report["live_orders_after"]
        )
        _assert_results(report, orders)
    except Exception as exc:
        report["aborted"] = f"{exc.__class__.__name__}: {exc}"
        raise
    finally:
        report["finished_at"] = datetime.now(UTC).isoformat()
        EVIDENCE.parent.mkdir(parents=True, exist_ok=True)
        EVIDENCE.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
        print(
            json.dumps(
                {
                    k: report.get(k)
                    for k in (
                        "kill_exit_code",
                        "commits_after_kill",
                        "checkpoint_after_kill",
                        "bronze_after_recovery",
                        "parquet",
                        "warehouse",
                        "live_orders_unchanged",
                        "aborted",
                    )
                },
                indent=2,
                default=str,
            )
        )


def _kill_during_batch() -> subprocess.CompletedProcess[str]:
    """Hold the microbatch open, then SIGKILL the driver container."""
    name = "titan-sparkkill-bronze-kill"
    proc = subprocess.Popen(
        [
            *COMPOSE,
            "--profile",
            "processing",
            "run",
            "--rm",
            "--no-deps",
            "--name",
            name,
            "-e",
            "TITAN_SPARK_KILL_AFTER=bronze_staged",
            "-e",
            "TITAN_SPARK_KILL_HOLD_SECONDS=120",
            "-e",
            "TITAN_SPARK_KILL_MARKER=/var/lib/titan/spark-checkpoints/kill-marker",
            "spark-bronze",
        ],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    deadline = time.monotonic() + 180
    killed = False
    kill_note = ""
    while time.monotonic() < deadline and proc.poll() is None:
        probe = subprocess.run(
            ["docker", "exec", name, "cat", "/var/lib/titan/spark-checkpoints/kill-marker"],
            check=False,
            capture_output=True,
            text=True,
        )
        if probe.stdout.strip() == "bronze_staged":
            killed_at = datetime.now(UTC).isoformat()
            result = subprocess.run(
                ["docker", "kill", "--signal=KILL", name],
                check=False,
                capture_output=True,
                text=True,
            )
            killed = result.returncode == 0
            kill_note = (
                f"\nexternal SIGKILL at {killed_at} signal=KILL container={name} "
                f"docker_kill_exit={result.returncode} marker=bronze_staged\n"
            )
            break
        time.sleep(1)
    try:
        stdout, _ = proc.communicate(timeout=60)
    except subprocess.TimeoutExpired:
        proc.kill()
        stdout, _ = proc.communicate()
    code = proc.returncode if proc.returncode is not None else 1
    tail = (stdout or "") + kill_note
    if not killed:
        tail += "\nexternal SIGKILL was not issued"
    return subprocess.CompletedProcess(["spark-bronze"], code, tail, "")


def _assert_results(report: dict[str, Any], orders: dict[str, Any]) -> None:
    parquet = report["parquet"]
    if parquet["rows"] != orders["kafka_messages"]:
        raise SystemExit(
            f"bronze rows {parquet['rows']} != kafka messages {orders['kafka_messages']}"
        )
    if parquet["invalid"] != 1:
        raise SystemExit(f"invalid bronze record was not kept: {parquet}")
    if parquet["distinct_offsets"] != parquet["rows"]:
        raise SystemExit(f"bronze offsets are duplicated: {parquet}")
    warehouse = report["warehouse"]
    if not warehouse["ready"] or warehouse["order_count"] != orders["logical_orders"]:
        raise SystemExit(f"serving publication is wrong: {warehouse}")
    if warehouse["revenue"] != warehouse["postgres_revenue"]:
        raise SystemExit(f"currency revenue diverged: {warehouse}")
    if warehouse["publication_id"] != warehouse["clickhouse_publication_id"]:
        raise SystemExit("postgres and clickhouse pointers differ")
    if not report["live_orders_unchanged"]:
        raise SystemExit("live development orders changed")


def _publish_dataset() -> dict[str, Any]:
    from event_generator.generator import generate_orders
    from titan_contracts.money import order_total, parse_money
    from titan_messaging.producer import connect_producer, publish_record

    events = generate_orders(
        4,
        seed=221190,
        deterministic_event_ids=True,
        occurred_span=timedelta(hours=48),
        late_every=3,
        late_by=timedelta(hours=20),
    )
    for event in events:
        _post(event.model_dump(mode="json"))
    _post(events[0].model_dump(mode="json"))
    conflict = events[0].model_dump(mode="json")
    conflict["event_id"] = "6f0d3e2a-1b7c-4d5e-8f90-a1b2c3d4e5f7"
    item = conflict["payload"]["items"][0]
    item["quantity"] = 3 if item["quantity"] == 2 else 2
    conflict["payload"]["total_amount"] = format(
        order_total(
            (line["quantity"], parse_money(line["unit_price"]))
            for line in conflict["payload"]["items"]
        ),
        "f",
    )
    _post(conflict)
    duplicate = generate_orders(1, seed=221191, deterministic_event_ids=True)[0]
    producer = connect_producer("localhost:19092")
    publish_record(producer, duplicate)
    publish_record(producer, duplicate)
    producer.produce("titan.commerce.order-events", key=b"ord_killbad01", value=b"not-json")
    producer.flush(10)
    for event in (*events, duplicate):
        _wait_event(str(event.event_id))
    _wait_event(conflict["event_id"])
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if _get("/api/v1/ingestion/stats")["quarantined_records"] >= 1:
            break
        time.sleep(0.5)
    else:
        raise SystemExit("invalid record was not quarantined")
    stats = _get("/api/v1/ingestion/stats")
    return {
        "late_order_id": events[2].payload.order_id,
        "logical_orders": stats["logical_orders"],
        "order_conflicts": stats["order_conflicts"],
        "duplicate_deliveries": stats["duplicate_deliveries"],
        "quarantined_records": stats["quarantined_records"],
        "kafka_messages": _kafka_messages(),
    }


def _kafka_messages() -> int:
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:19092",
            "group.id": "titan-sparkkill-audit",
            "enable.auto.commit": False,
        }
    )
    try:
        metadata = consumer.list_topics(timeout=10)
        total = 0
        for topic in (
            "titan.commerce.order-events",
            "titan.commerce.payment-events",
            "titan.commerce.inventory-events",
        ):
            partitions = metadata.topics[topic].partitions
            for number in partitions:
                low, high = consumer.get_watermark_offsets(
                    TopicPartition(topic, number), timeout=10
                )
                total += high - low
        return total
    finally:
        consumer.close()


def _bronze_parquet() -> dict[str, Any]:
    import pyarrow.dataset as ds  # type: ignore[import-untyped]
    import pyarrow.fs as pafs  # type: ignore[import-untyped]

    prefix = _psql("select object_prefix from bronze_batch_commits order by batch_id").strip()
    path = prefix.replace("s3a://", "").rstrip("/")
    filesystem = pafs.S3FileSystem(
        access_key="titan",
        secret_key="titan_dev_password",
        endpoint_override="localhost:19000",
        scheme="http",
    )
    table = ds.dataset(path, filesystem=filesystem, format="parquet").to_table(
        columns=["kafka_topic", "kafka_partition", "kafka_offset", "parse_status"]
    )
    keys = {
        (row["kafka_topic"], row["kafka_partition"], row["kafka_offset"])
        for row in table.to_pylist()
    }
    invalid = sum(1 for row in table.to_pylist() if row["parse_status"] == "invalid")
    return {
        "prefix": prefix,
        "rows": table.num_rows,
        "distinct_offsets": len(keys),
        "invalid": invalid,
    }


def _currency_revenue() -> dict[str, str]:
    rows = _psql(
        "select event_envelope->'payload'->>'currency', "
        "sum((event_envelope->'payload'->>'total_amount')::numeric) "
        "from commerce_events where event_type = 'order_created' and disposition = 'accepted' "
        "group by 1 order by 1"
    )
    revenue: dict[str, str] = {}
    for line in rows.splitlines():
        if not line.strip():
            continue
        currency, amount = line.split("|")
        revenue[currency] = format(Decimal(amount).quantize(Decimal("0.01")), "f")
    return revenue


def _count_only(rows: str) -> str:
    kept = []
    for line in rows.splitlines():
        parts = line.split("|")
        if len(parts) >= 3:
            kept.append("|".join(parts[:3]))
    return "\n".join(kept)


def _checkpoint_state() -> dict[str, Any]:
    code = (
        "import json, os\n"
        "root='/var/lib/titan/spark-checkpoints'\n"
        "bronze=os.path.join(root,'bronze')\n"
        "def names(path):\n"
        "    return sorted(os.listdir(path)) if os.path.isdir(path) else None\n"
        "marker=None\n"
        "mp=os.path.join(root,'kill-marker')\n"
        "if os.path.isfile(mp):\n"
        "    marker=open(mp, encoding='utf-8').read()\n"
        "print(json.dumps({'marker': marker, 'offsets': names(os.path.join(bronze,'offsets')), "
        "'commits': names(os.path.join(bronze,'commits'))}))\n"
    )
    completed = _compose_raw(
        "--profile",
        "processing",
        "run",
        "--rm",
        "--no-deps",
        "--entrypoint",
        "python3",
        "spark-bronze",
        "-c",
        code,
    )
    if completed.returncode != 0:
        return {"error": (completed.stdout + completed.stderr)[-800:]}
    line = completed.stdout.strip().splitlines()[-1]
    parsed = json.loads(line)
    if not isinstance(parsed, dict):
        raise RuntimeError("checkpoint probe was not an object")
    return parsed


def _assert_isolated() -> None:
    mounts = subprocess.run(
        [
            "docker",
            "inspect",
            "titan-sparkkill-postgres-1",
            "--format",
            "{{range .Mounts}}{{.Name}} {{end}}",
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    if "titan-sparkkill_postgres_data" not in mounts:
        raise SystemExit(f"postgres is not on the sparkkill volume: {mounts!r}")
    if "titan_postgres_data" in mounts or "titan-e2e_postgres_data" in mounts:
        raise SystemExit(f"postgres mounts another project's volume: {mounts!r}")


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
            "titan-sparkkill-postgres-1",
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


def _module(module: str) -> str:
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(
        [
            str(ROOT / "pipelines" / "warehouse" / "src"),
            str(ROOT / "pipelines" / "quality" / "src"),
            str(ROOT / "pipelines" / "ml" / "src"),
            env.get("PYTHONPATH", ""),
        ]
    )
    completed = _run(
        ["uv", "run", "--group", "orchestration", "python", "-m", module],
        timeout=1200,
        env=env,
    )
    if completed.returncode != 0:
        raise SystemExit((completed.stdout + completed.stderr)[-4000:])
    return (completed.stdout + completed.stderr)[-2000:]


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


def _post(payload: dict[str, Any]) -> None:
    request = urllib.request.Request(
        API + "/api/v1/ingestion/events",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    _read(request)


def _get(path: str) -> dict[str, Any]:
    body = _read(urllib.request.Request(API + path))
    if not isinstance(body, dict):
        raise RuntimeError("response was not an object")
    return body


def _read(request: urllib.request.Request) -> dict[str, Any]:
    with urllib.request.urlopen(request, timeout=30) as response:
        body = json.loads(response.read().decode())
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
    raise SystemExit("sparkkill API did not become ready")


def _wait_event(event_id: str) -> None:
    deadline = time.monotonic() + 40
    while time.monotonic() < deadline:
        recent = _get("/api/v1/events/recent?limit=100")
        if any(item["event_id"] == event_id for item in recent["items"]):
            return
        time.sleep(0.5)
    raise SystemExit(f"event {event_id} was not persisted")


def _wait_healthy(container: str) -> None:
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        completed = subprocess.run(
            ["docker", "inspect", "--format", "{{.State.Health.Status}}", container],
            check=False,
            capture_output=True,
            text=True,
        )
        if completed.stdout.strip() == "healthy":
            return
        time.sleep(2)
    raise SystemExit(f"{container} did not become healthy")


if __name__ == "__main__":
    main()
