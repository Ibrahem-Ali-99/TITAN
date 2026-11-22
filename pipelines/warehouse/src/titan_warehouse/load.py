"""Copy one published Silver version into ClickHouse and promote it after checks.

PostgreSQL records the attempt. ClickHouse `wh_active` is replaced only after
the candidate rows validate and dbt tests pass. Readers of the serving views
follow `wh_active`, so a failed load leaves the previous publication in place.
"""

import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import uuid4

import clickhouse_connect
import psycopg
import pyarrow.fs as pafs
import pyarrow.parquet as pq

from titan_warehouse.policy import can_promote, recorded_run

WAREHOUSE_LOCK_KEY = 740002
ROOT = Path(__file__).resolve().parents[4]
DBT_DIR = ROOT / "warehouse" / "dbt"
SQL_PATH = ROOT / "warehouse" / "clickhouse" / "001_tables.sql"


class WarehouseError(RuntimeError):
    pass


class StalePublication(WarehouseError):
    pass


def run_warehouse_pipeline(
    crash_after: str | None = None,
    silver_publication_id: str | None = None,
    stop_after: str | None = None,
) -> str:
    database_url = _database_url()
    with _lock(database_url):
        silver = _silver(database_url, silver_publication_id)
        latest = _silver(database_url, None)
        active = _active_warehouse(database_url)
        if not can_promote(silver["finished_at"], latest["finished_at"]):
            raise StalePublication(
                f"silver {silver['publication_id']} is older than the current Silver snapshot"
            )
        active_finished = None if active is None else active["silver_finished_at"]
        if not can_promote(silver["finished_at"], active_finished):
            raise StalePublication(
                f"silver {silver['publication_id']} is older than the published warehouse snapshot"
            )
        load_id = str(uuid4())
        warehouse_id = str(uuid4())
        _insert_load(database_url, load_id, silver, warehouse_id, "staging")
        _record("warehouse_load", "running", f"staging {warehouse_id}")
        started_load = datetime.now(UTC)
        try:
            client = _clickhouse()
            _ensure_tables(client)
            frames = _read_silver(silver["version_uri"])
            _insert_frames(client, warehouse_id, frames)
            if crash_after == "inserted":
                raise WarehouseError("crash after the ClickHouse insert and before publication")
            summary = _validate(client, warehouse_id, frames)
            _mark(database_url, load_id, "validated", summary)
            if stop_after == "validated":
                _record("clickhouse_load", "succeeded", load_id, metadata=summary)
                return load_id
            _run_dbt(warehouse_id)
            _mark(database_url, load_id, "tested", summary)
            if crash_after == "tested" or stop_after == "tested":
                if crash_after == "tested":
                    raise WarehouseError("crash after dbt tests and before publication")
                return load_id
            latest = _silver(database_url, None)
            active = _active_warehouse(database_url)
            if not can_promote(silver["finished_at"], latest["finished_at"]) or not can_promote(
                silver["finished_at"], None if active is None else active["silver_finished_at"]
            ):
                raise StalePublication("a newer Silver snapshot exists")
            started = datetime.now(UTC)
            try:
                from titan_quality.evaluate import QualityGateError, gate_load

                gate_load(
                    database_url,
                    client,
                    {
                        "warehouse_publication_id": warehouse_id,
                        "silver_publication_id": silver["publication_id"],
                        "revenue_json": summary["revenue_json"],
                    },
                )
            except QualityGateError as exc:
                _record(
                    "quality_gate",
                    "failed",
                    load_id,
                    error=str(exc)[:2000],
                    started_at=started,
                )
                raise
            _record("quality_gate", "succeeded", load_id, started_at=started)
            _promote(client, warehouse_id, silver, summary)
            if crash_after == "exchanged":
                raise WarehouseError(
                    "crash after the ClickHouse pointer exchange and before the PostgreSQL row"
                )
            _mark(database_url, load_id, "published", summary)
            _record(
                "warehouse_load",
                "succeeded",
                warehouse_id,
                metadata=summary,
                started_at=started_load,
            )
            return warehouse_id
        except Exception as exc:
            _mark(database_url, load_id, "failed", error=str(exc)[:2000])
            _record(
                "warehouse_load",
                "failed",
                warehouse_id,
                error=str(exc)[:2000],
                started_at=started_load,
            )
            raise


def _database_url() -> str:
    url = os.environ.get(
        "DATABASE_URL", "postgresql://titan:titan_dev_password@localhost:5435/titan"
    )
    return url.replace("postgresql+psycopg://", "postgresql://", 1)


def _lock(database_url: str):
    class _Lock:
        def __enter__(self):
            self.connection = psycopg.connect(database_url)
            self.connection.execute("select pg_advisory_lock(%s)", (WAREHOUSE_LOCK_KEY,))
            return self

        def __exit__(self, *_exc: object) -> None:
            try:
                self.connection.execute("select pg_advisory_unlock(%s)", (WAREHOUSE_LOCK_KEY,))
            finally:
                self.connection.close()

    return _Lock()


def _silver(database_url: str, publication_id: str | None) -> dict[str, object]:
    query = """
        select publication_id, version_uri, finished_at
        from lake_publications
        where status = 'published' and finished_at is not null
    """
    params: tuple[object, ...] = ()
    if publication_id is None:
        query += " order by finished_at desc limit 1"
    else:
        query += " and publication_id = %s"
        params = (publication_id,)
    with psycopg.connect(database_url) as connection:
        row = connection.execute(query, params).fetchone()
    if row is None or row[2] is None:
        raise WarehouseError("no published Silver version is available")
    return {"publication_id": str(row[0]), "version_uri": str(row[1]), "finished_at": row[2]}


def _active_warehouse(database_url: str) -> dict[str, object] | None:
    with psycopg.connect(database_url) as connection:
        row = connection.execute(
            """
            select warehouse_publication_id, silver_publication_id, silver_finished_at
            from warehouse_loads
            where status = 'published' and finished_at is not null
            order by finished_at desc
            limit 1
            """
        ).fetchone()
    if row is None:
        return None
    return {
        "warehouse_publication_id": str(row[0]),
        "silver_publication_id": str(row[1]),
        "silver_finished_at": row[2],
    }


def _insert_load(
    database_url: str,
    load_id: str,
    silver: dict[str, object],
    warehouse_id: str,
    status: str,
) -> None:
    now = datetime.now(UTC)
    with psycopg.connect(database_url) as connection:
        connection.execute(
            """
            insert into warehouse_loads (
                load_id, silver_publication_id, silver_finished_at, warehouse_publication_id,
                status, started_at
            ) values (%s, %s, %s, %s, %s, %s)
            """,
            (load_id, silver["publication_id"], silver["finished_at"], warehouse_id, status, now),
        )
        connection.commit()


def _mark(
    database_url: str,
    load_id: str,
    status: str,
    summary: dict[str, object] | None = None,
    error: str | None = None,
) -> None:
    summary = summary or {}
    with psycopg.connect(database_url) as connection:
        connection.execute(
            """
            update warehouse_loads
            set status = %s,
                finished_at = %s,
                order_count = %s,
                payment_count = %s,
                inventory_count = %s,
                item_count = %s,
                accepted_revenue_json = %s,
                error_detail = %s
            where load_id = %s
            """,
            (
                status,
                datetime.now(UTC),
                summary.get("order_count"),
                summary.get("payment_count"),
                summary.get("inventory_count"),
                summary.get("item_count"),
                summary.get("revenue_json"),
                error,
                load_id,
            ),
        )
        connection.commit()


def _record(
    job_name: str,
    state: str,
    detail: str,
    error: str | None = None,
    metadata: dict[str, object] | None = None,
    started_at: datetime | None = None,
) -> None:
    started, finished, duration_ms = recorded_run(state, datetime.now(UTC), started_at)
    with psycopg.connect(_database_url()) as connection:
        connection.execute(
            """
            insert into pipeline_runs (
                job_name, state, started_at, finished_at, duration_ms,
                detail, error_detail, metadata_json
            ) values (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                job_name,
                state,
                started,
                finished,
                duration_ms,
                detail,
                error,
                None if metadata is None else json.dumps(metadata),
            ),
        )
        connection.commit()


def _clickhouse():
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "titan"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", "titan_dev_password"),
        database=os.environ.get("CLICKHOUSE_DATABASE", "titan"),
        send_receive_timeout=30,
    )


def _ensure_tables(client) -> None:
    script = SQL_PATH.read_text(encoding="utf-8")
    for statement in script.split(";"):
        if statement.strip():
            client.command(statement)


def _s3():
    endpoint = os.environ.get("S3_ENDPOINT", "http://localhost:9000")
    secure = endpoint.startswith("https://")
    host = endpoint.removeprefix("https://").removeprefix("http://")
    return pafs.S3FileSystem(
        access_key=os.environ.get("S3_ACCESS_KEY", os.environ.get("MINIO_ROOT_USER", "titan")),
        secret_key=os.environ.get(
            "S3_SECRET_KEY",
            os.environ.get("MINIO_ROOT_PASSWORD", "titan_dev_password"),
        ),
        endpoint_override=host,
        scheme="https" if secure else "http",
    )


def _read_silver(version_uri: str) -> dict[str, list[dict[str, object]]]:
    filesystem = _s3()
    root = version_uri.replace("s3a://", "").replace("s3://", "").rstrip("/")
    frames = {}
    for name in ("orders", "order_items", "payments", "inventory"):
        path = f"{root}/{name}"
        info = filesystem.get_file_info(path)
        if info.type == pafs.FileType.NotFound:
            raise WarehouseError(f"published Silver is missing {name}")
        frames[name] = pq.read_table(path, filesystem=filesystem).to_pylist()
    if frames["orders"] and not frames["order_items"]:
        raise WarehouseError("published orders have no line items")
    return frames


def _insert_frames(client, warehouse_id: str, frames: dict[str, list[dict[str, object]]]) -> None:
    _insert(
        client,
        "wh_orders",
        frames["orders"],
        warehouse_id,
        [
            "event_id",
            "order_id",
            "customer_id",
            "currency",
            "total_amount",
            "occurred_at",
            "ingested_at",
            "source",
        ],
    )
    _insert(
        client,
        "wh_order_items",
        frames["order_items"],
        warehouse_id,
        [
            "event_id",
            "order_id",
            "customer_id",
            "currency",
            "line_number",
            "product_id",
            "quantity",
            "unit_price",
            "line_amount",
            "occurred_at",
        ],
    )
    _insert(
        client,
        "wh_payments",
        frames["payments"],
        warehouse_id,
        [
            "event_id",
            "payment_id",
            "order_id",
            "customer_id",
            "currency",
            "amount",
            "method",
            "order_matched",
            "occurred_at",
        ],
    )
    _insert(
        client,
        "wh_inventory",
        frames["inventory"],
        warehouse_id,
        [
            "event_id",
            "product_id",
            "warehouse_id",
            "quantity_on_hand",
            "inventory_delta",
            "order_id",
            "occurred_at",
        ],
    )


def _insert(
    client,
    table: str,
    rows: list[dict[str, object]],
    warehouse_id: str,
    columns: list[str],
) -> None:
    if not rows:
        return
    payload = []
    for row in rows:
        values = [_cell(row, column) for column in columns]
        payload.append([warehouse_id, *values])
    client.insert(table, payload, column_names=["warehouse_publication_id", *columns])


def _cell(row: dict[str, object], column: str):
    value = row.get(column)
    if column == "order_matched":
        return 1 if value else 0
    if isinstance(value, datetime):
        # Parquet timestamps are UTC wall times without a tzinfo. Attaching UTC
        # keeps ClickHouse from treating them as the host local zone.
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    return value


def _validate(
    client,
    warehouse_id: str,
    frames: dict[str, list[dict[str, object]]],
) -> dict[str, object]:
    loaded = _money(client, warehouse_id)
    expected = _money_from_rows(frames["orders"])
    if loaded != expected:
        raise WarehouseError(f"loaded revenue {loaded} does not match Silver {expected}")
    counts = {
        "orders": _count(client, "wh_orders", warehouse_id),
        "order_items": _count(client, "wh_order_items", warehouse_id),
        "payments": _count(client, "wh_payments", warehouse_id),
        "inventory": _count(client, "wh_inventory", warehouse_id),
    }
    if counts["orders"] != len(frames["orders"]) or counts["payments"] != len(frames["payments"]):
        raise WarehouseError(f"loaded counts {counts} do not match the Silver files")
    item_count = counts["order_items"] != len(frames["order_items"])
    if counts["inventory"] != len(frames["inventory"]) or item_count:
        raise WarehouseError(f"loaded counts {counts} do not match the Silver files")
    if _duplicates(client, warehouse_id):
        raise WarehouseError("loaded orders contain duplicate event_id values")
    line_totals = _line_totals(frames["order_items"])
    for order in frames["orders"]:
        key = (str(order["order_id"]), str(order["currency"]))
        if line_totals.get(key) != Decimal(str(order["total_amount"])):
            raise WarehouseError(f"order {key[0]} total does not match its line items")
    return {
        "order_count": counts["orders"],
        "payment_count": counts["payments"],
        "inventory_count": counts["inventory"],
        "item_count": counts["order_items"],
        "revenue_json": json.dumps(expected),
    }


def _count(client, table: str, warehouse_id: str) -> int:
    result = client.query(
        f"select count() from {table} where warehouse_publication_id = {{pub:String}}",
        parameters={"pub": warehouse_id},
    )
    return int(result.result_rows[0][0])


def _duplicates(client, warehouse_id: str) -> bool:
    result = client.query(
        """
        select count() from (
            select event_id from wh_orders
            where warehouse_publication_id = {pub:String}
            group by event_id
            having count() > 1
        )
        """,
        parameters={"pub": warehouse_id},
    )
    return int(result.result_rows[0][0]) > 0


def _money(client, warehouse_id: str) -> dict[str, str]:
    result = client.query(
        """
        select currency, sum(total_amount)
        from wh_orders
        where warehouse_publication_id = {pub:String}
        group by currency
        order by currency
        """,
        parameters={"pub": warehouse_id},
    )
    return {str(row[0]): _plain(row[1]) for row in result.result_rows}


def _money_from_rows(rows: list[dict[str, object]]) -> dict[str, str]:
    totals: dict[str, Decimal] = {}
    for row in rows:
        currency = str(row["currency"])
        totals[currency] = totals.get(currency, Decimal("0")) + Decimal(str(row["total_amount"]))
    return {currency: _plain(amount) for currency, amount in sorted(totals.items())}


def _line_totals(rows: list[dict[str, object]]) -> dict[tuple[str, str], Decimal]:
    totals: dict[tuple[str, str], Decimal] = {}
    for row in rows:
        key = (str(row["order_id"]), str(row["currency"]))
        totals[key] = totals.get(key, Decimal("0")) + Decimal(str(row["line_amount"]))
    return totals


def _plain(amount: object) -> str:
    return format(Decimal(str(amount)).quantize(Decimal("0.01")), "f")


def _run_dbt(warehouse_id: str) -> None:
    variables = json.dumps({"warehouse_publication_id": warehouse_id})
    _dbt(["build", "--select", "path:models/serving"])
    _dbt(["build", "--select", "path:models/candidate", "--vars", variables])
    _dbt(
        [
            "test",
            "--select",
            "cand_orders",
            "cand_order_items",
            "cand_payments",
            "cand_revenue_hour",
            "revenue_matches_lines",
            "payment_order_links",
            "valid_quantities",
            "promotion_gate",
            "--vars",
            variables,
        ]
    )


def _dbt(args: list[str]) -> None:
    completed = subprocess.run(
        [
            "dbt",
            *args,
            "--no-partial-parse",
            "--project-dir",
            str(DBT_DIR),
            "--profiles-dir",
            str(DBT_DIR),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        tail = (completed.stdout + completed.stderr)[-4000:]
        raise WarehouseError(f"dbt {' '.join(args)} failed: {tail}")


def _promote(
    client,
    warehouse_id: str,
    silver: dict[str, object],
    summary: dict[str, object],
) -> None:
    published_at = datetime.now(UTC).replace(tzinfo=None)
    client.command("drop table if exists wh_active_stage")
    client.command("create table wh_active_stage as wh_active")
    client.command("truncate table wh_active_stage")
    client.insert(
        "wh_active_stage",
        [
            [
                warehouse_id,
                silver["publication_id"],
                published_at,
                int(summary["order_count"]),
                int(summary["payment_count"]),
                int(summary["inventory_count"]),
                int(summary["item_count"]),
                str(summary["revenue_json"]),
            ]
        ],
        column_names=[
            "warehouse_publication_id",
            "silver_publication_id",
            "published_at",
            "order_count",
            "payment_count",
            "inventory_count",
            "item_count",
            "revenue_json",
        ],
    )
    client.command("exchange tables wh_active and wh_active_stage")
    client.command("drop table if exists wh_active_stage")


def test_load(load_id: str) -> str:
    database_url = _database_url()
    with _lock(database_url):
        row = _load_row(database_url, load_id)
        if row["status"] != "validated":
            raise WarehouseError(f"load {load_id} is {row['status']}, not validated")
        started = datetime.now(UTC)
        try:
            _run_dbt(str(row["warehouse_publication_id"]))
        except Exception as exc:
            _record(
                "warehouse_models",
                "failed",
                load_id,
                error=str(exc)[:2000],
                started_at=started,
            )
            raise
        _mark(database_url, load_id, "tested", _summary_from_row(row))
        _record("warehouse_models", "succeeded", load_id, started_at=started)
        return load_id


def run_quality_gate(load_id: str) -> str:
    """Evaluate a tested load without promoting it."""
    database_url = _database_url()
    row = _load_row(database_url, load_id)
    if row["status"] not in {"tested", "published"}:
        raise WarehouseError(f"load {load_id} is {row['status']}, not tested")
    client = _clickhouse()
    try:
        from titan_quality.evaluate import gate_load

        return gate_load(database_url, client, row)
    finally:
        client.close()


def publish_load(load_id: str) -> str:
    database_url = _database_url()
    with _lock(database_url):
        row = _load_row(database_url, load_id)
        if row["status"] == "published":
            return str(row["warehouse_publication_id"])
        if row["status"] != "tested":
            raise WarehouseError(f"load {load_id} is {row['status']}, not tested")
        latest = _silver(database_url, None)
        active = _active_warehouse(database_url)
        if not can_promote(row["silver_finished_at"], latest["finished_at"]):
            raise StalePublication(f"load {load_id} is older than the current Silver snapshot")
        active_finished = None if active is None else active["silver_finished_at"]
        if not can_promote(row["silver_finished_at"], active_finished):
            raise StalePublication(f"load {load_id} is older than the published snapshot")
        client = _clickhouse()
        summary = _summary_from_row(row)
        started = datetime.now(UTC)
        try:
            from titan_quality.evaluate import QualityGateError, gate_load

            gate_load(database_url, client, row)
        except QualityGateError as exc:
            _mark(database_url, load_id, "failed", summary, error=str(exc)[:2000])
            _record("quality_gate", "failed", load_id, error=str(exc)[:2000], started_at=started)
            raise WarehouseError(str(exc)) from exc
        _record("quality_gate", "succeeded", load_id, started_at=started)
        _promote(
            client,
            str(row["warehouse_publication_id"]),
            {
                "publication_id": row["silver_publication_id"],
                "finished_at": row["silver_finished_at"],
            },
            summary,
        )
        _mark(database_url, load_id, "published", summary)
        _record(
            "analytics_ready",
            "succeeded",
            str(row["warehouse_publication_id"]),
            metadata=summary,
            started_at=started,
        )
        return str(row["warehouse_publication_id"])


def _load_row(database_url: str, load_id: str) -> dict[str, object]:
    with psycopg.connect(database_url) as connection:
        row = connection.execute(
            """
            select status, warehouse_publication_id, silver_publication_id, silver_finished_at,
                   order_count, payment_count, inventory_count, item_count, accepted_revenue_json
            from warehouse_loads
            where load_id = %s
            """,
            (load_id,),
        ).fetchone()
    if row is None:
        raise WarehouseError(f"unknown warehouse load {load_id}")
    return {
        "status": str(row[0]),
        "warehouse_publication_id": str(row[1]),
        "silver_publication_id": str(row[2]),
        "silver_finished_at": row[3],
        "order_count": row[4],
        "payment_count": row[5],
        "inventory_count": row[6],
        "item_count": row[7],
        "revenue_json": row[8],
    }


def _summary_from_row(row: dict[str, object]) -> dict[str, object]:
    return {
        "order_count": row["order_count"],
        "payment_count": row["payment_count"],
        "inventory_count": row["inventory_count"],
        "item_count": row["item_count"],
        "revenue_json": row["revenue_json"],
    }


def reconcile_warehouse() -> str:
    """Point wh_active at the newest verified publication, or leave it."""
    from titan_warehouse.recovery import decide_recovery

    database_url = _database_url()
    with _lock(database_url):
        client = _clickhouse()
        try:
            pointer_rows = client.query(
                "select warehouse_publication_id from wh_active limit 1"
            ).result_rows
            pointer = None if not pointer_rows else str(pointer_rows[0][0])
            evidence = _recovery_evidence(database_url, client)
        finally:
            client.close()
        published = _published_loads(database_url)
        decision = decide_recovery(pointer, evidence)
        started = datetime.now(UTC)
        try:
            if decision.action == "exchange" and decision.publication_id is not None:
                chosen = next(item for item in published if item["id"] == decision.publication_id)
                client = _clickhouse()
                try:
                    _promote(
                        client,
                        chosen["id"],
                        {
                            "publication_id": chosen["silver_publication_id"],
                            "finished_at": chosen["silver_finished_at"],
                        },
                        {
                            "order_count": chosen["order_count"],
                            "payment_count": chosen["payment_count"],
                            "inventory_count": chosen["inventory_count"],
                            "item_count": chosen["item_count"],
                            "revenue_json": chosen["revenue_json"],
                        },
                    )
                finally:
                    client.close()
        except Exception as exc:
            _record(
                "warehouse_reconcile",
                "failed",
                decision.reason,
                error=str(exc)[:2000],
                metadata={
                    "action": decision.action,
                    "publication_id": decision.publication_id,
                    "previous_pointer": pointer,
                },
                started_at=started,
            )
            raise
        _record(
            "warehouse_reconcile",
            "failed" if decision.action == "refuse" else "succeeded",
            decision.reason,
            metadata={
                "action": decision.action,
                "publication_id": decision.publication_id,
                "previous_pointer": pointer,
            },
            started_at=started,
        )
        return decision.reason


def _recovery_evidence(database_url: str, client: Any) -> list[Any]:
    from titan_warehouse.recovery import PublicationEvidence

    published = _published_loads(database_url)
    with psycopg.connect(database_url) as connection:
        dbt_ids = {
            str(row[0])
            for row in connection.execute(
                """
                select detail from pipeline_runs
                where job_name = 'warehouse_models' and state = 'succeeded' and detail is not null
                """
            ).fetchall()
        }
        quality_rows = connection.execute(
            """
            select warehouse_publication_id, status
            from quality_runs
            order by started_at desc
            """
        ).fetchall()
        silver_ids = {
            str(row[0])
            for row in connection.execute(
                "select publication_id from lake_publications where status = 'published'"
            ).fetchall()
        }
    latest_quality: dict[str, str] = {}
    for publication_id, status in quality_rows:
        key = str(publication_id)
        if key not in latest_quality:
            latest_quality[key] = str(status)
    counts = _clickhouse_counts(client)
    evidence = []
    for item in published:
        publication_id = str(item["id"])
        observed = counts.get(publication_id, {})
        evidence.append(
            PublicationEvidence(
                publication_id=publication_id,
                published=True,
                dbt_passed=str(item["load_id"]) in dbt_ids,
                quality_passed=latest_quality.get(publication_id) == "passed",
                silver_published=str(item["silver_publication_id"]) in silver_ids,
                order_count=_stored_count(item["order_count"]),
                payment_count=_stored_count(item["payment_count"]),
                inventory_count=_stored_count(item["inventory_count"]),
                item_count=_stored_count(item["item_count"]),
                clickhouse_orders=int(observed.get("orders", -1)),
                clickhouse_payments=int(observed.get("payments", -1)),
                clickhouse_inventory=int(observed.get("inventory", -1)),
                clickhouse_items=int(observed.get("items", -1)),
            )
        )
    return evidence


def _stored_count(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        return -1
    return value


def _clickhouse_counts(client: Any) -> dict[str, dict[str, int]]:
    query = client.query
    tables = {
        "orders": "wh_orders",
        "payments": "wh_payments",
        "inventory": "wh_inventory",
        "items": "wh_order_items",
    }
    found: dict[str, dict[str, int]] = {}
    for name, table in tables.items():
        result = query(f"select warehouse_publication_id, count() from {table} group by 1")
        for row in result.result_rows:
            bucket = found.setdefault(str(row[0]), {})
            bucket[name] = int(row[1])
    return found


def _published_loads(database_url: str) -> list[dict[str, object]]:
    with psycopg.connect(database_url) as connection:
        rows = connection.execute(
            """
            select load_id, warehouse_publication_id, silver_publication_id, silver_finished_at,
                   order_count, payment_count, inventory_count, item_count, accepted_revenue_json
            from warehouse_loads
            where status = 'published' and finished_at is not null
            order by finished_at desc
            """
        ).fetchall()
    return [
        {
            "load_id": str(row[0]),
            "id": str(row[1]),
            "silver_publication_id": str(row[2]),
            "silver_finished_at": row[3],
            "order_count": row[4],
            "payment_count": row[5],
            "inventory_count": row[6],
            "item_count": row[7],
            "revenue_json": row[8],
        }
        for row in rows
    ]


def main() -> None:
    if "--reconcile" in sys.argv:
        print(reconcile_warehouse())
        return
    crash_after = None
    silver_id = None
    if "--crash-after" in sys.argv:
        crash_after = sys.argv[sys.argv.index("--crash-after") + 1]
    if "--silver" in sys.argv:
        silver_id = sys.argv[sys.argv.index("--silver") + 1]
    print(run_warehouse_pipeline(crash_after=crash_after, silver_publication_id=silver_id))


if __name__ == "__main__":
    main()
