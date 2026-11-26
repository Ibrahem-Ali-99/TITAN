"""Verify ClickHouse recovery by rebuilding from PostgreSQL metadata and MinIO Silver."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import clickhouse_connect
import psycopg

ROOT = Path(__file__).resolve().parents[1]
SQL_PATH = ROOT / "warehouse" / "clickhouse" / "001_tables.sql"


def _db() -> str:
    return os.environ["DATABASE_URL"]


def _ch() -> Any:
    return clickhouse_connect.get_client(
        host=os.environ["CLICKHOUSE_HOST"],
        port=int(os.environ["CLICKHOUSE_PORT"]),
        username=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
        database=os.environ["CLICKHOUSE_DATABASE"],
    )


def _ensure_tables(client: Any) -> None:
    for statement in SQL_PATH.read_text(encoding="utf-8").split(";"):
        if statement.strip():
            client.command(statement)


def _metadata() -> tuple[str, str, dict[str, str], list[int]]:
    with psycopg.connect(_db()) as connection:
        row = connection.execute(
            """
            select warehouse_publication_id, silver_publication_id,
                   accepted_revenue_json, order_count, payment_count,
                   inventory_count, item_count
            from warehouse_loads
            where status = 'published'
            order by finished_at desc limit 1
            """
        ).fetchone()
    if row is None:
        raise RuntimeError("no published warehouse metadata exists")
    return (
        str(row[0]),
        str(row[1]),
        {str(k): str(v) for k, v in json.loads(str(row[2])).items()},
        [int(row[3]), int(row[4]), int(row[5]), int(row[6])],
    )


def _active(client: Any) -> list[Any]:
    rows = client.query(
        "select warehouse_publication_id, silver_publication_id, order_count, "
        "payment_count, inventory_count, item_count, revenue_json from wh_active"
    ).result_rows
    if len(rows) != 1:
        raise RuntimeError(f"expected one active pointer, found {len(rows)}")
    return cast(list[Any], rows[0])


def _active_optional(client: Any) -> list[Any] | None:
    rows = client.query(
        "select warehouse_publication_id, silver_publication_id, order_count, "
        "payment_count, inventory_count, item_count, revenue_json from wh_active"
    ).result_rows
    if len(rows) > 1:
        raise RuntimeError(f"expected at most one active pointer, found {len(rows)}")
    return None if not rows else cast(list[Any], rows[0])


def _facts(client: Any, publication_id: str) -> tuple[list[int], dict[str, str]]:
    counts = []
    for table in ("wh_orders", "wh_payments", "wh_inventory", "wh_order_items"):
        counts.append(
            int(
                client.query(
                    f"select count() from {table} where warehouse_publication_id = {{pub:String}}",
                    parameters={"pub": publication_id},
                ).result_rows[0][0]
            )
        )
    revenue = {
        str(row[0]): format(Decimal(str(row[1])), ".2f")
        for row in client.query(
            "select currency, sum(total_amount) from wh_orders "
            "where warehouse_publication_id = {pub:String} "
            "group by currency order by currency",
            parameters={"pub": publication_id},
        ).result_rows
    }
    return counts, revenue


def _assert_unique_order_keys(client: Any, publication_id: str, expected: int) -> None:
    row = client.query(
        "select count(), uniqExact(order_id) from wh_orders "
        "where warehouse_publication_id = {pub:String}",
        parameters={"pub": publication_id},
    ).result_rows[0]
    if int(row[0]) != expected or int(row[1]) != expected:
        raise RuntimeError(f"order identity reconciliation failed: rows={row[0]} keys={row[1]}")


def _run(*args: str) -> None:
    subprocess.run(
        [sys.executable, "-m", "scripts.run_pipeline", "titan_warehouse", *args],
        cwd=ROOT,
        check=True,
        env=os.environ.copy(),
    )


def main() -> None:
    started = time.perf_counter()
    publication_id, silver_id, expected_revenue, expected_counts = _metadata()
    client = _ch()
    _ensure_tables(client)
    before = _active_optional(client)
    before_pointer = None if before is None else str(before[0])
    try:
        failed = subprocess.run(
            [
                sys.executable,
                "-m",
                "scripts.run_pipeline",
                "titan_warehouse",
                "--crash-after",
                "tested",
            ],
            cwd=ROOT,
            env=os.environ.copy(),
            check=False,
        )
        after_failed = _active_optional(client)
        after_failed_pointer = None if after_failed is None else str(after_failed[0])
        if failed.returncode == 0 or after_failed_pointer != before_pointer:
            raise RuntimeError("interrupted publication changed the active pointer")

        for table in ("wh_orders", "wh_order_items", "wh_payments", "wh_inventory", "wh_active"):
            client.command(f"truncate table {table}")
        if client.query("select count() from wh_active").result_rows[0][0] != 0:
            raise RuntimeError("warehouse pointer was not emptied before recovery")

        _run("--silver", silver_id)
        recovered = _active(client)
        recovered_counts, recovered_revenue = _facts(client, str(recovered[0]))
        _assert_unique_order_keys(client, str(recovered[0]), expected_counts[0])
        if recovered_counts != expected_counts or recovered_revenue != expected_revenue:
            raise RuntimeError(
                "recovery reconciliation failed: "
                f"counts={recovered_counts}, revenue={recovered_revenue}"
            )
        if str(recovered[1]) != silver_id:
            raise RuntimeError("recovered pointer does not preserve Silver lineage")

        recovered_id = str(recovered[0])
        _run("--silver", silver_id)
        rerun_pointer = str(_active(client)[0])
        rerun_counts, rerun_revenue = _facts(client, rerun_pointer)
        _assert_unique_order_keys(client, rerun_pointer, expected_counts[0])
        if rerun_counts != expected_counts or rerun_revenue != expected_revenue:
            raise RuntimeError("repeated recovery changed facts or currency totals")
        rejected = subprocess.run(
            [
                sys.executable,
                "-m",
                "scripts.run_pipeline",
                "titan_warehouse",
                "--silver",
                "00000000-0000-0000-0000-000000000000",
            ],
            cwd=ROOT,
            env=os.environ.copy(),
            check=False,
            capture_output=True,
            text=True,
        )
        if rejected.returncode == 0 or str(_active(client)[0]) != rerun_pointer:
            raise RuntimeError("missing recovery input was accepted or changed the pointer")
        report = {
            "strategy": "rebuild from verified MinIO Silver using PostgreSQL publication metadata",
            "source_publication_id": publication_id,
            "silver_publication_id": silver_id,
            "recovered_publication_id": recovered_id,
            "counts": {
                "orders": recovered_counts[0],
                "payments": recovered_counts[1],
                "inventory": recovered_counts[2],
                "items": recovered_counts[3],
            },
            "revenue_by_currency": recovered_revenue,
            "interrupted_publication_pointer_unchanged": True,
            "recovery_reconciled": True,
            "repeated_recovery_idempotent": True,
            "missing_recovery_input_rejected": True,
            "duration_ms": round((time.perf_counter() - started) * 1000),
        }
        path = ROOT / "benchmarks" / "clickhouse-dr.json"
        path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(report, indent=2))
    finally:
        client.close()


if __name__ == "__main__":
    main()
