"""Reconcile the disposable Spark, lake, and serving outputs."""

from __future__ import annotations

import json
import os
from pathlib import Path
from urllib.parse import urlparse

import clickhouse_connect
import psycopg
from minio import Minio


def main() -> None:
    database_url = os.environ["DATABASE_URL"]
    with psycopg.connect(database_url) as connection:
        row = connection.execute(
            """
            select status, order_count, payment_count, inventory_count, item_count,
                   accepted_revenue_json
            from warehouse_loads
            where status = 'published'
            order by finished_at desc
            limit 1
            """
        ).fetchone()
        if row is None:
            raise AssertionError("no published warehouse load")
        failed_row = connection.execute(
            "select count(*) from warehouse_loads where status = 'failed'"
        ).fetchone()
        if failed_row is None:
            raise AssertionError("failed-load count query returned no row")
        failed = int(failed_row[0])
    client = clickhouse_connect.get_client(
        host=os.environ["CLICKHOUSE_HOST"],
        port=int(os.environ["CLICKHOUSE_PORT"]),
        username=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
        database=os.environ["CLICKHOUSE_DATABASE"],
    )
    active = client.query(
        """
        select warehouse_publication_id, order_count, payment_count,
               inventory_count, item_count, revenue_json
        from wh_active
        """
    ).result_rows
    if len(active) != 1:
        raise AssertionError(f"expected one active serving pointer, found {len(active)}")
    counts = client.query(
        """
        select count(), countDistinct(event_id)
        from wh_orders
        where warehouse_publication_id = {pub:String}
        """,
        parameters={"pub": str(active[0][0])},
    ).result_rows[0]
    revenue = client.query(
        """
        select currency, sum(total_amount)
        from wh_orders
        where warehouse_publication_id = {pub:String}
        group by currency
        order by currency
        """,
        parameters={"pub": str(active[0][0])},
    ).result_rows
    endpoint = urlparse(os.environ["S3_ENDPOINT"])
    lake = Minio(
        endpoint.netloc,
        access_key=os.environ["S3_ACCESS_KEY"],
        secret_key=os.environ["S3_SECRET_KEY"],
        secure=endpoint.scheme == "https",
    )
    parquet_objects = [
        item.object_name
        for item in lake.list_objects(
            os.environ["LAKE_BUCKET"], prefix="silver/versions/", recursive=True
        )
        if item.object_name.endswith(".parquet")
    ]
    expected = json.loads(Path("benchmarks/full_pipeline_ci.json").read_text(encoding="utf-8"))
    actual_revenue = {str(row[0]): format(row[1], ".2f") for row in revenue}
    if actual_revenue != expected["expected_order_revenue_by_currency"]:
        raise AssertionError(f"revenue mismatch: {actual_revenue}")
    if int(counts[0]) != int(counts[1]) or int(active[0][1]) != int(counts[0]):
        raise AssertionError("serving order counts are not unique and reconciled")
    report = {
        **expected,
        "published_load": {
            "status": row[0],
            "order_count": row[1],
            "payment_count": row[2],
            "inventory_count": row[3],
            "item_count": row[4],
            "revenue_json": row[5],
        },
        "failed_loads_before_recovery": failed,
        "active_pointer": active[0][0],
        "active_counts": list(active[0][1:5]),
        "unique_clickhouse_orders": int(counts[1]),
        "clickhouse_revenue_by_currency": actual_revenue,
        "minio_silver_parquet_objects": len(parquet_objects),
    }
    Path("benchmarks/full_pipeline_ci.json").write_text(
        json.dumps(report, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
