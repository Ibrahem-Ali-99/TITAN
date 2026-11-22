"""Print one JSON object describing Bronze files and the current Silver version.

Counts are read when this command runs. The API does not call it on each poll.
"""

import json

from pyspark.sql import functions as F

from titan_spark.config import load_settings
from titan_spark.metadata import _connect
from titan_spark.session import build_session


def main() -> None:
    settings = load_settings()
    spark = build_session(settings, "titan-lake-report")
    bronze = _read_optional(spark, settings.bronze_uri)
    publications = _publications(settings)
    current = next((row for row in publications if row["status"] == "published"), None)
    silver_orders = None
    silver_revenue: dict[str, str] = {}
    if current is not None:
        orders = spark.read.parquet(f"{current['version_uri']}/orders")
        silver_orders = orders.count()
        rows = orders.groupBy("currency").agg(F.sum("total_amount").alias("amount")).collect()
        silver_revenue = {
            str(row["currency"]): format(row["amount"], "f")
            for row in rows
            if row["currency"] is not None and row["amount"] is not None
        }
    distinct = 0
    raw_rows = 0
    invalid_rows = 0
    if bronze is not None:
        raw_rows = bronze.count()
        distinct = (
            bronze.select("kafka_topic", "kafka_partition", "kafka_offset").distinct().count()
        )
        invalid_rows = bronze.filter(F.col("parse_status") == F.lit("invalid")).count()
    report = {
        "bronze_rows": raw_rows,
        "bronze_distinct_offsets": distinct,
        "bronze_invalid_rows": invalid_rows,
        "publications": publications,
        "current_publication_id": None if current is None else current["publication_id"],
        "current_silver_orders": silver_orders,
        "current_revenue": silver_revenue,
    }
    print(json.dumps(report))


def _read_optional(spark, uri: str):
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(uri)
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    if not fs.exists(path):
        return None
    return spark.read.parquet(uri)


def _publications(settings) -> list[dict[str, object]]:
    with _connect(settings) as connection:
        cursor = connection.execute(
            """
            select publication_id, status, version_uri, silver_order_count,
                   accepted_revenue_json, pending_event_count
            from lake_publications
            order by started_at desc
            """
        )
        rows = cursor.fetchall()
    return [
        {
            "publication_id": row[0],
            "status": row[1],
            "version_uri": row[2],
            "silver_order_count": row[3],
            "accepted_revenue_json": row[4],
            "pending_event_count": row[5],
        }
        for row in rows
    ]


if __name__ == "__main__":
    main()
