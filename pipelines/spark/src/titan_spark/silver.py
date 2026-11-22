"""Silver curation from PostgreSQL dispositions.

Spark does not decide which order was accepted. ``commerce_events.disposition``
does. A Kafka row that is already in Bronze and not yet classified is pending.
It is not rejected and it is not written as an accepted order.

Each run writes a new version directory. The current version is the latest
``lake_publications`` row with status ``published``. A failed validation leaves
that pointer where it was. Copying Parquet to MinIO is not an atomic table swap.
"""

import json
import sys
from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from titan_spark.config import Settings, load_settings
from titan_spark.metadata import (
    SILVER_LOCK_KEY,
    advisory_lock,
    committed_bronze_prefixes,
    finish_publication,
    start_publication,
)
from titan_spark.session import build_session, copy_tree
from titan_spark.topics import INVENTORY_UPDATED, ORDER_CREATED, PAYMENT_COMPLETED
from titan_spark.windows import order_windows

MONEY = DecimalType(18, 2)

ORDER_COLUMNS = [
    "event_id",
    "order_id",
    "customer_id",
    "currency",
    "total_amount",
    "occurred_at",
    "ingested_at",
    "source",
    "disposition",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
]
PAYMENT_COLUMNS = [
    "event_id",
    "payment_id",
    "order_id",
    "customer_id",
    "currency",
    "amount",
    "method",
    "order_matched",
    "occurred_at",
    "ingested_at",
    "source",
    "disposition",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
]
INVENTORY_COLUMNS = [
    "event_id",
    "product_id",
    "warehouse_id",
    "quantity_on_hand",
    "inventory_delta",
    "order_id",
    "occurred_at",
    "ingested_at",
    "source",
    "disposition",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
]

ITEM_SCHEMA = ArrayType(
    StructType(
        [
            StructField("product_id", StringType()),
            StructField("quantity", IntegerType()),
            StructField("unit_price", StringType()),
        ]
    )
)

EVENTS_SQL = """(
    select event_id::text as event_id,
           event_type,
           disposition,
           order_id,
           occurred_at,
           ingested_at,
           source,
           event_envelope::text as event_envelope,
           kafka_topic,
           kafka_partition,
           kafka_offset
    from commerce_events
) events"""


def curate(events: DataFrame) -> dict[str, DataFrame]:
    """Build Silver frames from an authoritative event projection."""
    accepted = events.filter(F.col("disposition") == F.lit("accepted"))
    order_events = accepted.filter(F.col("event_type") == F.lit(ORDER_CREATED))
    orders = _orders(order_events)
    payments = _payments(
        accepted.filter(F.col("event_type") == F.lit(PAYMENT_COMPLETED)),
        orders,
    )
    inventory = _inventory(accepted.filter(F.col("event_type") == F.lit(INVENTORY_UPDATED)))
    return {
        "orders": orders,
        "order_items": _order_items(order_events),
        "payments": payments,
        "inventory": inventory,
    }


def validate_curation(events: DataFrame, curated: dict[str, DataFrame]) -> list[str]:
    errors: list[str] = []
    orders = curated["orders"]
    accepted_orders = events.filter(
        (F.col("event_type") == F.lit(ORDER_CREATED)) & (F.col("disposition") == F.lit("accepted"))
    )
    if orders.count() != accepted_orders.count():
        errors.append("silver order count does not match accepted order_created rows")
    if orders.select("event_id").distinct().count() != orders.count():
        errors.append("silver orders contain duplicate event_id values")
    blocked = events.filter(F.col("disposition").isin("conflict", "duplicate_operation"))
    leaked = orders.join(blocked.select("event_id"), "event_id", "inner").count()
    if leaked:
        errors.append("conflict or duplicate_operation event_id is in silver orders")
    if _money_map(orders, "total_amount") != _source_order_revenue(accepted_orders):
        errors.append("silver revenue does not match accepted order totals")
    null_money = orders.filter(F.col("total_amount").isNull() | F.col("currency").isNull()).count()
    if null_money:
        errors.append("an accepted order is missing currency or total_amount")
    return errors


def pending_event_count(bronze: DataFrame | None, events: DataFrame) -> int:
    """Valid Bronze event ids that PostgreSQL has not classified yet."""
    if bronze is None:
        return 0
    valid = bronze.filter(F.col("parse_status") == F.lit("valid")).select("event_id").distinct()
    classified = events.select("event_id").distinct()
    return valid.join(classified, "event_id", "left_anti").count()


def run_silver(settings: Settings) -> str:
    with advisory_lock(settings, SILVER_LOCK_KEY):
        return _run_silver_locked(settings)


def _run_silver_locked(settings: Settings) -> str:
    spark = build_session(settings, "titan-silver")
    publication_id = str(uuid4())
    version_uri = f"{settings.silver_root}/versions/{publication_id}"
    start_publication(settings, publication_id, version_uri)
    try:
        events = _read_events(spark, settings)
        curated = curate(events)
        metrics = order_windows(curated["orders"], settings.window_duration)
        local = f"file://{settings.checkpoint_root}/silver-staging/{publication_id}"
        _write_local(spark, local, curated, metrics)
        errors = validate_curation(events, _read_local(spark, local))
        if errors:
            finish_publication(
                settings,
                publication_id,
                status="failed",
                error_detail="; ".join(errors),
            )
            return publication_id
        copy_tree(spark, local, version_uri)
        published = _read_version(spark, version_uri)
        errors = validate_curation(events, published)
        if errors:
            finish_publication(
                settings,
                publication_id,
                status="failed",
                error_detail="; ".join(errors),
            )
            return publication_id
        bronze = _read_bronze_paths(spark, committed_bronze_prefixes(settings))
        summary = _summary(events, published, metrics, bronze)
        finish_publication(settings, publication_id, status="published", **summary)
        _write_pointer(spark, settings, publication_id, version_uri)
        return publication_id
    except Exception as exc:
        finish_publication(
            settings,
            publication_id,
            status="failed",
            error_detail=str(exc)[:2000],
        )
        raise


def _orders(accepted_orders: DataFrame) -> DataFrame:
    amount = F.get_json_object("event_envelope", "$.payload.total_amount").cast(MONEY)
    return accepted_orders.select(
        "event_id",
        "order_id",
        F.get_json_object("event_envelope", "$.payload.customer_id").alias("customer_id"),
        F.get_json_object("event_envelope", "$.payload.currency").alias("currency"),
        amount.alias("total_amount"),
        "occurred_at",
        "ingested_at",
        "source",
        "disposition",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
    ).dropDuplicates(["event_id"])


def _payments(accepted_payments: DataFrame, orders: DataFrame) -> DataFrame:
    amount = F.get_json_object("event_envelope", "$.payload.amount").cast(MONEY)
    shaped = accepted_payments.select(
        "event_id",
        F.get_json_object("event_envelope", "$.payload.payment_id").alias("payment_id"),
        "order_id",
        F.get_json_object("event_envelope", "$.payload.customer_id").alias("customer_id"),
        F.get_json_object("event_envelope", "$.payload.currency").alias("currency"),
        amount.alias("amount"),
        F.get_json_object("event_envelope", "$.payload.method").alias("method"),
        "occurred_at",
        "ingested_at",
        "source",
        "disposition",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
    ).dropDuplicates(["event_id"])
    order_customer = orders.select(
        "order_id",
        F.col("customer_id").alias("order_customer_id"),
    ).dropDuplicates(["order_id"])
    joined = shaped.join(order_customer, "order_id", "left")
    contradictory = F.col("order_customer_id").isNotNull() & (
        F.col("customer_id") != F.col("order_customer_id")
    )
    return (
        joined.filter(~contradictory)
        .withColumn("order_matched", F.col("order_customer_id").isNotNull())
        .select(*PAYMENT_COLUMNS)
    )


def _order_items(order_events: DataFrame) -> DataFrame:
    parsed = order_events.withColumn(
        "items",
        F.from_json(F.get_json_object("event_envelope", "$.payload.items"), ITEM_SCHEMA),
    )
    lines = parsed.select(
        "event_id",
        "order_id",
        F.get_json_object("event_envelope", "$.payload.customer_id").alias("customer_id"),
        F.get_json_object("event_envelope", "$.payload.currency").alias("currency"),
        "occurred_at",
        F.posexplode("items").alias("line_number", "item"),
    )
    unit_price = F.col("item.unit_price").cast(MONEY)
    quantity = F.col("item.quantity")
    return lines.select(
        "event_id",
        "order_id",
        "customer_id",
        "currency",
        F.col("line_number").cast(IntegerType()).alias("line_number"),
        F.col("item.product_id").alias("product_id"),
        quantity.alias("quantity"),
        unit_price.alias("unit_price"),
        (quantity.cast(MONEY) * unit_price).alias("line_amount"),
        "occurred_at",
    ).dropDuplicates(["event_id", "line_number"])


def _inventory(accepted_inventory: DataFrame) -> DataFrame:
    return accepted_inventory.select(
        "event_id",
        F.get_json_object("event_envelope", "$.payload.product_id").alias("product_id"),
        F.get_json_object("event_envelope", "$.payload.warehouse_id").alias("warehouse_id"),
        F.get_json_object("event_envelope", "$.payload.quantity_on_hand")
        .cast(IntegerType())
        .alias("quantity_on_hand"),
        F.get_json_object("event_envelope", "$.payload.delta")
        .cast(IntegerType())
        .alias("inventory_delta"),
        F.get_json_object("event_envelope", "$.payload.order_id").alias("order_id"),
        "occurred_at",
        "ingested_at",
        "source",
        "disposition",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
    ).dropDuplicates(["event_id"])


def _read_events(spark: SparkSession, settings: Settings) -> DataFrame:
    return (
        spark.read.format("jdbc")
        .option("url", settings.jdbc_url)
        .option("dbtable", EVENTS_SQL)
        .option("user", settings.jdbc_user)
        .option("password", settings.jdbc_password)
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", "500")
        .load()
    )


def _write_local(
    spark: SparkSession,
    local: str,
    curated: dict[str, DataFrame],
    metrics: DataFrame,
) -> None:
    _write_frame(curated["orders"], f"{local}/orders")
    _write_frame(curated["order_items"], f"{local}/order_items")
    _write_frame(curated["payments"], f"{local}/payments")
    _write_frame(curated["inventory"], f"{local}/inventory")
    _write_frame(metrics, f"{local}/order_metrics")
    del spark


def _write_frame(frame: DataFrame, path: str) -> None:
    frame.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(path)


def _read_local(spark: SparkSession, local: str) -> dict[str, DataFrame]:
    return {
        "orders": spark.read.parquet(f"{local}/orders"),
        "payments": spark.read.parquet(f"{local}/payments"),
        "inventory": spark.read.parquet(f"{local}/inventory"),
    }


def _read_version(spark: SparkSession, version_uri: str) -> dict[str, DataFrame]:
    return _read_local(spark, version_uri)


def _read_bronze_paths(spark: SparkSession, paths: list[str]) -> DataFrame | None:
    if not paths:
        return None
    return spark.read.parquet(*paths)


def _summary(
    events: DataFrame,
    published: dict[str, DataFrame],
    metrics: DataFrame,
    bronze: DataFrame | None,
) -> dict[str, object]:
    orders = published["orders"]
    revenue = _money_map(orders, "total_amount")
    latest = orders.agg(F.max("occurred_at").alias("max_occurred_at")).collect()[0]
    bronze_rows = 0 if bronze is None else bronze.count()
    distinct_offsets = 0
    if bronze is not None:
        distinct_offsets = (
            bronze.select("kafka_topic", "kafka_partition", "kafka_offset").distinct().count()
        )
    return {
        "bronze_record_count": bronze_rows,
        "bronze_distinct_offsets": distinct_offsets,
        "pending_event_count": pending_event_count(bronze, events),
        "silver_order_count": orders.count(),
        "silver_payment_count": published["payments"].count(),
        "silver_inventory_count": published["inventory"].count(),
        "order_metric_rows": metrics.count(),
        "accepted_revenue_json": json.dumps(revenue),
        "identifier_conflict_count": _identifier_conflicts(events, orders),
        "max_occurred_at": latest["max_occurred_at"],
    }


def _identifier_conflicts(events: DataFrame, orders: DataFrame) -> int:
    payments = events.filter(
        (F.col("event_type") == F.lit(PAYMENT_COMPLETED))
        & (F.col("disposition") == F.lit("accepted"))
    )
    shaped = payments.select(
        "event_id",
        "order_id",
        F.get_json_object("event_envelope", "$.payload.customer_id").alias("customer_id"),
    )
    compared = shaped.join(
        orders.select("order_id", F.col("customer_id").alias("order_customer_id")),
        "order_id",
        "left",
    )
    contradictory = F.col("order_customer_id").isNotNull() & (
        F.col("customer_id") != F.col("order_customer_id")
    )
    return compared.filter(contradictory).count()


def _money_map(frame: DataFrame, amount_column: str) -> dict[str, str]:
    rows = frame.groupBy("currency").agg(F.sum(amount_column).alias("amount")).collect()
    result: dict[str, str] = {}
    for row in rows:
        currency = row["currency"]
        amount = row["amount"]
        if currency is None or amount is None:
            continue
        result[str(currency)] = format(Decimal(str(amount)), "f")
    return result


def _source_order_revenue(accepted_orders: DataFrame) -> dict[str, str]:
    amount = F.get_json_object("event_envelope", "$.payload.total_amount").cast(MONEY)
    currency = F.get_json_object("event_envelope", "$.payload.currency")
    framed = accepted_orders.select(currency.alias("currency"), amount.alias("total_amount"))
    return _money_map(framed, "total_amount")


def _write_pointer(
    spark: SparkSession, settings: Settings, publication_id: str, version_uri: str
) -> None:
    pointer = {
        "publication_id": publication_id,
        "version_uri": version_uri,
        "result_kind": "canonical_batch",
        "published_at": datetime.now(timezone.utc).isoformat(),
    }
    frame = spark.createDataFrame(
        [(json.dumps(pointer),)],
        schema="document string",
    )
    local = f"file://{settings.checkpoint_root}/silver-staging/{publication_id}-pointer"
    frame.coalesce(1).write.mode("overwrite").text(local)
    try:
        copy_tree(spark, local, f"{settings.silver_root}/current")
    except Exception as exc:
        print(f"current pointer file was not copied: {exc}", file=sys.stderr)


def main() -> None:
    publication_id = run_silver(load_settings())
    print(publication_id)


if __name__ == "__main__":
    main()
