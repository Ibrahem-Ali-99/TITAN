"""Spark checks for Bronze, Silver, windows, and checkpoint retry."""

import json
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from titan_spark.bronze import write_bronze_batch
from titan_spark.parse import parse_kafka_batch
from titan_spark.silver import curate, pending_event_count, validate_curation
from titan_spark.windows import order_windows, start_preliminary

KAFKA_SCHEMA = (
    "topic string, partition int, offset long, key string, timestamp timestamp, value string"
)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("titan-spark-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_invalid_kafka_value_is_kept(spark: SparkSession) -> None:
    raw = spark.createDataFrame(
        [("titan.commerce.order-events", 0, 4, "ord_badkey01", _at(12), "not-json")],
        KAFKA_SCHEMA,
    )
    parsed = parse_kafka_batch(raw).collect()[0]
    assert parsed["parse_status"] == "invalid"
    assert parsed["raw_value"] == "not-json"
    assert parsed["parse_error"] == "malformed_json"


def test_retried_bronze_batch_does_not_duplicate(spark: SparkSession, tmp_path) -> None:
    raw = spark.createDataFrame(
        [
            ("titan.commerce.order-events", 0, 1, "ord_day3order1", _at(12), _order("a", "10.00")),
            ("titan.commerce.order-events", 0, 2, "ord_day3order1", _at(12), "not-json"),
        ],
        KAFKA_SCHEMA,
    )
    parsed = parse_kafka_batch(raw).withColumn("batch_id", F.lit(0))
    parsed = parsed.withColumn("processing_time", F.current_timestamp())
    staging = f"file://{tmp_path}/staging"
    remote = f"file://{tmp_path}/bronze"
    write_bronze_batch(spark, parsed, 0, staging, remote)
    write_bronze_batch(spark, parsed, 0, staging, remote)
    stored = spark.read.parquet(remote)
    assert stored.count() == 2
    distinct = stored.select("kafka_topic", "kafka_partition", "kafka_offset").distinct().count()
    assert distinct == 2
    assert stored.filter(F.col("parse_status") == F.lit("invalid")).count() == 1


def test_conflict_does_not_enter_silver_orders_or_revenue(spark: SparkSession) -> None:
    events = spark.createDataFrame(
        [
            _event("accepted-id", "order_created", "accepted", "ord_day3order1", "10.00"),
            _event("conflict-id", "order_created", "conflict", "ord_day3order1", "80.00"),
            _event("dup-id", "order_created", "duplicate_operation", "ord_day3order1", "10.00"),
            _payment("pay-id", "ord_day3order1", "cus_day3customer"),
            _inventory("inv-id", "prd_day3product"),
        ],
        EVENT_SCHEMA,
    )
    curated = curate(events)
    orders = curated["orders"].collect()
    assert [row["event_id"] for row in orders] == ["accepted-id"]
    assert orders[0]["total_amount"] == Decimal("10.00")
    assert curated["payments"].count() == 1
    assert curated["inventory"].collect()[0]["product_id"] == "prd_day3product"
    assert validate_curation(events, curated) == []


def test_contradictory_payment_customer_is_excluded(spark: SparkSession) -> None:
    events = spark.createDataFrame(
        [
            _event("accepted-id", "order_created", "accepted", "ord_day3order1", "10.00"),
            _payment("pay-id", "ord_day3order1", "cus_othercust1"),
        ],
        EVENT_SCHEMA,
    )
    curated = curate(events)
    assert curated["payments"].count() == 0
    assert validate_curation(events, curated) == []


def test_pending_bronze_event_is_not_rejected_or_accepted(spark: SparkSession) -> None:
    events = spark.createDataFrame(
        [_event("accepted-id", "order_created", "accepted", "ord_day3order1", "10.00")],
        EVENT_SCHEMA,
    )
    bronze = spark.createDataFrame(
        [("accepted-id", "valid"), ("pending-id", "valid"), ("bad-id", "invalid")],
        "event_id string, parse_status string",
    )
    assert pending_event_count(bronze, events) == 1
    assert "pending-id" not in [row["event_id"] for row in curate(events)["orders"].collect()]


def test_decimal_parquet_round_trip(spark: SparkSession, tmp_path) -> None:
    frame = spark.createDataFrame([(Decimal("10.10"),)], "total_amount decimal(18,2)")
    path = f"file://{tmp_path}/money"
    frame.write.parquet(path)
    stored = spark.read.parquet(path).collect()[0]["total_amount"]
    assert stored == Decimal("10.10")


def test_event_time_windows_keep_currencies_separate(spark: SparkSession) -> None:
    orders = spark.createDataFrame(
        [
            ("a", "USD", Decimal("10.00"), _at(12, 15)),
            ("b", "USD", Decimal("2.50"), _at(12, 45)),
            ("c", "EUR", Decimal("4.00"), _at(13, 5)),
        ],
        "event_id string, currency string, total_amount decimal(18,2), occurred_at timestamp",
    )
    rows = {
        (row["window_start"].hour, row["currency"]): (
            row["order_count"],
            row["accepted_revenue"],
        )
        for row in order_windows(orders, "1 hour").collect()
    }
    assert rows[(12, "USD")] == (2, Decimal("12.50"))
    assert rows[(13, "EUR")] == (1, Decimal("4.00"))
    assert all(
        row["result_kind"] == "canonical_batch" for row in order_windows(orders, "1 hour").collect()
    )


def test_repeating_a_version_directory_does_not_append(spark: SparkSession, tmp_path) -> None:
    orders = curate(
        spark.createDataFrame(
            [_event("accepted-id", "order_created", "accepted", "ord_day3order1", "10.00")],
            EVENT_SCHEMA,
        )
    )["orders"]
    first = f"file://{tmp_path}/v1/orders"
    second = f"file://{tmp_path}/v2/orders"
    orders.coalesce(1).write.parquet(first)
    orders.coalesce(1).write.parquet(second)
    assert spark.read.parquet(first).count() == 1
    assert spark.read.parquet(second).count() == 1


def test_checkpoint_restart_does_not_duplicate(spark: SparkSession, tmp_path) -> None:
    source = tmp_path / "source"
    sink = tmp_path / "sink"
    checkpoint = tmp_path / "checkpoint"
    source.mkdir()
    (source / "a.json").write_text('{"n": 1}\n')

    def once():
        return (
            spark.readStream.schema("n int")
            .json(str(source))
            .writeStream.format("parquet")
            .option("path", str(sink))
            .option("checkpointLocation", str(checkpoint))
            .trigger(availableNow=True)
            .start()
        )

    once().awaitTermination()
    once().awaitTermination()
    assert spark.read.parquet(str(sink)).count() == 1


def test_failed_microbatch_is_retried_without_a_second_copy(spark: SparkSession, tmp_path) -> None:
    source = tmp_path / "source"
    sink = tmp_path / "sink"
    checkpoint = tmp_path / "checkpoint"
    source.mkdir()
    (source / "a.json").write_text('{"n": 7}\n')
    attempts = {"n": 0}

    def foreach(batch, epoch: int) -> None:
        batch.coalesce(1).write.mode("overwrite").parquet(str(sink / f"batch_id={epoch}"))
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise RuntimeError("crash after the batch files were written")

    def start():
        return (
            spark.readStream.schema("n int")
            .json(str(source))
            .writeStream.foreachBatch(foreach)
            .option("checkpointLocation", str(checkpoint))
            .trigger(availableNow=True)
            .start()
        )

    with pytest.raises(Exception, match="crash after"):
        start().awaitTermination()
    start().awaitTermination()
    assert attempts["n"] == 2
    stored = spark.read.parquet(str(sink / "batch_id=0"))
    assert stored.count() == 1
    assert stored.collect()[0]["n"] == 7


def test_watermark_drops_only_the_streaming_aggregate(spark: SparkSession, tmp_path) -> None:
    source = tmp_path / "source"
    sink = tmp_path / "sink"
    checkpoint = tmp_path / "checkpoint"
    source.mkdir()
    query = start_preliminary(
        spark,
        str(source),
        str(sink),
        str(checkpoint),
        "10 minutes",
        "1 hour",
    )
    try:
        _add(source, "1.json", "2026-01-01T12:05:00Z", "1")
        query.processAllAvailable()
        _add(source, "2.json", "2026-01-01T12:20:00Z", "2")
        query.processAllAvailable()
        _add(source, "3.json", "2026-01-01T13:30:00Z", "3")
        query.processAllAvailable()
        _add(source, "5.json", "2026-01-01T15:00:00Z", "5")
        query.processAllAvailable()
        before = _window_counts(spark, sink)
        _add(source, "4.json", "2026-01-01T12:40:00Z", "4")
        query.processAllAvailable()
        _add(source, "6.json", "2026-01-01T16:00:00Z", "6")
        query.processAllAvailable()
        after = _window_counts(spark, sink)
    finally:
        query.stop()
    assert before.get(12) == 2
    assert after.get(12) == 2
    historical = order_windows(_historical(spark), "1 hour").collect()
    noon = [row for row in historical if row["window_start"].hour == 12]
    assert noon[0]["order_count"] == 3
    assert noon[0]["result_kind"] == "canonical_batch"


def _historical(spark: SparkSession):
    return spark.createDataFrame(
        [
            ("1", "USD", Decimal("1.00"), _stamp("2026-01-01T12:05:00Z")),
            ("2", "USD", Decimal("1.00"), _stamp("2026-01-01T12:20:00Z")),
            ("4", "USD", Decimal("1.00"), _stamp("2026-01-01T12:40:00Z")),
            ("3", "USD", Decimal("1.00"), _stamp("2026-01-01T13:30:00Z")),
        ],
        "event_id string, currency string, total_amount decimal(18,2), occurred_at timestamp",
    )


def _add(source, name: str, occurred_at: str, event_id: str) -> None:
    (source / name).write_text(
        f'{{"occurred_at":"{occurred_at}","currency":"USD","event_id":"{event_id}"}}\n'
    )


def _window_counts(spark: SparkSession, sink) -> dict[int, int]:
    path = str(sink)
    try:
        rows = spark.read.parquet(path).collect()
    except Exception:
        return {}
    return {row["window_start"].hour: row["order_count"] for row in rows}


def _at(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 1, 1, hour, minute, tzinfo=timezone.utc)


def _stamp(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


def _order(event_id: str, total: str) -> str:
    return json.dumps(
        {
            "event_id": event_id,
            "event_type": "order_created",
            "schema_version": 1,
            "occurred_at": "2026-01-01T12:00:00Z",
            "produced_at": "2026-01-01T12:00:00Z",
            "source": "test",
            "payload": {
                "order_id": "ord_day3order1",
                "customer_id": "cus_day3customer",
                "currency": "USD",
                "total_amount": total,
            },
        }
    )


def _event(event_id: str, event_type: str, disposition: str, order_id: str, total: str) -> tuple:
    envelope = _order(event_id, total).replace("ord_day3order1", order_id)
    return (
        event_id,
        event_type,
        disposition,
        order_id,
        _at(12),
        _at(12),
        "test",
        envelope,
        "titan.commerce.order-events",
        0,
        1,
    )


def _payment(event_id: str, order_id: str, customer_id: str) -> tuple:
    envelope = json.dumps(
        {
            "event_id": event_id,
            "event_type": "payment_completed",
            "schema_version": 1,
            "occurred_at": "2026-01-01T12:00:00Z",
            "produced_at": "2026-01-01T12:00:00Z",
            "source": "test",
            "payload": {
                "payment_id": "pay_day3pay001",
                "order_id": order_id,
                "customer_id": customer_id,
                "currency": "USD",
                "amount": "10.00",
                "method": "card",
            },
        }
    )
    return (
        event_id,
        "payment_completed",
        "accepted",
        order_id,
        _at(12),
        _at(12),
        "test",
        envelope,
        "titan.commerce.payment-events",
        0,
        2,
    )


def _inventory(event_id: str, product_id: str) -> tuple:
    envelope = json.dumps(
        {
            "event_id": event_id,
            "event_type": "inventory_updated",
            "schema_version": 1,
            "occurred_at": "2026-01-01T12:00:00Z",
            "produced_at": "2026-01-01T12:00:00Z",
            "source": "test",
            "payload": {
                "product_id": product_id,
                "warehouse_id": "wh_main0001",
                "quantity_on_hand": 10,
                "delta": 1,
            },
        }
    )
    return (
        event_id,
        "inventory_updated",
        "accepted",
        None,
        _at(12),
        _at(12),
        "test",
        envelope,
        "titan.commerce.inventory-events",
        0,
        3,
    )


EVENT_SCHEMA = """
    event_id string,
    event_type string,
    disposition string,
    order_id string,
    occurred_at timestamp,
    ingested_at timestamp,
    source string,
    event_envelope string,
    kafka_topic string,
    kafka_partition int,
    kafka_offset long
"""
