"""Event-time windows.

``order_windows`` is the historical recomputation. It uses ``occurred_at`` and
includes every accepted order the caller passes in. ``run_preliminary`` is a
separate Structured Streaming query. Its watermark drops late rows from that
query only. Those rows remain eligible for the next Silver publication.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from titan_spark.parse import parse_utc


def order_windows(orders: DataFrame, window_duration: str) -> DataFrame:
    if orders.limit(1).count() == 0:
        return orders.sparkSession.createDataFrame(
            [],
            schema="""
                window_start timestamp,
                window_end timestamp,
                currency string,
                order_count long,
                accepted_revenue decimal(18,2),
                result_kind string
            """,
        )
    grouped = orders.groupBy(
        F.window("occurred_at", window_duration).alias("window"),
        "currency",
    ).agg(
        F.count("*").alias("order_count"),
        F.sum("total_amount").alias("accepted_revenue"),
    )
    return grouped.select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "currency",
        "order_count",
        "accepted_revenue",
        F.lit("canonical_batch").alias("result_kind"),
    )


def preliminary_aggregation(
    events: DataFrame, watermark_delay: str, window_duration: str
) -> DataFrame:
    timed = events.withColumn("occurred_at_ts", parse_utc(F.col("occurred_at")))
    aggregated = (
        timed.filter(F.col("occurred_at_ts").isNotNull())
        .withWatermark("occurred_at_ts", watermark_delay)
        .groupBy(F.window("occurred_at_ts", window_duration).alias("window"), "currency")
        .agg(F.count("*").alias("order_count"))
    )
    return aggregated.select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "currency",
        "order_count",
        F.lit("preliminary_streaming").alias("result_kind"),
    )


def start_preliminary(
    spark: SparkSession,
    source_path: str,
    sink_path: str,
    checkpoint: str,
    watermark_delay: str,
    window_duration: str,
):
    source = (
        spark.readStream.schema("occurred_at string, currency string, event_id string")
        .option("maxFilesPerTrigger", 1)
        .json(source_path)
    )
    aggregated = preliminary_aggregation(source, watermark_delay, window_duration)
    return (
        aggregated.writeStream.outputMode("append")
        .format("parquet")
        .option("path", sink_path)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime="1 second")
        .start()
    )
