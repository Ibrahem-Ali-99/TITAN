"""Distributed envelope parsing. Invalid Kafka values stay in the batch."""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from titan_spark.topics import KNOWN_EVENT_TYPES

ENVELOPE_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("event_type", StringType()),
        StructField("schema_version", IntegerType()),
        StructField("occurred_at", StringType()),
        StructField("produced_at", StringType()),
        StructField("source", StringType()),
        StructField("_corrupt", StringType()),
    ]
)


def parse_utc(column: Column) -> Column:
    """Accept a trailing ``Z`` or ``+00:00``. Any other offset becomes null."""
    text = column.cast("string")
    utc = text.endswith("Z") | text.endswith("+00:00")
    stripped = F.regexp_replace(text, "(Z|\\+00:00)$", "")
    spaced = F.regexp_replace(stripped, "T", " ")
    return F.when(utc, F.to_timestamp(spaced)).otherwise(F.lit(None).cast("timestamp"))


def parse_kafka_batch(kafka_batch: DataFrame) -> DataFrame:
    """Keep every input row. ``parse_status`` says whether the envelope parsed."""
    raw = kafka_batch.select(
        F.col("topic").alias("kafka_topic"),
        F.col("partition").cast("int").alias("kafka_partition"),
        F.col("offset").cast("long").alias("kafka_offset"),
        F.col("key").cast("string").alias("kafka_key"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("value").cast("string").alias("raw_value"),
    )
    parsed = F.from_json(
        F.col("raw_value"),
        ENVELOPE_SCHEMA,
        {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt"},
    )
    known = F.col("parsed.event_type").isin(list(KNOWN_EVENT_TYPES))
    version_ok = F.col("parsed.schema_version") == F.lit(1)
    identity_ok = F.col("parsed.event_id").isNotNull() & (F.length(F.col("parsed.event_id")) > 0)
    valid = (
        F.col("raw_value").isNotNull()
        & F.col("_corrupt").isNull()
        & known
        & version_ok
        & identity_ok
    )
    reason = (
        F.when(F.col("raw_value").isNull(), F.lit("empty_value"))
        .when(F.col("_corrupt").isNotNull(), F.lit("malformed_json"))
        .when(~known, F.lit("unsupported_event_type"))
        .when(~version_ok, F.lit("unsupported_schema_version"))
        .when(~identity_ok, F.lit("missing_event_id"))
        .otherwise(F.lit(None))
    )
    return (
        raw.withColumn("parsed", parsed)
        .withColumn("_corrupt", F.col("parsed._corrupt"))
        .withColumn("parse_status", F.when(valid, F.lit("valid")).otherwise(F.lit("invalid")))
        .withColumn("parse_error", F.when(valid, F.lit(None).cast("string")).otherwise(reason))
        .withColumn("event_id", F.col("parsed.event_id"))
        .withColumn("event_type", F.col("parsed.event_type"))
        .withColumn("schema_version", F.col("parsed.schema_version"))
        .withColumn("occurred_at", parse_utc(F.col("parsed.occurred_at")))
        .withColumn("produced_at", parse_utc(F.col("parsed.produced_at")))
        .withColumn("source", F.col("parsed.source"))
        .drop("parsed", "_corrupt")
    )
