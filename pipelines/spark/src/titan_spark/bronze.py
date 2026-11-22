"""Kafka to Bronze. The checkpoint is on a local volume, not on MinIO.

Spark records its own progress in that checkpoint. It does not read the
``titan-ingestion`` consumer group's committed offsets, and the two consumers
do not observe records in the same order.

A microbatch is written to a local directory and then copied to
``s3a://titan-lake/bronze/commerce_events/batch_id=<id>/``. The same batch id
overwrites that prefix, so a retry after a failed commit replaces the files.
Readers still dedupe by Kafka topic, partition, and offset. This is replay
safety for one batch, not an exactly-once lake.
"""

import sys
from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from titan_spark.config import Settings, load_settings
from titan_spark.metadata import record_job
from titan_spark.parse import parse_kafka_batch
from titan_spark.session import build_session, copy_tree
from titan_spark.topics import COMMERCE_TOPIC_NAMES


def write_bronze_batch(
    spark,
    parsed: DataFrame,
    batch_id: int,
    staging_root: str,
    bronze_uri: str,
) -> None:
    local = f"{staging_root}/bronze/{batch_id}"
    remote = f"{bronze_uri}/batch_id={batch_id}"
    (parsed.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(local))
    copy_tree(spark, local, remote)


def run_bronze(settings: Settings, trigger: str) -> None:
    spark = build_session(settings, "titan-bronze")
    staging = f"file://{settings.checkpoint_root}/staging"
    record_job(
        settings,
        job_name="bronze_stream",
        state="running",
        detail="microbatches overwrite their own batch_id prefix",
    )
    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", ",".join(COMMERCE_TOPIC_NAMES))
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", str(settings.max_offsets_per_trigger))
        .option("failOnDataLoss", "true")
        .load()
    )

    def foreach(batch: DataFrame, batch_id: int) -> None:
        if batch.limit(1).count() == 0:
            record_job(
                settings,
                job_name="bronze_stream",
                state="running",
                last_batch_id=batch_id,
                detail="empty microbatch",
            )
            return
        parsed = parse_kafka_batch(batch).withColumn("batch_id", F.lit(int(batch_id)))
        parsed = parsed.withColumn("processing_time", F.current_timestamp())
        write_bronze_batch(spark, parsed, int(batch_id), staging, settings.bronze_uri)
        offsets = _batch_offsets(parsed)
        record_job(
            settings,
            job_name="bronze_stream",
            state="running",
            last_batch_id=int(batch_id),
            last_offsets=offsets,
            detail="batch copied to bronze; checkpoint commits after this function returns",
        )

    writer = kafka.writeStream.foreachBatch(foreach).option(
        "checkpointLocation", settings.bronze_checkpoint
    )
    if trigger == "availableNow":
        query = writer.trigger(availableNow=True).start()
    elif trigger == "processing":
        query = writer.trigger(processingTime="10 seconds").start()
    else:
        raise ValueError("trigger must be availableNow or processing")
    try:
        query.awaitTermination()
    except Exception as exc:
        record_job(
            settings,
            job_name="bronze_stream",
            state="failed",
            error_detail=str(exc)[:2000],
        )
        raise
    progress = query.lastProgress or {}
    record_job(
        settings,
        job_name="bronze_stream",
        state="succeeded",
        last_batch_id=progress.get("batchId"),
        last_offsets=_progress_offsets(progress),
        detail="available offsets were processed; this is not a container health check",
    )


def _batch_offsets(parsed: DataFrame) -> str:
    rows = (
        parsed.groupBy("kafka_topic", "kafka_partition")
        .agg(F.max("kafka_offset").alias("max_offset"))
        .collect()
    )
    parts = [f"{row['kafka_topic']}:{row['kafka_partition']}={row['max_offset']}" for row in rows]
    return ",".join(sorted(parts))[:4000]


def _progress_offsets(progress: dict) -> str:
    sources = progress.get("sources") or []
    chunks: list[str] = []
    for source in sources:
        end = source.get("endOffset")
        if end is not None:
            chunks.append(str(end))
    return " ".join(chunks)[:4000]


def main() -> None:
    trigger = "availableNow"
    if "--trigger" in sys.argv:
        trigger = sys.argv[sys.argv.index("--trigger") + 1]
    started = datetime.now(timezone.utc).isoformat()
    print(f"bronze start {started} trigger={trigger}", file=sys.stderr)
    run_bronze(load_settings(), trigger)


if __name__ == "__main__":
    main()
