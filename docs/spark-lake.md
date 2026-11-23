# Spark lake

Day 3 adds a single-node Spark reader beside the existing ingestion consumer. It does not replace that consumer and it does not write `commerce_events`.

## Ownership

Kafka is the log of emitted records. PostgreSQL is authoritative for acceptance and for `accepted`, `duplicate_operation`, and `conflict`. Bronze Parquet in MinIO keeps the raw Kafka record, including invalid values and duplicate deliveries. Silver is a validated snapshot of the rows PostgreSQL has already classified. Spark does not write ClickHouse. The later warehouse load is described in [warehouse.md](warehouse.md).

The Spark query and the `titan-ingestion` group are independent. A record can be in Bronze before PostgreSQL has classified it. That state is `pending`. It is not an accepted order and it is not a rejection. Arrival order is not assumed to match.

## Versions

| Piece | Pin | Why |
| --- | --- | --- |
| Spark | 4.0.1, `spark-4.0.1-bin-hadoop3` | Current 4.0 maintenance release. Requires Java 17 or 21. |
| Java | 21 (Temurin) | Supported by Spark 4.0.1. |
| Hadoop | 3.4.1 | `hadoop.version` in the Spark 4.0.1 POM. |
| S3A | `hadoop-aws` 3.4.1 | Must match Hadoop. Hadoop's own dependency report pairs it with AWS SDK v2 bundle 2.24.6. |
| Kafka connector | `spark-sql-kafka-0-10_2.13` 4.0.1, `spark-token-provider-kafka-0-10_2.13` 4.0.1, `kafka-clients` 3.9.1, `commons-pool2` 2.12.0 | The hadoop3 binary omits the connector. Jars are pinned to the Spark 4.0.1 POM compile dependencies and checked with Maven SHA-1. Spark SQL and Hadoop stay provided by the distribution. |
| PostgreSQL JDBC | 42.7.7 | Read-only access to `commerce_events`. |

Checkpoints are not on MinIO. Spark's default checkpoint commit renames a temporary file into place. The [Spark cloud integration guide](https://spark.apache.org/docs/latest/cloud-integration.html) says S3A directory renames are unsafe. The Bronze checkpoint is the Docker volume `spark_checkpoints` mounted at `/var/lib/titan/spark-checkpoints`, where rename is a local filesystem operation. Spark resumes from that checkpoint. It does not use the ingestion group's offsets.

S3A uses path-style access against `S3_ENDPOINT` and `SimpleAWSCredentialsProvider`. Those settings are applied to the live Hadoop configuration after the Spark context exists. `spark.conf` updates made after startup do not reach that configuration, so MinIO would otherwise fall through to the default AWS credential chain. The settings object redacts credentials, and the jobs log at ERROR.

## Paths

```text
s3a://titan-lake/bronze/commerce_events/batch_id=<id>/
s3a://titan-lake/silver/versions/<publication_id>/orders/
s3a://titan-lake/silver/versions/<publication_id>/payments/
s3a://titan-lake/silver/versions/<publication_id>/inventory/
s3a://titan-lake/silver/versions/<publication_id>/order_metrics/
s3a://titan-lake/silver/current/
```

`lake_publications` in PostgreSQL selects the current Silver version: the latest row with status `published`. `silver/current/` is a convenience copy written after that row commits. If they disagree, PostgreSQL wins. Staging and failed versions are not current, even if their files were copied.

Plain Parquet has no table transaction. A microbatch is written on the local volume first, then copied to its `batch_id` prefix. A retry of that batch overwrites the same prefix. Bronze readers still dedupe by topic, partition, and offset. That is replay safety for one batch, not exactly-once end to end. Silver writes a new version directory. Repeating the job adds a version; it does not append into the current one. The two consumers of a version see one snapshot.

## Schemas

Bronze keeps topic, partition, offset, key, Kafka timestamp, raw value, processing time, parse status, and envelope fields when they parse. An invalid value stays in the file with `parse_status=invalid`. PostgreSQL quarantine remains the ingestion dead-letter record.

Silver orders contain accepted `order_created` rows only. Payments and inventory updates use their accepted rows. A payment whose customer disagrees with the accepted order is counted as an identifier conflict and left out of the payments dataset. A payment whose order is not in the snapshot yet is kept with `order_matched=false`. Money is `decimal(18,2)` parsed from the decimal string, not from a float. `occurred_at` is the business time. `ingested_at` stays the database write time. Both are UTC.

Order metrics are one row per event-time window and currency. Revenue is not summed across currencies. `result_kind=canonical_batch` is the historical recomputation from accepted orders. A separate streaming query can emit `preliminary_streaming` windows with a configurable watermark (`WATERMARK_DELAY`, default 10 minutes). A watermark drops late rows from that streaming query only. The next Silver run still includes every accepted order.

New Bronze columns may be added as nullable fields. A Silver schema change is a new publication. Old versions are not rewritten. Published files are not edited in place.

## Run

Spark is a Compose profile so it is not running beside the idle stack. The driver uses 768 MB inside a 1536 MB container limit, `local[1]`.

```powershell
docker compose run --rm --build migrate
docker compose --profile processing run --rm --build lake-init
docker compose --profile processing run --rm spark-bronze
docker compose --profile processing run --rm spark-silver
docker compose --profile processing run --rm spark-tests
uv run python scripts/verify_day3.py
```

`spark-bronze` uses `availableNow`: it reads to the current Kafka end and exits. A second run uses the checkpoint. Delete the `spark_checkpoints` volume only when a full Bronze rebuild is intended; batch ids would start again.

The scale path is a different Spark master and dropping `coalesce(1)` once a version is large enough to partition by `occurred_at` date. The classification rule does not change: Silver still copies PostgreSQL dispositions.

## Resources

The idle stack does not include Spark. Kafka 768 MB, PostgreSQL 512 MB, ClickHouse 1536 MB, MinIO 256 MB, API 512 MB, ingestion 512 MB, and the web app 768 MB are the existing limits. A Bronze or Silver run adds one 1536 MB container. ClickHouse is still unused. Stop it for the duration of a Spark run if Docker Desktop is near its memory limit, then start it again before `verify_infrastructure.py`.

The Pipelines page reads `GET /api/v1/lake/status`. Those numbers are the stored publication and the last job report. A Spark container with no report is `not_reported`. A `running` report older than 90 seconds is `stale`.
