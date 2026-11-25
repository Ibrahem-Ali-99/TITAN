# Kafka disaster recovery (Upgrade 4C)

TITAN uses a portable event-log export and replay strategy. The backup captures a
fixed per-partition range `[low_watermark, high_watermark)` for explicitly named
topics. Each JSONL record preserves the topic, source partition and offset, key,
exact value bytes, headers, and broker timestamp. The manifest hashes the complete
JSONL payload and rejects missing, duplicated, truncated, or modified records.

Restore is intentionally restricted to an isolated broker whose topic names and
partition counts already match the manifest and whose topics are empty. Records
are produced to their original partitions, but Kafka assigns new offsets; the
reported mapping is the authoritative source-to-restored offset evidence. Consumer
groups are never restored or sought, and ingestion/Spark must remain stopped until
the restored log has been independently verified.

This is appropriate for TITAN's single-broker development architecture and is
portable across brokers. It is not broker-volume recovery: retention may have
removed records before export, and a live producer changing a partition during
watermark capture requires an operational quiescence window. Production recovery
would additionally require replicated Kafka, ACLs, schema-registry policy (if
introduced), PostgreSQL deduplication state, and coordinated Spark checkpoint
handling. Spark checkpoints are deliberately not part of this backup.

## Local verification

Use a disposable Compose project or broker, create isolated topic names with the
same partition count, publish deterministic records, then run:

```text
python -m scripts.kafka_dr --bootstrap HOST:PORT backup --topic titan.dr.events --output OUT
python -m scripts.kafka_dr --bootstrap HOST:PORT verify --backup OUT
python -m scripts.kafka_dr --bootstrap HOST:PORT restore --backup OUT
```

The restore command refuses nonempty topics and mismatched partition counts. The
negative tests mutate or remove a JSONL record and verify that restore refuses to
proceed. The resulting Kafka offsets must be treated as new positions; replaying
the same commerce payloads through the normal ingestion consumer is safe only when
the disposable PostgreSQL database contains the same deduplication constraints.

## Kafka-to-PostgreSQL recovery integration

`scripts/kafka_postgres_recovery.py` prepares five deterministic records across
three partitions: two accepted orders, one duplicate event ID, one new event ID
with a conflicting order business key, and one malformed JSON value. It exports the
source topic, restores it to `recovery.titan.recovery.source`, and starts the real
ingestion consumer with `INGESTION_CONSUMER_GROUP=titan-recovery-ingestion` and a
dedicated topic list. The verification waits until all five records have a durable
commerce or quarantine disposition, committed offsets reach the restored high
watermarks, and lag is zero. It then restarts the consumer and requires the same
database counts and classifications. The disposable CI workflow is
`.github/workflows/kafka-postgres-recovery.yml`.

The local disposable run processed exactly five restored messages: two accepted
commerce rows, one conflict row, one quarantine row (`malformed_json`), and one
duplicate-delivery observation. The recovery group committed through the restored
high watermarks and reported zero lag before and after an ingestion restart. The
accepted event IDs remained unique and the durable classification counts were
unchanged after restart. Preparation took 2,889 ms and verification including the
restart took 1,423 ms. Evidence is recorded in `benchmarks/kafka-dr.json`.

## Recovery-point and RTO limitations

This export is a logical snapshot, not point-in-time recovery. The recovery point
is bounded by the captured high watermarks and Kafka retention. The timing depends
on record count, payload size, broker disk, and network. A complete TITAN recovery
still requires coordinated PostgreSQL, MinIO, Kafka, and Spark state recovery and
has not been claimed by this phase.
## Confidentiality controls

Kafka export trees, including `records.jsonl` and manifests, are written with
`0700` directory and `0600` file modes on POSIX systems. CI applies `umask 077`.
Windows operators must enforce equivalent directory ACLs. Payloads remain
plaintext by design; checksums provide integrity, not confidentiality.
