# Coordinated platform disaster recovery (Upgrade 4E)

TITAN does not have an atomic distributed snapshot across Kafka, PostgreSQL,
MinIO and ClickHouse. The defensible recovery-point contract is therefore a
bounded, quiesced boundary: stop producers and consumers, wait for durable
PostgreSQL dispositions, capture Kafka per-partition watermarks, select one
published Silver publication, and verify that PostgreSQL warehouse metadata and
the ClickHouse serving pointer reference that same Silver identity.

`scripts/coordinated_dr.py capture` creates an integrity-protected manifest
containing Kafka ranges, PostgreSQL event identity/dispositions, MinIO Silver
object sizes/checksums, ClickHouse serving metadata, and the checkpoint policy.
`verify` rejects manifest tampering and refuses lineage disagreement. The
recovery order is: restore PostgreSQL metadata and event identity, restore or
replay bounded Kafka records without copying source offsets, restore verified
MinIO objects, regenerate Bronze/Silver without reusing Spark checkpoints,
rebuild ClickHouse through the atomic warehouse loader, run quality and
financial reconciliation, then activate the serving pointer.

The procedure must run in a new Compose project with new volumes, database,
bucket, topics and consumer group. Ingestion and serving remain disabled until
validation completes. Existing component utilities remain responsible for the
actual PostgreSQL, Kafka and MinIO archive mechanics; this coordinator proves
their selected recovery boundary is mutually consistent.

Recovery-point limitations are explicit: Kafka source offsets are evidence and
are remapped on replay; Spark checkpoints are not portable recovery state;
PostgreSQL, MinIO and ClickHouse require lineage checks; and observed laboratory
timings are not production RTO/RPO guarantees. A missing or corrupt component
backup must stop recovery before serving activation.

## Actual artifact restore

The local completion test used two separate Compose projects, `titan-4e-source`
and `titan-4e-dest`, with separate Kafka, PostgreSQL, MinIO, ClickHouse and
checkpoint volumes. Actual PostgreSQL, MinIO and Kafka backup artifacts were
created and verified before restoration.

The destination restored PostgreSQL into `titan_restore`, MinIO into
`titan-lake-restore`, and Kafka into three empty `restored.*` topics. The real
ingestion consumer replayed the records with a new consumer group; source
offsets were not copied. All 256 event IDs remained unique and accepted, while
512 replayed messages were durably recorded as duplicate observations. The
existing ClickHouse loader rebuilt and activated 86 orders, 86 payments, 84
inventory rows and 86 items after dbt and quality checks. Currency totals were
EUR 2308.38, GBP 2171.39 and USD 1995.90.

Negative tests rejected a missing MinIO object, invalid Kafka records, a
nonempty MinIO destination, and a corrupted PostgreSQL archive checksum was
detected before restore. An
interrupted ClickHouse rebuild left the active pointer unchanged. Evidence is
in `benchmarks/coordinated-restore.json`. PostgreSQL restore wall time was not
instrumented by the existing utility; other measured timings and checksums are
recorded in the evidence file.
## Confidentiality controls

Coordinated manifests and component backup trees are protected with POSIX
`0700`/`0600` modes and CI `umask 077`. Windows ACLs must provide the equivalent
boundary. The coordinated manifest contains lineage and recovery metadata but
does not encrypt component artifacts; access-controlled storage and retention
remain operator responsibilities.
