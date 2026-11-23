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
