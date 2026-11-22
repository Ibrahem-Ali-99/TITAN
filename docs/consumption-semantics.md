# Consumption semantics

The ingestion service is **at-least-once** with **idempotent persistence**. It is not an end-to-end exactly-once pipeline.

## Offset policy

1. `enable.auto.commit` and `enable.auto.offset.store` are false.
2. A record is validated.
3. A valid `order_created` event is inserted in one database transaction.
4. The Kafka offset is committed only after that transaction commits.
5. `event_id` is the primary key. A second delivery hits the uniqueness constraint, leaves one row, and then commits the offset.
6. A database outage raises a transient error. The offset is not committed. The consumer pauses assigned partitions, keeps polling so it stays in the group, and holds any already-fetched later records in order. The failed record is retried from memory. Those later records are not seeked: a seek to the last queued offset would skip the records in between.
7. Shutdown stops the loop without committing an in-flight record that was not stored.

A restart therefore rereads uncommitted records. Those records do not increase the persisted count when the `event_id` is already stored.

Committing an offset means "this record was durably applied or durably quarantined." It does not mean the producer, broker, and database shared one transaction. A crash between the database commit and the offset commit is recovered by the duplicate path.

## Invalid records

Malformed JSON, schema violations, inconsistent totals, a missing key, and a key that is not the `order_id` are written to `ingestion_quarantine` with topic, partition, offset, a truncated raw value, and the error. The offset is committed after that insert so one bad record cannot block the partition.

If the quarantine insert fails because the database is down, the offset is not committed.

Records are truncated for storage. The table is a minimal quarantine, not a replay product. Replay tooling is a later milestone.

## What the API counts

`persisted_events` is the number of rows in `commerce_events`. It is not broker throughput, not producer attempts, and not quarantined records. `ingested_at` is when the consumer stored the row. `occurred_at` is the synthetic business time on the event.

## Keys and topic

Topic: `titan.commerce.order-events`, 3 partitions, replication factor 1, 7-day retention. The record key is `order_id`. The topic is created by the client; broker auto-creation is off.
