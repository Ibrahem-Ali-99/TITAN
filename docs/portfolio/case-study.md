# Case study

TITAN is a portfolio platform for a commerce analytics pipeline. The business question is narrow: which orders were accepted, in which currency, and which published snapshot is safe to read. The events are synthetic. There is no merchant, no production deployment, and no measured cost saving.

## What was built

Orders, payments, and inventory events are produced by a generator, REST, or CSV. Kafka carries them. A Python consumer writes the classification to PostgreSQL and commits the offset afterward. Invalid records go to quarantine. Spark writes Bronze evidence to MinIO and a Silver version from the accepted dispositions. ClickHouse holds versioned facts. dbt tests the candidate. A blocking quality gate runs before `wh_active` changes. Dagster orders that publication. FastAPI and a Next.js console read the published snapshot.

Demand training compares a fixed gradient-boosting model with a seasonal naive baseline on the last seven dates. The baseline won. The API label is `seasonal_naive`. The candidate metrics and artifact were kept. Later warehouse publications do not rewrite that training publication id.

## What was verified

- A duplicate delivery and a conflicting payload did not add a second accepted order.
- An injected revenue mismatch left the previous publication serving.
- The following clean run published `50d22fe5-8db0-4f2f-bc00-47bda48152fc` with 362 accepted orders: EUR 18711.58, GBP 16558.03, USD 17768.77.
- Stopping Kafka left that class of read available from ClickHouse, and starting Kafka again left the publication id unchanged. This was observed on the prior publication before the demo run.
- Reconciliation refuses a publication that has orders but is missing dbt, quality, Silver lineage, or matching fact counts.
- A 5,000-record producer sample on `titan.benchmark.acks` acknowledged 1184.964 events/second in 4.22 seconds. It did not change `logical_orders`.

## Limits

The data is synthetic. The benchmark is one producer-acknowledgement sample on a side topic, on a workstation, with other containers running. Spark `kill -9` recovery is recorded in [../audits/spark-crash-recovery.md](../audits/spark-crash-recovery.md). A live two-member consumer rebalance and a MinIO outage during a write were not executed. Console screenshots captured later are in [screenshots.md](screenshots.md).
