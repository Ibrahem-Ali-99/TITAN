# Upwork portfolio description

TITAN is a local data-engineering project I built to show a complete commerce analytics path.

Synthetic orders, payments, and inventory events enter through a generator, REST, or CSV. Kafka delivers them. The consumer classifies each record in PostgreSQL, quarantines invalid payloads, and commits offsets after the write. Spark builds Bronze evidence and a Silver version on MinIO. ClickHouse serves one published snapshot. dbt and a blocking quality gate run before that snapshot changes. Dagster orders the jobs. FastAPI and a Next.js console read the same publication.

A duplicate event does not create a second accepted order. A failed quality gate leaves the previous publication in place. Forecast serving uses the seasonal naive method because it beat the trained gradient-boosting model on the held-out week. Both results stay stored.

This is a workstation system with synthetic data. I have not deployed it as a production commerce platform, and the benchmark I recorded is a producer-acknowledgement sample, not a capacity claim.
