# Architecture

Day 1 is one vertical slice plus two idle infrastructure services.

```text
event generator  --->  Kafka topic titan.commerce.order-events
                              |
                              v
                        ingestion consumer
                              |
                              v
                        PostgreSQL
                         commerce_events
                         ingestion_quarantine
                              |
                              v
                          FastAPI  --->  Next.js overview
```

MinIO holds Bronze and Silver Parquet. ClickHouse serves one published warehouse snapshot. dbt builds the models readers query. Dagster orders the jobs and does not transform rows.

```text
published Silver Parquet
        |
        v
ClickHouse versioned facts
        |
        v
dbt candidate tests, then wh_active exchange
        |
        v
FastAPI  --->  Next.js warehouse, pipelines, overview
```

## Ownership

| Path | Responsibility |
| --- | --- |
| `packages/contracts` | Canonical `order_created` envelope and money rules |
| `packages/store` | SQLAlchemy models and Alembic migrations |
| `services/event-generator` | Synthetic producer and Kafka acknowledgements |
| `services/ingestion` | Consume, validate, persist, then commit offsets |
| `apps/api` | Read models over the operational tables |
| `apps/web` | Operations overview and the published warehouse |
| `pipelines/warehouse` | Silver to ClickHouse load and publication pointer |
| `pipelines/dagster` | Asset order, retries, and the hourly schedule |
| `warehouse/dbt` | Analytical models over the active publication |

The API depends on the store package, not on the Kafka client. The generator does not import the database.

## Delivery semantics

Consumption is at-least-once. PostgreSQL uniqueness on `event_id` makes redelivery idempotent. Producer idempotence covers broker retries only. There is no distributed transaction between Kafka and PostgreSQL.

See [consumption-semantics.md](consumption-semantics.md) and [money-policy.md](money-policy.md).
