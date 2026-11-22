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

ClickHouse and MinIO start with the Compose stack so later milestones have a warehouse and an object store. No pipeline writes to them.

## Ownership

| Path | Responsibility |
| --- | --- |
| `packages/contracts` | Canonical `order_created` envelope and money rules |
| `packages/store` | SQLAlchemy models and Alembic migrations |
| `services/event-generator` | Synthetic producer and Kafka acknowledgements |
| `services/ingestion` | Consume, validate, persist, then commit offsets |
| `apps/api` | Read models over the operational tables |
| `apps/web` | Operations overview. Planned modules stay empty of metrics |

The API depends on the store package, not on the Kafka client. The generator does not import the database.

## Delivery semantics

Consumption is at-least-once. PostgreSQL uniqueness on `event_id` makes redelivery idempotent. Producer idempotence covers broker retries only. There is no distributed transaction between Kafka and PostgreSQL.

See [consumption-semantics.md](consumption-semantics.md) and [money-policy.md](money-policy.md).
