# Resume project description

Built TITAN, a local commerce data platform: Kafka ingestion into PostgreSQL, Spark Bronze and Silver on MinIO, a versioned ClickHouse warehouse tested with dbt, Dagster publication, and a FastAPI plus Next.js operations console.

Designed at-least-once consumption with idempotent event persistence, business-level conflict handling, and quarantine. Publication advances only after dbt and blocking quality checks. Recovery puts the serving pointer back on the newest warehouse version whose quality run, dbt run, Silver lineage, and fact counts agree.

Trained a demand model on synthetic daily quantities and served the seasonal naive baseline when its held-out error was lower, keeping the candidate metrics and artifact. Measured a 5,000-record producer acknowledgement sample on an isolated topic at 1184.964 events/second over 4.22 seconds. The events are synthetic and the result is one workstation sample.
