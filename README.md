# TITAN

Local foundation for a real-time commerce data platform. Day 1 runs one path:

**synthetic `order_created` event → Kafka → Python consumer → PostgreSQL → FastAPI → Next.js.**

MinIO holds the Parquet lake. ClickHouse serves the published Silver snapshot through dbt models. Dagster orders that publication. Events produced here are synthetic and are labeled `synthetic.event-generator`.

Persisted counts are rows in PostgreSQL. They are not Kafka broker throughput. Consumption is at-least-once with idempotent persistence, not end-to-end exactly-once. Day 2 adds REST and CSV inputs on the same Kafka path. Details are in [docs/consumption-semantics.md](docs/consumption-semantics.md), [docs/money-policy.md](docs/money-policy.md), and [docs/ingestion-hub.md](docs/ingestion-hub.md).

## Prerequisites

- Windows with Docker Desktop (WSL2). Allocate at least 6 GB of memory to Docker; 8 GB is more comfortable. See [docs/local-infrastructure.md](docs/local-infrastructure.md).
- Python 3.12 and [uv](https://docs.astral.sh/uv/) 0.12.x for host-side tests and the generator.
- Node.js 20.9 or newer (this repo was built with Node.js 26.13.1) to build the dashboard on the host.

## Pinned runtime images

| Component | Pin | Why |
| --- | --- | --- |
| Kafka | `apache/kafka:4.4.0` | Current supported Apache release, KRaft, no ZooKeeper |
| PostgreSQL | `postgres:18.7-alpine3.23` | Current 18.x patch. 19.0 was only days old on 22 Nov 2026 |
| ClickHouse | `clickhouse/clickhouse-server:26.8.36.3` | Current LTS patch, not `latest` |
| MinIO | `alpine/minio:RELEASE.2025-10-15T17-29-55Z` | Community mirror. Official Hub/Quay pulls were not available |
| Python | `python:3.12.13-slim-bookworm` | Matches the workspace interpreter |
| Node | `node:26.13.1-bookworm-slim` | Current Node.js LTS used for the web image |
| Spark | 4.0.1 on Java 21, Hadoop 3.4.1 | Single-node profile. S3A uses `hadoop-aws` 3.4.1 and AWS SDK bundle 2.24.6 |
| Dagster | 1.13.31 | Orchestration only. Python 3.12, not the Spark image |
| dbt | core 1.12.7, dbt-clickhouse 1.10.3 | The adapter tests include ClickHouse 26.8 |

Python libraries are pinned in the workspace `pyproject.toml` files and locked in `uv.lock`. Notable pins: FastAPI 0.165.0, Pydantic 2.14.4, SQLAlchemy 2.1.9, Alembic 1.20.1, psycopg 3.3.9, confluent-kafka 2.16.2 (librdkafka 2.16.0), uvicorn 0.54.0. The web app pins Next.js 16.4.6.

## Start the stack

```powershell
copy .env.example .env
docker compose up --build -d
docker compose ps
```

Migrations run in the one-shot `migrate` service before the consumer and API start. The event generator is not a resident process.

Host-side tools, from the repository root:

```powershell
uv sync --all-packages
uv run alembic -c packages/store/alembic.ini upgrade head
uv run python -m event_generator --count 10 --seed 42 --rate 5
```

The same generator inside Compose, using the internal broker address:

```powershell
docker compose --profile generate run --rm event-generator --count 10 --seed 42 --rate 5 --bootstrap-servers kafka:19092
```

## Check health and the vertical slice

```powershell
uv run python scripts/verify_infrastructure.py
uv run python scripts/verify_pipeline.py
```

`verify_infrastructure.py` checks Kafka metadata, `SELECT 1` on PostgreSQL, an authenticated ClickHouse `SELECT 1`, and an authenticated MinIO bucket list. It does not insert business or warehouse data.

`verify_pipeline.py` publishes one synthetic order, waits until it is stored, publishes that same `event_id` again, and checks that PostgreSQL still has one row and the stats count did not move. It then publishes an invalid record and a following valid order to show the consumer kept going.

Direct checks:

```powershell
curl http://localhost:8000/health/live
curl http://localhost:8000/health/ready
curl http://localhost:8000/api/v1/ingestion/stats
curl "http://localhost:8000/api/v1/events/recent?limit=20"
```

Open http://localhost:3000 for the overview. Data Sources and Streaming read the same API. The pages poll every 5 seconds.

After pulling Day 2, rerun migrations:

```powershell
docker compose run --rm --build migrate
```

Publish one order through the API:

```powershell
uv run python scripts/verify_day2.py
```

Day 3 Spark jobs are on the `processing` profile and are documented in [docs/spark-lake.md](docs/spark-lake.md). Day 4 warehouse publication is documented in [docs/warehouse.md](docs/warehouse.md). Day 5 quality checks, monitoring, and demand forecasts are documented in [docs/quality-ml.md](docs/quality-ml.md).

```powershell
uv sync --group orchestration
$env:PYTHONPATH = "pipelines/warehouse/src;pipelines/dagster/src"
uv run --group orchestration python -m titan_dagster
```

That materializes Silver, the ClickHouse load, the dbt tests, and the publication pointer. The hourly schedule is off until the daemon is started:

```powershell
$env:DAGSTER_HOME = (Resolve-Path "pipelines/dagster/home")
uv run --group orchestration dagster-daemon run -w pipelines/dagster/workspace.yaml
```

The optional Compose profile is `docker compose --profile orchestration up -d dagster-daemon`. It is not required for a manual run.

```powershell
docker compose run --rm --build migrate
uv run python scripts/verify_day3.py
uv run python scripts/verify_day4.py
uv run python scripts/verify_day5.py
```

The Day 5 script publishes a 28-day synthetic history when the warehouse has fewer than 21 distinct item dates, then trains from the published warehouse. The short demo generator is still `--count`. Do not use `--history-days` for that demo. Day 6 recovery and the forecast serving rule are documented in [docs/reliability.md](docs/reliability.md).

```powershell
uv run python scripts/verify_day6.py
```

## Tests and static checks

```powershell
uv run ruff check
uv run ruff format --check
uv run mypy
uv run pytest -m "not integration"
```

With the stack already running:

```powershell
uv run pytest -m integration
```

From `apps/web`:

```powershell
npm ci
npm run lint
npx tsc --noEmit
npm run build
```

Integration tests are skipped unless Kafka, PostgreSQL, and the API are listening. CI runs the unit tests and the frontend build. It does not start this Compose stack.

## Troubleshooting

- **Kafka timeout from Windows.** Host code must use `localhost:9092`. Containers must use `kafka:19092`. The controller port is not published.
- **API readiness stays 503.** `migrate` has not finished or Postgres is unhealthy. `docker compose logs migrate postgres`.
- **Consumer unhealthy.** The heartbeat file is only updated while the loop is running. `docker compose logs ingestion`.
- **Port already allocated.** Something else owns 5435, 9092, 8123, 9000, 9001, 8000, or 3000. Titan publishes PostgreSQL on 5435 because 5432 was already a local PostgreSQL on the verification machine. Change `POSTGRES_HOST_PORT` and the host `DATABASE_URL` together.
- **Stale Kafka volume.** `docker compose down -v` deletes local data, including the formatted KRaft directory. The cluster id is fixed for a given volume.
- **MinIO image.** The pin is a community mirror because the official registry images could not be pulled anonymously. Do not treat it as a production image.
- **Dashboard cannot reach the API.** The browser uses `NEXT_PUBLIC_API_BASE_URL=http://localhost:8000`, not the Docker service name.

## Layout

```text
apps/api            FastAPI read API
apps/web            Next.js operations console
services/event-generator
services/ingestion  Kafka consumer
packages/contracts  event schema
packages/store      models and Alembic
pipelines/spark     Bronze streaming, Silver batch, and lake checks
pipelines/warehouse ClickHouse load and publication pointer
pipelines/dagster  asset order, retries, and the hourly schedule
warehouse/dbt      ClickHouse analytical models
infrastructure/docker
```

Model training is not part of this repository.
