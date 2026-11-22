# TITAN

TITAN is a local commerce data platform. It answers a narrow question: which synthetic orders were accepted, in which currency, and which published snapshot is safe to show.

Events are generated, posted over REST, or uploaded as CSV. They move through Kafka, a PostgreSQL classification, Spark Bronze and Silver on MinIO, a versioned ClickHouse warehouse, dbt tests, and a blocking quality gate. Dagster orders that publication. FastAPI and a Next.js console read the snapshot that both databases agree on.

The data is synthetic. Delivery is at-least-once with idempotent persistence. It is not end-to-end exactly-once.

## What it demonstrates

- Multi-source ingestion, quarantine, duplicate delivery, and business-key conflicts
- Offset commit after a durable PostgreSQL write
- Bronze evidence that is invisible until a commit row exists
- Versioned Silver and a ClickHouse publication exchanged only after dbt and quality checks
- Currency-separated revenue, with no cross-currency sum
- Baseline-aware forecasting: seasonal naive is served because its held-out MAE was lower
- A recovery decision that requires quality, dbt, Silver lineage, and matching fact counts
- A measured producer-acknowledgement sample, labeled as that and nothing broader

## Architecture

```mermaid
flowchart LR
  sources["Generator, REST, CSV"] --> kafka["Kafka"]
  kafka --> ingest["Ingestion"]
  ingest --> pg["PostgreSQL"]
  kafka --> bronze["Spark Bronze"]
  pg --> silver["Spark Silver"]
  bronze --> minio["MinIO"]
  silver --> minio
  silver --> ch["ClickHouse"]
  ch --> dbt["dbt and quality gate"]
  dbt --> api["FastAPI"]
  api --> web["Next.js"]
```

Dagster orders Silver, the ClickHouse load, dbt, the quality gate, and the pointer exchange. It does not transform rows. Details and the guarantee table are in [docs/portfolio/architecture.md](docs/portfolio/architecture.md).

| Piece | Responsibility |
| --- | --- |
| `packages/contracts` | Event envelope and money rules |
| `packages/store` | Operational tables and migrations |
| `services/ingestion` | Consume, classify, then commit |
| `pipelines/spark` | Bronze and Silver |
| `pipelines/warehouse` | ClickHouse load, exchange, reconcile |
| `warehouse/dbt` | Models and tests |
| `pipelines/dagster` | Asset order and schedules |
| `pipelines/quality` | Blocking publication checks |
| `pipelines/ml` | Demand training and the serving choice |
| `apps/api`, `apps/web` | Read models and the operations console |

## Prerequisites

- Windows PowerShell, Docker Desktop with at least 8 GB of memory, Python 3.12, and [uv](https://docs.astral.sh/uv/) 0.12.x (the images pin uv 0.12.1).
- The web image and the web CI job use Node.js 26.13.1. Host Node is required only for `npm test`, `npm run lint`, and `npm run build` outside Docker.
- Compose memory limits: Kafka 768 MiB, PostgreSQL 512 MiB, MinIO 256 MiB, ClickHouse 1536 MiB, Spark 1536 MiB, ingestion 512 MiB, API 512 MiB, web 768 MiB, Dagster daemon 768 MiB.
- No hidden environment variables. Host tools read `.env` from the repository root. Compose reads that file for interpolation. `COMPOSE_PROJECT_NAME` and `COMPOSE_FILE` must also be set in the shell when you use the isolated overlay, because Compose does not take its file list from `.env`.

The Compose file sets `name: titan`. A second checkout that runs `docker compose up` without another project name attaches the development volumes `titan_*`.

## Quick start

These commands start the development project. Skip them when that project is already running and use the isolated checkout below.

```powershell
copy .env.example .env
docker compose up --build -d
docker compose run --rm --build migrate
uv sync --all-packages
uv sync --group orchestration
uv run python scripts/verify_infrastructure.py
```

The API is http://localhost:8000 and the console is http://localhost:3000. Profiles: `generate` for the containerized producer, `processing` for Spark, `orchestration` for the optional Dagster daemon. The daemon is off by default so the hourly warehouse job does not publish on its own. The first `processing` command builds `titan-spark:4.0.1`.

```powershell
docker compose down
```

That stops the `titan` containers and keeps the named volumes. Do not add `-v` unless you intend to delete the development PostgreSQL, Kafka, MinIO, ClickHouse, Spark checkpoint, and Dagster data.

Set `TITAN_ADMIN_TOKEN` in `.env` and recreate the API before replay or CSV resume. `.env.example` already contains the local placeholder `local-demo-token`. An empty token makes those two calls return 401. REST publication and CSV upload stay open on localhost. This is a workstation boundary, not a public deployment.

Pinned images and the money rules are in [docs/local-infrastructure.md](docs/local-infrastructure.md) and [docs/money-policy.md](docs/money-policy.md).

## Isolated checkout

Use this when the development stack is already up, or when the demonstration must start from empty volumes. The checkout directory must not contain a copied `.env`, `.venv`, database dump, or `node_modules`.

```powershell
$env:COMPOSE_PROJECT_NAME = "titan-clone"
$env:COMPOSE_FILE = "docker-compose.yml;docker-compose.clone.yml"
copy .env.clone.example .env
docker compose up --build -d
docker compose run --rm --build migrate
uv sync --all-packages
uv sync --group orchestration
uv run python scripts/verify_infrastructure.py
```

`verify_infrastructure.py` checks Postgres, Kafka, ClickHouse, and MinIO on the ports in `.env`. For any project name other than `titan`, it also requires the Postgres container to mount `<project>_postgres_data` and refuses `titan_postgres_data`.

| Service | Clone host port |
| --- | --- |
| Kafka | localhost:29092 |
| PostgreSQL | localhost:25435 |
| ClickHouse | localhost:28123 |
| MinIO | localhost:29000 |
| API | http://localhost:28000 |
| Console | http://localhost:23000 |

Stop the clone without touching development volumes:

```powershell
$env:COMPOSE_PROJECT_NAME = "titan-clone"
$env:COMPOSE_FILE = "docker-compose.yml;docker-compose.clone.yml"
docker compose down
```

`docker compose -p titan-clone down -v` deletes only `titan-clone_*` volumes. Do not run `down -v` while `COMPOSE_PROJECT_NAME` is unset or `titan`.

## First publication

`scripts/demo_day7.py` adds one order to a warehouse that is already `ready` and leaves the serving forecast in place. A new database has neither. From the same shell as the quick start or the isolated checkout:

```powershell
uv run python -m event_generator --history-days 28 --seed 51022 --rate 0
docker compose --profile processing run --rm spark-bronze
docker compose --profile processing run --rm spark-silver
uv run --group orchestration python scripts/run_pipeline.py titan_warehouse
uv run --group orchestration python scripts/run_pipeline.py titan_ml
```

The generator publishes a deterministic 28-day backfill through Kafka. Spark Bronze and Silver run once. `titan_warehouse` loads ClickHouse, runs dbt, and promotes the publication only after the quality gate. `titan_ml` trains on that publication and serves `seasonal_naive` unless the candidate MAE is strictly lower. `scripts/run_pipeline.py` adds the pipeline source trees and loads `.env` before the module starts, so a new shell does not need a hand-built `PYTHONPATH`.

Wait until `GET /api/v1/warehouse/status` reports `"ready": true` before the demo.

## Demo

```powershell
uv run python scripts/demo_day7.py
```

The walkthrough is [DEMO.md](DEMO.md). The last successful run published warehouse `50d22fe5-8db0-4f2f-bc00-47bda48152fc` with 362 accepted orders: EUR 18711.58, GBP 16558.03, USD 17768.77. The order it added was `ord_54943729demo` at EUR 19.95. The larger jump from the earlier 259-order snapshot includes accepted rows that had been stored in PostgreSQL and not yet published.

`GET /api/v1/warehouse/status` after that run reported `ready: true` for that publication. The serving forecast remained `seasonal_naive` on run `3a3f7264-818c-4e85-9b56-4793d68157ef`, trained from publication `a44926d4-0d7d-4a30-857d-d543341475a0`.

## Verification

```powershell
uv run ruff check
uv run ruff format --check
uv run mypy
uv run pytest -m "not integration"
uv run python scripts/scan_secrets.py
docker compose --profile processing run --rm --no-deps spark-tests
uv run --group orchestration dbt build --no-partial-parse --project-dir warehouse/dbt --profiles-dir warehouse/dbt
$env:DAGSTER_HOME = (Resolve-Path pipelines/dagster/home)
$env:PYTHONPATH = "pipelines/warehouse/src;pipelines/quality/src;pipelines/ml/src;pipelines/dagster/src"
uv run --group orchestration dagster definitions validate -w pipelines/dagster/workspace.yaml
```

Live scripts, each of which needs the stack:

```powershell
uv run python scripts/verify_day7.py
uv run python scripts/demo_day7.py
$env:TITAN_WEB_BASE_URL = "http://localhost:3000"
uv run --with playwright python scripts/audit_a11y.py
```

`audit_a11y.py` opens the eight console routes at 1440×900, 1024×768, 768×1024, and 390×844 after the page has finished loading. Point `TITAN_WEB_BASE_URL` at the console you started. The clone console is `http://localhost:23000`. The first run needs Playwright Chromium (`uvx --from playwright playwright install chromium`).

`verify_day7.py` stops Kafka, checks that the warehouse status stays ready, starts Kafka, and runs reconcile. Day 2 through Day 6 scripts remain in `scripts/` for the earlier milestones.

GitHub Actions runs ruff, mypy, unit tests, the secret scan, Dagster definition validation, and the web build. A green hosted run is not claimed until one exists for the commit you are reading.

## Benchmark

`scripts/benchmark_ingestion.py` writes `benchmarks/day7-<count>.json`. The default count is 100. The 5,000-event sample below used an isolated topic so it would not create accepted orders.

```powershell
$env:TITAN_BENCH_COUNT = "5000"
$env:TITAN_BENCH_SEED = "71022"
uv run python scripts/benchmark_ingestion.py
```

Measured at `2026-11-22T17:31:28.666544+00:00` on an AMD Ryzen 7 9800X3D with 32 GB of RAM. Docker Desktop showed about 13.65 GiB for containers without an explicit limit. Other local containers were running. Kafka was 337 MiB of 768 MiB before the sample and 375 MiB after.

| Scope | Result |
| --- | --- |
| Topic | `titan.benchmark.acks` |
| Records | 5000, seed 71022 |
| Elapsed | 4.22 s |
| Acknowledged / second | 1184.964 |
| Ack p50 / p95 / p99 | 0.000598 s / 0.000843 s / 0.001013 s |
| `logical_orders` | 360 before and after |
| Ingestion lag | 0 before and after |

This is producer acknowledgement time. It is not warehouse freshness, not HTTP latency, and not a production capacity figure. An earlier 100-event sample on the commerce topics is in `benchmarks/day6-100.json` and did enter PostgreSQL. Do not describe either sample as sustained end-to-end throughput.

## Forecasts and security

Training uses daily quantity per product. The last seven dates are the test week. One-step rows use earlier actuals. The forward week is recursive and does not read a future actual. The gradient-boosting candidate is served only when its MAE is strictly lower. Otherwise the label is `seasonal_naive`, and the candidate stays in `ml_runs`. See [docs/quality-ml.md](docs/quality-ml.md).

Recovery, offset rules, and the admin token are in [docs/reliability.md](docs/reliability.md).

## Troubleshooting

- `docker compose up` in a second directory changes the development containers. The file sets `name: titan`. Export `COMPOSE_PROJECT_NAME` and `COMPOSE_FILE` before every Compose command in that shell.
- `verify_infrastructure.py` reports `FAIL postgres container ... is not inspectable` when the project name in `.env` does not match the containers that are running.
- A variable already set in the shell hides the same name in `.env`. The loaders use `setdefault`. Clear `DATABASE_URL`, `CLICKHOUSE_PORT`, and `S3_ENDPOINT` in that shell before the clone checks, or the commands talk to another project's ports.
- Postgres `localhost` can spend the whole connect timeout on IPv6. `.env.clone.example` uses `127.0.0.1` for `DATABASE_URL`. Kafka, ClickHouse, and MinIO stay on `localhost` with their clone ports.
- `/api/v1/warehouse/status` stays `ready: false` until Silver, dbt, and the quality gate have promoted a publication. `scripts/demo_day7.py` exits in that state. Run the first-publication commands.
- Replay or CSV resume returns 401 when `TITAN_ADMIN_TOKEN` is empty. Set the placeholder, then `docker compose up -d --force-recreate api`.
- dbt or the warehouse command cannot reach ClickHouse when `CLICKHOUSE_PORT` is still `8123` while the clone publishes `28123`. Use `.env.clone.example` with the clone overlay.
- The first Spark command spends several minutes building `titan-spark:4.0.1`. Later runs reuse the image.

## Known limits

- Synthetic data only. No claim about a real catalog or a real store.
- No public-internet authentication. Admin replay and CSV resume are fail-closed. Ingestion ports are still open on the workstation.
- A live two-member Kafka rebalance and a MinIO outage during a write were not executed. Spark `kill -9` recovery is recorded in [docs/audits/spark-crash-recovery.md](docs/audits/spark-crash-recovery.md).
- The console is a local Next.js app. Screenshot notes are in [docs/portfolio/screenshots.md](docs/portfolio/screenshots.md). Hosted GitHub Actions for a commit are not claimed until that run exists.
- The hourly Dagster schedule is defined and left stopped.

## Portfolio notes

Short descriptions for a resume, a case study, and a demo narration are in [docs/portfolio](docs/portfolio).
