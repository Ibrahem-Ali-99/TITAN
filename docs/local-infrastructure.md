# Local infrastructure

Docker Desktop with the WSL2 backend is the expected runtime. Give Docker at least **6 GB** of memory and **2 CPUs**. **8 GB** is the comfortable setting for this Compose file. Kafka is capped near 768 MB with a 512 MB heap, ClickHouse near 1.5 GB, and Postgres near 512 MB. The rest is the API, the consumer, MinIO, and the Next.js dev server.

This stack does not include Spark, orchestration, or a warehouse load. ClickHouse starts with an empty `titan` database and no tables. MinIO starts with no buckets. Their health checks only prove the process and, for the verification script, the configured credentials.

## Ports

| Service | Host port | Container address | Published? |
| --- | --- | --- | --- |
| Kafka clients on the host | 9092 | `127.0.0.1:9092` (`PLAINTEXT_HOST`) | yes, loopback only |
| Kafka clients in Compose | — | `kafka:19092` (`PLAINTEXT`) | no |
| Kafka controller | — | `kafka:29093` | no |
| PostgreSQL | 5435 | `postgres:5432` | yes, loopback only |
| ClickHouse HTTP | 8123 | `clickhouse:8123` | yes, loopback only |
| MinIO API | 9000 | `minio:9000` | yes, loopback only |
| MinIO console | 9001 | `minio:9001` | yes, loopback only |
| FastAPI | 8000 | `api:8000` | yes, loopback only |
| Next.js | 3000 | `web:3000` | yes, loopback only |

Host tools must use `localhost:9092`. Containers must use `kafka:19092`. A container that uses `localhost` reaches itself, not Kafka.

PostgreSQL is published on host port **5435** (`POSTGRES_HOST_PORT`). Containers keep using `postgres:5432`. The Day 1 workstation already had listeners on 5432, 5433, and 5434, and a published 5432 was answered by a local PostgreSQL with a different password. Host `DATABASE_URL` must use the same port as `POSTGRES_HOST_PORT`.

PostgreSQL 18 stores data under `/var/lib/postgresql`. The named volume is mounted there.

## MinIO image

The official `minio/minio` repository is no longer anonymously pullable from Docker Hub, and the pinned Quay tag used by older guides was not available from this machine. Compose uses the community image `alpine/minio:RELEASE.2025-10-15T17-29-55Z`, which does pull. Treat that as a local-dev supply-chain compromise, not as a production base image.

That image runs as the `minio` user. A fresh Docker named volume is owned by root, so the process exits with `file access denied` while creating `/data/.minio.sys`. Compose sets `user: "0:0"` so the local process can initialize the volume. This is not privileged mode and it is not a production hardening choice.

## Reset

`docker compose down` stops the `titan` containers and keeps the named volumes. `docker compose down -v` also deletes Kafka, Postgres, ClickHouse, MinIO, the Spark checkpoint, and Dagster data. The Kafka cluster id is fixed so a restart without `-v` reuses the formatted log directory.

Do not add `-v` unless that deletion is intentional.

## Compose memory limits

The file sets `name: titan`. A second checkout that runs `docker compose up` without another project name attaches the development volumes `titan_*`.

Limits in the Compose file: Kafka 768 MiB, PostgreSQL 512 MiB, MinIO 256 MiB, ClickHouse 1536 MiB, Spark 1536 MiB, ingestion 512 MiB, API 512 MiB, web 768 MiB, Dagster daemon 768 MiB. Spark and the daemon run only with their profiles. **8 GB** of Docker memory is the comfortable setting for this file. Host tools read `.env` from the repository root. Compose reads that file for interpolation. `COMPOSE_PROJECT_NAME` and `COMPOSE_FILE` must be set in the shell for the isolated overlay, because Compose does not take its file list from `.env`.

Profiles: `generate` for the containerized producer, `processing` for Spark, `orchestration` for the optional Dagster daemon. The daemon is off by default so the hourly warehouse job does not publish on its own. The protected/default orchestration profile has no Docker socket. Because the current Dagster assets launch Spark through Compose, a trusted workstation that needs containerized scheduled Spark must explicitly add `-f docker-compose.dagster-local.yml`; that override is never for shared or production-like deployments. The first `processing` command builds `titan-spark:4.0.1`.

Set `TITAN_READ_TOKEN`, `TITAN_WRITE_TOKEN`, and `TITAN_ADMIN_TOKEN` in the ignored `.env` file and recreate the API and web services before using the dashboard or protected operations. Empty protected tokens fail closed. The base Compose file binds every published development service to loopback; the dashboard proxy also requires `TITAN_DASHBOARD_LOCAL_ONLY=true`. These are local-development boundaries, not production authentication or secret management.

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
uv run python -m scripts.verify_infrastructure
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

## Local checks

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
uv run python -m scripts.verify_day7
uv run python -m scripts.demo_day7
$env:TITAN_WEB_BASE_URL = "http://localhost:3000"
uv run --with playwright python scripts/audit_a11y.py
```

`audit_a11y.py` opens the eight console routes at 1440×900, 1024×768, 768×1024, and 390×844 after the page has finished loading. Point `TITAN_WEB_BASE_URL` at the console you started. The clone console is `http://localhost:23000`. The first run needs Playwright Chromium (`uvx --from playwright playwright install chromium`).

`verify_day7.py` stops Kafka, checks that the warehouse status stays ready, starts Kafka, and runs reconcile. Day 2 through Day 6 scripts remain in `scripts/` for the earlier milestones.

The 5,000-event acknowledgement sample:

```powershell
$env:TITAN_BENCH_COUNT = "5000"
$env:TITAN_BENCH_SEED = "71022"
uv run python -m scripts.benchmark_ingestion
```

## Troubleshooting

- `docker compose up` in a second directory changes the development containers. The file sets `name: titan`. Export `COMPOSE_PROJECT_NAME` and `COMPOSE_FILE` before every Compose command in that shell.
- `verify_infrastructure.py` reports `FAIL postgres container ... is not inspectable` when the project name in `.env` does not match the containers that are running.
- A variable already set in the shell hides the same name in `.env`. The loaders use `setdefault`. Clear `DATABASE_URL`, `CLICKHOUSE_PORT`, and `S3_ENDPOINT` in that shell before the clone checks, or the commands talk to another project's ports.
- Postgres `localhost` can spend the whole connect timeout on IPv6. `.env.clone.example` uses `127.0.0.1` for `DATABASE_URL`. Kafka, ClickHouse, and MinIO stay on `localhost` with their clone ports.
- `/api/v1/warehouse/status` stays `ready: false` until Silver, dbt, and the quality gate have promoted a publication. `scripts/demo_day7.py` exits in that state. Run the first-publication commands in the repository README.
- Replay or CSV resume returns 401 when `TITAN_ADMIN_TOKEN` is empty. Set the placeholder, then `docker compose up -d --force-recreate api`.
- dbt or the warehouse command cannot reach ClickHouse when `CLICKHOUSE_PORT` is still `8123` while the clone publishes `28123`. Use `.env.clone.example` with the clone overlay.
- The first Spark command spends several minutes building `titan-spark:4.0.1`. Later runs reuse the image.
