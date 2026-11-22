# Warehouse publication

Published Silver Parquet is loaded into ClickHouse. dbt models read one publication. Dagster orders the jobs. FastAPI reads the serving views.

## Data model

`wh_orders`, `wh_order_items`, `wh_payments`, and `wh_inventory` are MergeTree tables. Every row carries `warehouse_publication_id`. Money is `Decimal(18, 2)` and keeps its currency code. Inventory rows are events. `inventory_latest` keeps the newest event per product and warehouse.

`wh_active` holds one pointer. Readers use it. A load that fails after the insert leaves orphan rows that the serving views do not see. Promotion is `EXCHANGE TABLES`, then a PostgreSQL row with status `published`. Those two writes are not one transaction. The API serves analytics only when the PostgreSQL published id equals `wh_active`.

An older Silver `finished_at` cannot replace a warehouse publication built from a later Silver version. Reloading the same Silver version creates a new warehouse id and the same business counts.

## Commands

From the repository root, with the Compose stack already up:

```powershell
docker compose run --rm --build migrate
uv sync --group orchestration
$env:PYTHONPATH = "pipelines/warehouse/src;pipelines/dagster/src"
$env:CLICKHOUSE_HOST = "localhost"
$env:S3_ENDPOINT = "http://localhost:9000"
uv run --group orchestration python -m titan_dagster
```

The default assets are `silver_publication`, `clickhouse_load`, `warehouse_models`, and `analytics_ready`. Bronze is a separate asset because Silver is built from PostgreSQL dispositions:

```powershell
uv run --group orchestration python -m titan_dagster bronze_evidence
```

A warehouse load without Dagster:

```powershell
uv run --group orchestration python -m titan_warehouse
```

## Schedule

`hourly_analytics` is `15 * * * *`. It does not run until a daemon is started:

```powershell
$env:DAGSTER_HOME = (Resolve-Path "pipelines/dagster/home")
uv run --group orchestration dagster-daemon run -w pipelines/dagster/workspace.yaml
```

`docker compose --profile orchestration up -d dagster-daemon` starts the same daemon in Compose. The container needs the Docker socket so a scheduled run can launch Spark. The manual command above is enough for a local publication.

A scheduled run loads the latest Silver version. The promote step refuses a candidate older than the current Silver snapshot or the current warehouse publication.

## Failure and retry

- Crash after the ClickHouse insert: the load is `failed`, `wh_active` stays on the previous publication.
- Failed dbt test: the load is not promoted. Set `TITAN_DBT_GATE=fail` only to exercise that gate.
- Retry the same Silver version with `python -m titan_warehouse`. Serving counts do not double.
- A newer Silver version is published by `spark-silver`, then the warehouse command.
- Do not start two promotions at once. The warehouse path holds PostgreSQL advisory lock `740002`. Silver holds `740001`.

Bronze readers use `bronze_batch_commits`. A prefix is visible after the copy returns and the commit row is written. Day 3 prefixes that already contain `_SUCCESS` are registered on the next Bronze start.

`pipeline_runs` keeps every recorded attempt. `spark_job_status` is still the latest row per Spark job.

## API

`GET /api/v1/warehouse/status` returns `ready: false` when nothing is published or the pointers differ. The analytics routes then return 503. They do not substitute an empty zero. Time filters are optional. Results are limited.

## Resources and limits

ClickHouse is limited to 1536 MB. Spark jobs are 1536 MB and run one at a time. The daemon is 768 MB and should not run beside a Spark job on an 8 GB Docker VM. dbt materializations are views. There is no incremental model.

Customer and product dimensions are ids observed on accepted orders and lines. The generator does not provide categories or demographics, and the models do not invent them.

The orchestration image is not part of the default `docker compose up`. Host `uv run --group orchestration` is the path `scripts/verify_day4.py` uses.
