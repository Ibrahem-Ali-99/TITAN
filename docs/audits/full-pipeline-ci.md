# Full-pipeline integration CI

Upgrade 3B adds `.github/workflows/full-pipeline-ci.yml`. It is a separate,
push-to-master and manually dispatched workflow so the ordinary pull-request
checks remain fast and unchanged.

The workflow uses a dedicated Compose project (`titan-full-ci`), ports 39092,
35435, 38123, 39000 and 38000, and disposable named volumes. Kafka, PostgreSQL,
ClickHouse, MinIO, the migration, ingestion and API services are started first.
The deterministic 14-day history fixture (seed 33031) is published twice to
exercise idempotency. Spark Bronze and Silver then run from the existing images,
and the existing warehouse publisher runs dbt, quality checks and atomic
promotion. The workflow always captures diagnostics and removes only its own
project and volumes.

Expected reconciliation is generated in `benchmarks/full_pipeline_ci.json`:
all generated event IDs persist once with `accepted` disposition, and order
revenue is compared by EUR, GBP and USD. The warehouse publisher additionally
checks Silver counts, line totals, duplicate event IDs, dbt tests, the quality
gate and the serving pointer. A rerun of the fixture is safe because the
PostgreSQL event ID and ClickHouse publication protections are exercised.

Local reproduction:

```powershell
$env:COMPOSE_PROJECT_NAME = "titan-full-ci-local"
$env:COMPOSE_FILE = "docker-compose.yml;.github/docker-compose.full-pipeline.yml"
docker compose --project-name titan-full-ci-local -f docker-compose.yml -f .github/docker-compose.full-pipeline.yml up --build --detach --wait kafka postgres clickhouse minio migrate ingestion api lake-init
uv run --group orchestration python scripts/full_pipeline_ci.py
docker compose --project-name titan-full-ci-local -f docker-compose.yml -f .github/docker-compose.full-pipeline.yml run --rm spark-bronze
docker compose --project-name titan-full-ci-local -f docker-compose.yml -f .github/docker-compose.full-pipeline.yml run --rm spark-silver
uv run --group orchestration python scripts/run_pipeline.py titan_warehouse
docker compose --project-name titan-full-ci-local -f docker-compose.yml -f .github/docker-compose.full-pipeline.yml down --volumes --remove-orphans
```

The hosted workflow has a 35-minute bound. It was not claimed as hosted-passing
until GitHub Actions executes it; local verification and hosted verification
are reported separately. The full path requires the Spark image build and is
therefore intentionally not part of the normal PR integration job.
