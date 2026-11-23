# Full-pipeline integration CI

Upgrade 3B adds `.github/workflows/spark-image.yml` and
`.github/workflows/full-pipeline-ci.yml`. The image workflow is the only job
that prepares Spark. The integration workflow runs after a successful image
publication and never downloads the Spark distribution.

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

The image is published to GHCR as
`ghcr.io/<owner>/titan-spark:4.0.1-<source-commit>`. The integration workflow
pulls that tag once, resolves its registry-provided immutable digest, and
passes only `ghcr.io/<owner>/titan-spark@sha256:<digest>` to Compose. The image
is built from the pinned Eclipse Temurin 21 base, Spark 4.0.1, Hadoop 3.4.1,
AWS SDK bundle 2.24.6, Kafka 3.9.1, PostgreSQL JDBC 42.7.7 and Commons Pool 2.12.0.
Spark's archive SHA-512 and every Maven artifact SHA-1 are verified during the
image build. GHCR publishing uses only `contents: read` and `packages: write`;
the integration workflow has `contents: read` and `packages: read`.

Rebuild locally with a populated `.ci-cache/spark` directory containing the
Spark tarball and matching `.sha512` file, then run:

```powershell
docker buildx build --load --tag titan-spark:4.0.1 --file infrastructure/docker/spark.Dockerfile .
```

The cache directory is intentionally ignored and must never be committed.
The hosted image workflow is the reproducible publishing procedure.
