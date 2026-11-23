# TITAN Kafka/PostgreSQL Integration CI

Upgrade 3A adds a dedicated GitHub Actions job in `.github/workflows/integration-ci.yml`.

## Architecture

The job runs on `ubuntu-latest` with the repository's pinned Python 3.12/uv environment. Docker Compose starts only the services needed for the first real integration path:

- Apache Kafka `4.4.0`
- PostgreSQL `18.7-alpine3.23`
- TITAN migration job
- TITAN ingestion consumer image built from `infrastructure/docker/python.Dockerfile`
- TITAN API image built from the same Dockerfile

The job uses project name `titan-ci` and the CI-only ports 19092, 15435, and 18000. Compose named volumes are therefore `titan-ci_*`; no developer `.env`, development ports, or development volumes are used. The CI password and admin token are disposable literals, not repository or GitHub secrets.

The Kafka override advertises `localhost:19092` on the host listener and `kafka:19092` on the Compose listener. This is required because Kafka redirects clients after bootstrap; leaving the base `localhost:9092` advertisement would send host test producers to the development broker.

## Scenarios and assertions

`tests/integration/test_pipeline.py` invokes `scripts/pipeline_flow.py`, which:

1. Refuses an empty or `titan` Compose project and refuses development endpoints.
2. Confirms migrations created `commerce_events` and `ingestion_quarantine`.
3. Publishes a valid event and waits for its durable PostgreSQL row.
4. Publishes the same event ID again and verifies one business row plus committed Kafka progress.
5. Publishes malformed data and verifies durable quarantine.
6. Publishes a new event ID with the same business key and verifies conflict classification without increasing logical orders.
7. Publishes a follower event and verifies recovery/progress.

The workflow runs this suite once, restarts only the CI ingestion container, waits for its health check, and runs the suite again with fresh UUIDs. This verifies restart recovery without relying on arbitrary sleeps. All waits are bounded at 30–60 seconds in the integration helpers; the complete job has a 20-minute timeout.

## Local reproduction

From `D:\TITAN` in PowerShell, use a disposable project name and the CI override:

```powershell
$env:COMPOSE_PROJECT_NAME = "titan-ci-local"
$env:COMPOSE_FILE = "docker-compose.yml;.github/docker-compose.integration.yml"
$env:POSTGRES_PASSWORD = "titan_ci_password"
docker compose --project-name $env:COMPOSE_PROJECT_NAME -f docker-compose.yml -f .github/docker-compose.integration.yml up --build --detach --wait kafka postgres migrate ingestion api

$env:DATABASE_URL = "postgresql+psycopg://titan:titan_ci_password@127.0.0.1:15435/titan"
$env:KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:19092"
$env:API_BASE_URL = "http://127.0.0.1:18000"
uv run pytest -m integration -q --tb=short
docker compose --project-name $env:COMPOSE_PROJECT_NAME -f docker-compose.yml -f .github/docker-compose.integration.yml restart ingestion
docker compose --project-name $env:COMPOSE_PROJECT_NAME -f docker-compose.yml -f .github/docker-compose.integration.yml up --detach --wait ingestion
uv run pytest tests/integration/test_pipeline.py -q --tb=short

docker compose --project-name $env:COMPOSE_PROJECT_NAME -f docker-compose.yml -f .github/docker-compose.integration.yml down --volumes --remove-orphans
```

Never substitute project name `titan`, development ports, or a development `.env`. The cleanup command is safe for this explicitly disposable project only.

## Diagnostics and limitations

Failure diagnostics collect Compose status and the last 200 lines from Kafka, PostgreSQL, and ingestion before cleanup. Dependency caching is provided by `setup-uv`; Docker layer caching is left to the hosted runner because the job builds only the two small Python images it needs. The job does not exercise ClickHouse, Spark, MinIO, dbt, or the web UI; those remain separate pipeline checks. Hosted CI execution has not been claimed until GitHub Actions runs this workflow.

Expected execution time is approximately 4–8 minutes on a warm GitHub-hosted runner, depending mainly on Docker image and uv cache state.
