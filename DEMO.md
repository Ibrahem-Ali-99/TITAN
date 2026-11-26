# Demonstration

The demo is synthetic. It adds one new order, one matching payment, one inventory update, a duplicate delivery of that order, a conflicting payload for the same order, and one invalid Kafka record. It then runs Bronze, Silver, a failing quality gate, and a Dagster publication of the candidate that passed.

A second run uses a new identifier. It does not reset volumes and it does not delete earlier rows.

## Before you start

From the repository root, with Docker Desktop running, follow the quick start in [README.md](README.md). A checkout that must not attach the development volumes uses `docker-compose.clone.yml` and `.env.clone.example` instead.

An empty database is not ready for this script. Publish the 28-day history and train the forecast first:

```powershell
uv run python -m event_generator --history-days 28 --seed 51022 --rate 0
docker compose --profile processing run --rm spark-bronze
docker compose --profile processing run --rm spark-silver
uv run --group orchestration python scripts/run_pipeline.py titan_warehouse
uv run --group orchestration python scripts/run_pipeline.py titan_ml
```

Wait until `/api/v1/warehouse/status` returns `"ready": true` and `/api/v1/ml/models` has one `serving` row. The development API is `http://localhost:8000`. An isolated checkout uses the `API_BASE_URL` in its `.env`.

Replay and CSV resume stay closed until `TITAN_ADMIN_TOKEN` is set and the API container is recreated. The demo publishes through the open REST ingestion route and does not call replay.

## Populate the live throughput charts

For a portfolio walkthrough, publish a short burst of unique orders, matching payments, and inventory updates through the real authenticated REST/Kafka path. It preserves existing rows and spaces arrivals across current UTC minute buckets:

```powershell
$env:API_BASE_URL = "http://localhost:18000"
uv run python -m scripts.demo_traffic --orders 8 --interval 6
```

The command waits for persistence and fails unless current activity buckets are returned. It is intentionally separate from the warehouse publication demo because Overview and Streaming visualize ingestion-time activity, while Warehouse visualizes the last published snapshot.

## Run

```powershell
uv run python scripts/demo_day7.py
```

The script exits non-zero when a stage fails. It writes `benchmarks/demo-day7.json` only after the new publication is serving.

## What the last successful run recorded

Measured by `scripts/demo_day7.py` on this workstation:

| Field | Value |
| --- | --- |
| Run token | `54943729` |
| Order | `ord_54943729demo` |
| Event | `1ef2981b-4510-55cb-8f8f-8ddfb6c2a10c` |
| Previous publication | `a44926d4-0d7d-4a30-857d-d543341475a0` |
| Serving publication | `50d22fe5-8db0-4f2f-bc00-47bda48152fc` |
| Silver publication | `7e8d34e7-c701-4084-8d56-edc040a5fe59` |
| Accepted orders in that publication | 362 |
| EUR | 18711.58 |
| GBP | 16558.03 |
| USD | 17768.77 |
| Serving forecast | `seasonal_naive` |
| Serving run | `3a3f7264-818c-4e85-9b56-4793d68157ef` |
| Training publication | `a44926d4-0d7d-4a30-857d-d543341475a0` |

The order count rose from 259 because accepted PostgreSQL orders that had not yet been published, including the earlier 100-event producer sample, entered this Silver version with the demo order. The duplicate delivery and the conflicting payload did not add a second accepted order. The injected quality failure left `a44926d4-0d7d-4a30-857d-d543341475a0` serving until the clean Dagster run.

The serving model was not retrained. Its training publication is still the earlier warehouse.

## Checks the script performs

1. API readiness and the current warehouse publication.
2. REST publication of the order twice, then the conflict, payment, and inventory event.
3. One invalid Kafka record.
4. Accepted-order count increases by one. Conflict and quarantine counters increase.
5. Spark Bronze, then Spark Silver.
6. Dagster load, dbt, and quality gate with `TITAN_QUALITY_INJECT=revenue_mismatch`. The publication id must stay put.
7. The same Dagster assets without the injection, including `analytics_ready`.
8. The API serves the new id, and `ord_54943729demo` is present at EUR 19.95.
9. The serving strategy and training publication id are unchanged.

Open http://localhost:3000 after the script. The warehouse page should show publication `50d22fe5-8db0-4f2f-bc00-47bda48152fc` and the three currency totals above. The forecast card should read `seasonal_naive`.
