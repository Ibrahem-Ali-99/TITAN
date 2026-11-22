# End-to-end reliability

Executed 2026-11-22 from commit `56a5e439fd5309b5b753bb12adccb0131fae618d`, then the fixes below. The development project `titan` was left running. Its accepted `order_created` count was 362 before the run and 362 after it.

**Verdict: CONDITIONAL.** The isolated path, replay, consumer restart, Spark availableNow resume, dbt, Dagster, and publication recovery ran and the two defects they exposed are fixed. This is not a release-ready claim. A clean clone, hosted GitHub Actions, and the earlier unverified browser widths remain **NOT VERIFIED**. The later Spark `kill -9` is recorded in `docs/audits/spark-crash-recovery.md`. Delivery is at-least-once with idempotent persistence. It is not exactly-once.

## Isolation

Project `titan-e2e`, files `docker-compose.yml` and `docker-compose.e2e.yml`.

| Resource | Isolated value |
| --- | --- |
| Postgres volume | `titan-e2e_postgres_data` (inspected mount) |
| Kafka volume | `titan-e2e_kafka_data`, host `localhost:19092` |
| ClickHouse | `titan-e2e_clickhouse_data`, host `localhost:18123` |
| MinIO | `titan-e2e_minio_data`, host `localhost:19000` |
| API | `http://localhost:18000` |
| Consumer group | `titan-ingestion` on this broker only |

`docker compose -p titan-e2e ... down` ran without `-v`. The `titan-e2e_*` volumes remain. The `titan_*` volumes were not removed or attached.

Command:

```powershell
uv run python scripts/verify_e2e_reliability.py
```

Continuation phases used `TITAN_E2E_FROM=publish` and `TITAN_E2E_FROM=remainder`. Raw scenario output is `docs/audits/e2e-evidence.json`.

## Workflow evidence

| Stage | Result |
| --- | --- |
| REST | 4 seeded orders, one repeated `event_id`, one conflicting payload. Logical orders did not increase for the repeat or the conflict. |
| Kafka duplicate | The same order was produced twice. One accepted row. `duplicate_deliveries` ended at 3, including the REST repeat and the later persisted replay. |
| Malformed | `not-json` on `titan.commerce.order-events` became quarantine id 1, reason `malformed_json`. |
| CSV | One valid EUR order `ord_e2ecsv000001` and one invalid date. Stats: accepted 1, rejected 1. |
| Replay | Exact replay of the malformed record: `refused`, `malformed_json`. Replacement replay: `published` and then `persisted`. Replay of an already accepted `event_id`: `published`, and the accepted order count stayed 1 for that order. |
| Consumer stop | An event published while ingestion was stopped was absent from PostgreSQL. After `start`, it persisted. Logical orders moved 7 → 8. |
| Integration pytest | `uv run pytest -m integration -q` against the isolated ports: 1 passed, 55 deselected. |
| Spark Bronze | First availableNow commit: batch 0, 17 records. Second run after one new event: batch 1, 1 record. The first batch was not rewritten as a second copy. |
| Running rows | `pipeline_runs` with `state = 'running' and finished_at is not null`: 0 on this project. |
| Spark Silver | Publication `45896d93-adf7-4491-811f-def96fa1991e`, status `published`, `silver_order_count` 11. |
| ClickHouse | Active pointer orders 11, items 24. Revenue EUR 1194.16, GBP 257.49, USD 256.19. Same three sums as accepted PostgreSQL envelopes. Late order `ord_f7ed54e3430d` (`occurred_at` `2026-01-01T12:00:00+00:00`) is one row in `fct_orders`. |
| dbt | The successful publication's dbt build completed. With `TITAN_DBT_GATE=fail`, dbt reported `PASS=21 ERROR=1` and `promotion_gate` failed. The serving pointer did not move. |
| Quality injection | `TITAN_QUALITY_INJECT=revenue_mismatch` failed `orders.revenue_matches_lines` and left the load `failed`. |
| Dagster | Materialize `silver_publication`, `clickhouse_load`, `warehouse_models`, `quality_gate`, `analytics_ready` exited 0 in about 39 seconds. Serving orders stayed 11 and the currency sums stayed the same. The publication id changed. `dagster definitions validate` exited 0. |
| Spark tests | `docker compose -p titan-e2e --profile processing run --rm spark-tests`: 11 passed in 31.70s. |

Accepted orders after the ingest and integration steps: 11, one accepted row per `order_id`, plus 1 conflict row. Currencies were not added together.

## Failure results

| Scenario | Outcome |
| --- | --- |
| Duplicate Kafka delivery | One accepted order. A second delivery is an observation, not a second order. |
| Duplicate business event | Same `event_id` did not add a logical order. A new `event_id` with a different total for the same order was stored as `conflict`. |
| Malformed and quarantine | Quarantine row stored. It is not an order. |
| Replay | Uncorrected JSON refused. Corrected replacement persisted. Replaying an accepted event did not double the order. |
| Consumer restart | Offset commit survived the stop. The held event was applied once. |
| Spark restart | A second `availableNow` process committed only the new record. Checkpoint `kill -9` during a microbatch was **NOT VERIFIED**. |
| Late event | The backdated order is in Silver and in `fct_orders`. |
| Crash after insert | Load failed in 634 ms. The active pointer stayed put. |
| Crash after exchange | ClickHouse moved to `f409eea7-7999-4070-807d-ed5f29761c07` and PostgreSQL marked that load failed. |
| Reconcile before the fix | Refused: `no verified publication is recoverable`. The API then reported the pointer mismatch and `ready: false`. |
| Reconcile after the fix | Moved the pointer back to `0e171bf1-0d3f-4c2f-bb7c-9f23f4d9287b`. API `ready: true`, orders 11, same currency sums. |
| dbt gate and quality gate | Both failed closed. They did not replace the restored pointer. |

## Monitoring

New `warehouse_load` running rows on this project have `finished_at` null (`running|t` from `finished_at is null`). Terminal rows have a finish time.

`GET /api/v1/monitoring/jobs` returned the newest `e2e_scope_probe` row: `succeeded`, scope `pub-b`. The older failure scoped to `pub-a` is not that latest job. `GET /api/v1/monitoring/runs?limit=1` returned the newer success, not the older failure. Historical failures were not rewritten.

## Defects fixed

### E2E-001 — Major — direct publication omitted the dbt job row

`python -m titan_warehouse` ran dbt and quality, exchanged the pointer, and marked the load published. It did not insert `pipeline_runs.job_name = 'warehouse_models'`. The API treats that missing row as "the active publication has no successful dbt run", so `ready` stayed false and order counts were withheld. `fct_orders` already had 11 rows.

`run_warehouse_pipeline` now records `warehouse_models` failed or succeeded around `_run_dbt`, using the load id the API already queries. The next direct publish on the isolated stack was `ready: true` with one matching `warehouse_models` success. Dagster's separate `test_load` path already recorded that job and was left in place.

### E2E-002 — Major — empty fact groups blocked recovery

`GROUP BY warehouse_publication_id` omits a table with zero rows. Recovery read that omission as `-1`. This stack had 11 orders, 0 payments, and 0 inventory, so `is_verified` rejected the otherwise complete publication and left the crashed pointer in place.

`fact_count` now uses 0 when the group is absent. `tests/unit/test_day6_policy.py` covers a publication with zero payments and inventory. After the change, reconcile restored the verified publication.

## Commands and counts

| Command | Result |
| --- | --- |
| `uv run pytest -m "not integration" -q` | 56 passed, 1 deselected |
| `uv run pytest -m integration -q` on isolated ports | 1 passed, 55 deselected |
| Spark image tests | 11 passed |
| `dagster definitions validate` | passed |
| Dagster materialize | exit 0, orders 11 before and after |
| Live `titan-postgres-1` accepted orders | 362 before and after |

Unit tests do not start the isolated stack. `scripts/verify_e2e_reliability.py` is the executed scenario runner.

## Still NOT VERIFIED

- Spark JVM `kill -9` during a microbatch.
- A clone started from an empty workstation with no existing images.
- Hosted GitHub Actions for this commit.
- Browser widths 1024 and 768, and an accessibility scan.
