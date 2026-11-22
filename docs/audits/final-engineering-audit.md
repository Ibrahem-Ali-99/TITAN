# Final engineering audit

Audit date: 2026-11-22. Starting commit: `3e1ce831a26a87973e8c9637367e6335769c56f0`. Workstation: Windows, Docker Compose project `titan`. Live development volumes were not reset. No push. `.cursor/mcp.json` was not committed.

**Verdict: CONDITIONAL.** The executed slice found no remaining Critical or Major defect in current code. Three defects were fixed and covered by tests. Several acceptance gates were not executed and stay **NOT VERIFIED**. Historical `pipeline_runs` rows were not rewritten.

## Architecture inventory

Operational path in code and the running stack:

synthetic, REST, or CSV event → Kafka → ingestion → PostgreSQL `commerce_events` → Spark Bronze (Kafka evidence) and Silver (PostgreSQL-authoritative) → MinIO Parquet → ClickHouse → dbt models → Dagster job order → FastAPI → Next.js.

Spark does not write `commerce_events`. Quality and demand training write their own metadata. Dagster orders jobs and does not transform rows. Delivery is at-least-once with idempotent persistence. This audit does not claim end-to-end exactly-once semantics.

Default Compose services observed `Up` and healthy, except `web` which has no healthcheck and was restarted for this audit: `api`, `clickhouse`, `ingestion`, `kafka`, `minio`, `postgres`. Spark, the Dagster daemon, and the event generator are profile-gated and were not running.

Implemented event types in PostgreSQL: `order_created`, `payment_completed`, `inventory_updated`. Line items are columns on the order, stored in `wh_order_items`. Customers are derived from `customer_id` on orders (`dim_customers`). There is no customer master feed and no website-activity event type. That narrowing is the implemented domain.

## Requirements traceability

| Capability | Status |
| --- | --- |
| REST, CSV, and synthetic ingestion | Implemented. Live stats below. CSV and REST counts are broker acknowledgements, not PostgreSQL persistence. |
| Kafka consume, quarantine, duplicate delivery | Implemented. `kafka_consumed_events` = persisted + quarantined + duplicate deliveries (751 + 16 + 26 = 793). |
| Idempotent orders | Implemented for the live snapshot: 383 `order_created` rows = 362 accepted + 16 conflict + 5 duplicate operation. Conflicts are not extra orders. |
| Replay | Partially verified. Stats show 1 published and 1 refused replay. A new replay was not executed against the live stack. |
| Late events and watermarks | Implemented in Spark policy: streaming windows may drop late rows; batch inclusion keeps every accepted order. Not re-executed in Spark this session. |
| Bronze, Silver, Gold | Implemented. Serving publication `50d22fe5-8db0-4f2f-bc00-47bda48152fc` points at Silver `7e8d34e7-c701-4084-8d56-edc040a5fe59`. |
| ClickHouse and dbt | Serving `fct_orders` is 362. Raw `wh_*` tables retain every staged publication, including failed attempts. dbt `build` was not run this session. |
| Dagster | Workflow validates definitions in CI. The daemon was not running. `dagster definitions validate` was not executed on this workstation in this session. |
| Quality gate | Latest serving load `e60d6010-bad3-4efa-bf4d-d22b0be66cae` has `quality_gate` succeeded, 240 ms. |
| Forecasting | Implemented. Serving run uses seasonal naive because the candidate model is worse than that baseline. |
| Monitoring | Implemented, with the lifecycle defect below. |
| Eight dashboard pages | Implemented. Smoke at 1440 and 390 passed. 1024 and 768 were not opened. |
| CI | Workflow exists and now runs `npm test`. A hosted GitHub run of this commit was not executed. |
| Benchmark | The README 5,000-ack measurement was not repeated. This session measured API latency only. |

## Reconciliation (read-only, 2026-11-22)

PostgreSQL `commerce_events`:

| event_type | disposition | count |
| --- | --- | --- |
| inventory_updated | accepted | 175 |
| order_created | accepted | 362 |
| order_created | conflict | 16 |
| order_created | duplicate_operation | 5 |
| payment_completed | accepted | 193 |

`GET /api/v1/warehouse/status` (one call, then a separate 10-call timing sample): `ready=true`, warehouse and ClickHouse publication `50d22fe5-8db0-4f2f-bc00-47bda48152fc`, silver `7e8d34e7-c701-4084-8d56-edc040a5fe59`, `published_at=2026-11-22T17:35:14.822790Z`, `freshness_seconds=10817` at query time, orders 362, payments 193, inventory 175, items 540. Revenue is separate by currency: EUR 18711.58 (120 orders), GBP 16558.03 (116), USD 17768.77 (126). 120+116+126=362. Currencies were not added.

ClickHouse, filtered to that publication: `wh_orders` 362, `wh_order_items` 540, `wh_payments` 193, same three currency sums. `fct_orders` 362. `wh_active` has 1 row. Unfiltered `wh_orders` is 6517 because earlier publications, including failed staging attempts, remain in the table. The serving models filter on the active publication id. The unfiltered total is retained history, not the dashboard total.

A tested-but-not-published load `1a57fbc9-11f1-4ef3-a43c-66bd5f328f1a` also has 362 staged orders. The serving pointer stayed on `50d22fe5`. Failed loads such as `566148a4-cc61-4421-a9c7-402e2a778263` left 361 staged orders from an earlier Silver snapshot. They are not the active publication.

`GET /api/v1/ingestion/stats`: persisted events 751, persisted `order_created` 383, quarantined 16, kafka consumed outcomes 793, logical orders 362, conflicts 16, duplicate operations 5, duplicate deliveries 26, accepted payments 193, inventory updates 175, REST published 106, REST rejected 1, CSV accepted 4, CSV rejected 4, CSV publish failed 0, replays published 1, replays refused 1. `most_recent_ingested_at=2026-11-22T17:33:29.104034Z`. `recent_activity` was empty inside the 60-minute window.

ML `GET /api/v1/ml/models?limit=5` (earlier in this audit): serving run `3a3f7264-818c-4e85-9b56-4793d68157ef`, strategy `seasonal_naive`, algorithm `HistGradientBoostingRegressor`, `better_than_baseline=false`. Candidate MAE 2.5971 versus seasonal-naive MAE 1.469388 on 49 test rows. Training publication `a44926d4-0d7d-4a30-857d-d543341475a0` (259 orders), which is an older published load, not the current 362-order snapshot. That is lineage. The candidate is retained and not served.

## Monitoring

`GET /api/v1/monitoring/jobs` selects `max(pipeline_runs.id)` per `job_name`. For this append-only writer, that is the latest inserted attempt. It is not grouped by publication. Scope comparison uses `warehouse_publication_id` or `publication_id` from metadata. A success of another job, or another publication scope, does not recover a failure. `warehouse_load` rows generally have null metadata, so they share one scope. Parsing free-text detail UUIDs was not added: `quality_gate.detail` is a load id and would split a real recovery.

Live latest `warehouse_load` is id 137, `state=running`, `started_at=finished_at=2026-11-22T17:34:50.248681Z`, detail `staging 50d22fe5-8db0-4f2f-bc00-47bda48152fc`. The publication itself succeeded under other job names (`clickhouse_load` 138, `warehouse_models` 139, `quality_gate` 140, `analytics_ready` 141). Those successes do not rewrite the `warehouse_load` row. Five `warehouse_load` failures (`_count()` TypeError, and one crash after the ClickHouse pointer exchange) are historical. The latest attempt for that job is the running marker, so the UI labels them `Historical failure · Recovery unknown` and does not count them as active incidents.

`max(id)` can diverge from `started_at` if a later insert is backdated. The success writer sets `started_at` when the attempt starts and inserts the terminal row after the running row, so id order matches attempt order for this writer. That coupling is a residual assumption, not a proof for every future writer.

## Defect register

### AUD-001 — Major — running attempts stored as finished

- Component: `pipelines/warehouse` `recorded_run` / `_record`, `pipelines/spark` `pipeline_finished_at` / `record_job`.
- Evidence: `warehouse_load` id 137 and 134 have `state=running` and `started_at = finished_at`. Counts for that job: succeeded 15, failed 16, running 36. Both writers assigned `finished_at` for every state, including `running`.
- Root cause: the insert treated the recording clock as a completion time.
- Reproduction: read those rows. New running reports from the old functions would set `finished_at`.
- Change: a `running` state now stores `finished_at` and `duration_ms` as null. Terminal states still close.
- Regression: `tests/unit/test_run_lifecycle.py`.
- Verification: `uv run pytest -m "not integration" -q` → 55 passed, 1 deselected.
- Remaining risk: historical running rows still have `finished_at`. They were not updated. Monitoring classifies `running` plus a finish time as `Status unknown`, which remains the honest label for those old rows. See AUD-005.

### AUD-002 — Minor — invalid metadata shared the empty publication scope

- Component: `apps/api` `_publication_scope`.
- Evidence: JSON that failed to parse, or parsed to a non-object, returned null, the same scope as a row with no metadata.
- Root cause: the exception path collapsed every bad payload into the unscoped bucket.
- Change: invalid metadata now gets `invalid-metadata-` plus the first 12 hex characters of SHA-256 of the raw string. Distinct bad payloads do not match each other or the empty scope.
- Regression: `test_invalid_metadata_does_not_share_the_unscoped_publication`.
- Verification: included in the 55 passed tests.
- Remaining risk: two identical invalid strings still share a scope. That is the same payload, not a merge of unrelated JSON.

### AUD-003 — Minor — service health said Ready while the latest job was unresolved

- Component: `apps/web` `platformHealth` and the monitoring dashboard.
- Evidence: warehouse `ready=true` and zero active failures produced the label `Ready` while latest `warehouse_load` was `running` with a finish time (`Status unknown`).
- Root cause: the pill used warehouse readiness alone once active failures were zero.
- Change: when the warehouse is ready and any latest job is not `succeeded`, or the latest-job request failed, the pill is `Warehouse ready`. Unknown warehouse readiness stays `Unknown`. A failed latest attempt stays `Degraded`.
- Regression: `incidents.test.ts` expects `Warehouse ready` when history is unresolved and `Ready` only when `latestUnresolved` is false.
- Verification: `npm test` → 10 passed. Production `next build` generated 11 static routes, including `/monitoring`. After restarting `web`, the monitoring page at 1440px showed `Warehouse ready`, `Recovery unknown`, and no `unresolved` text. Console errors: none.
- Remaining risk: the warehouse KPI on the same page still says `Ready` because that tile is the publication flag. That is the warehouse, not full platform health.

### AUD-004 — Suggestion — pipelines history recovery uses the 20-row window

- Component: `apps/web` `pipelines-dashboard.tsx` `hasLaterSuccess` against `recent_runs` (`limit 20` by id).
- Evidence: a failed row is labeled `Failed` unless a later success of the same job is inside that window. The monitoring page uses `/monitoring/jobs` and does not.
- Why it is not a defect fix in this change: the row state `failed` is the stored outcome of that attempt. For the current newest-20 window, the latest `warehouse_load` is the running marker, so those failures are not claimed as recovered. A success that exists only outside the window and only by `started_at` (not id) could be missed. No such row was demonstrated.
- Change: none.
- Remaining risk: the pipelines page is attempt history. Incident authority is the monitoring page.

### AUD-005 — Suggestion — historical running rows still have a finish time

- Component: PostgreSQL `pipeline_runs`.
- Evidence: id 137 and 134 remain `closed_immediately=true` after the code fix. 36 `warehouse_load` running rows were counted.
- Proposed migration, not applied:

```sql
UPDATE pipeline_runs
SET finished_at = NULL, duration_ms = NULL
WHERE state = 'running' AND finished_at IS NOT NULL;
```

- Consequence: monitoring would then age those rows and label them `Stale` after 15 minutes instead of `Status unknown`. Applying it rewrites history. It was not run.

### AUD-006 — Minor — ruff format check failed on committed files

- Component: `apps/api/src/titan_api/warehouse.py`, `pipelines/warehouse/src/titan_warehouse/recovery.py`, `scripts/benchmark_ingestion.py`.
- Evidence: `uv run ruff format --check .` reported those three files. `ruff check` and `mypy` (78 files) were clean before the format.
- Root cause: wrapping that the pinned formatter collapses.
- Change: `ruff format` on those files only. No behavior change.
- Verification: `uv run ruff format --check .` then passed (127 files).

### AUD-007 — Suggestion — incident tests were absent from CI

- Component: `.github/workflows/ci.yml` web job.
- Change: `npm test` runs after `npm ci` and before lint.
- Verification: local `npm test` passed. A GitHub-hosted run of the changed workflow was not executed.

## Tests and commands

| Command | Result |
| --- | --- |
| `uv run pytest -m "not integration" -q` | 55 passed, 1 deselected, 1 Starlette `httpx` TestClient deprecation warning |
| `uv run ruff check .` | passed |
| `uv run ruff format --check .` | passed after AUD-006 |
| `uv run mypy` | 78 files, no issues |
| `uv run python scripts/scan_secrets.py` | exit 0 |
| `docker compose config --quiet` | exit 0 |
| `docker compose run --rm --no-deps web npm test` | 10 passed. Node warned `MODULE_TYPELESS_PACKAGE_JSON`. `type: module` was not added. |
| `docker compose run --rm --no-deps web npm run build` | Next.js 16.4.6, 11 static routes |
| `npm audit` inside the web image | 0 vulnerabilities across 475 dependencies |
| `pytest -m integration` | **NOT VERIFIED** (deselected; requires the stack and was not run) |
| Spark profile tests, `dbt build`/`dbt test`, `dagster definitions validate` | **NOT VERIFIED** |
| Hosted GitHub Actions for this commit | **NOT VERIFIED** |

## Browser

Headless Chrome, routes `/`, `/data-sources`, `/streaming`, `/pipelines`, `/warehouse`, `/data-quality`, `/ml`, `/monitoring`.

At 1440×1100 and 390×844: HTTP 200, expected `h1`, 8 primary nav links, `documentElement` did not overflow, no console or page errors after hydration settled.

Monitoring after the web restart, 1440px: service health `Warehouse ready`; copy includes `Recovery unknown`; the word `unresolved` is absent; console errors empty. Widths 1024 and 768 were not opened. Figma frames were not re-fetched in this session. Keyboard, contrast, and focus were not measured with an accessibility scanner.

## Performance

Environment: the Compose stack above, API on localhost:8000, measured with `Invoke-RestMethod` wall clock. This is not a throughput benchmark and not one million events.

| Endpoint | Sample | min | p50 | max |
| --- | --- | --- | --- | --- |
| `GET /api/v1/warehouse/status` | 10 sequential calls | 30 ms | 32 ms | 85 ms |
| `GET /health/ready` | 10 sequential calls | 46 ms | 48 ms | 52 ms |

`/health/ready` returned `status=ready` and `checks.postgres=ok`. Kafka consumer lag shown on the monitoring page was 0 for `titan-ingestion`. End-to-end ingest latency, Spark duration, and a new producer-ack benchmark were not measured this session. The README 5,000-record ack figure remains that earlier measurement.

## Security

`scripts/scan_secrets.py` exited 0. No secret values are recorded here. Compose publishes workstation ports and uses the documented local database passwords. That is local development configuration, not a public deployment. Admin replay and CSV resume stay fail-closed when `TITAN_ADMIN_TOKEN` is unset. CORS is local. No credential rotation and no public deploy were performed. Dependency audit of the web tree reported no vulnerabilities. Python dependency CVE scanning beyond the secret scan was not run.

## Release gates

| Gate | Result |
| --- | --- |
| Data flow matches the live snapshot | Passed for the read-only reconciliation above |
| No unresolved Critical, Major, or Minor in the executed slice | Passed for AUD-001, AUD-002, AUD-003, AUD-006. Historical rows were left in place on purpose. |
| Financial reconciliation | Passed, currencies separate |
| Replay and restart | Passed on the disposable `titan-e2e` project. See `docs/audits/end-to-end-reliability.md`. |
| Spark hard-kill recovery | **PASSED.** `docker kill --signal=KILL` during a Bronze microbatch on project `titan-sparkkill`, then a consistent restart. See `docs/audits/spark-crash-recovery.md`. |
| Automated suites that were executed | Passed. Isolated integration pytest 1 passed. Spark image tests 11 passed. dbt tests ran inside publication (22 tests, the injected `promotion_gate` failure was the expected 1 error). Dagster materialize and `definitions validate` passed. |
| Eight pages | Passed at 1440 and 390. 1024 and 768: **NOT VERIFIED** |
| Accessibility | **NOT VERIFIED** beyond console, overflow, and nav presence |
| Monitoring semantics | Passed on the live page for the warehouse-ready and recovery-unknown labels |
| Security blockers | None found in the executed checks |
| Docker startup from a clean clone | **NOT VERIFIED**. The already-running stack stayed up. `docker compose config --quiet` passed. |
| Performance evidence | Passed as the latency sample above. Not a capacity test. |
| Documentation matches the implementation | This file records the narrowed domain and the unverified gates |
| Git diff reviewed | The commit for this audit contains the lifecycle fix, the scope and health-label changes, the formatter wrap, CI `npm test`, and this report |

## Reliability follow-up

`docs/audits/end-to-end-reliability.md` records the isolated execution. Two publication defects found there are fixed: a direct warehouse command now records `warehouse_models`, and recovery treats a missing ClickHouse fact group as zero rows. The live development database stayed at 362 accepted orders. Verdict remains **CONDITIONAL** because a clean clone, hosted GitHub Actions, viewport widths 1024 and 768, and an accessibility scan were not part of that run.

`docs/audits/spark-crash-recovery.md` records the later hard-kill. The driver container was SIGKILLed after the Bronze Parquet copy and before the batch commit. Restart on the same checkpoint republished the same 5 orders, 10 line items, and currency-separated revenue. The live accepted-order count stayed 362.

## Git

Local branch only. Nothing was pushed. `.cursor/mcp.json` stays untracked.
