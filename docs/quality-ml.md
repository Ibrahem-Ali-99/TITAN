# Quality, monitoring, and demand forecasts

Day 5 adds checks around an existing warehouse publication, durable run history, and one forecasting model. It does not add a rules platform, MLflow, Prometheus, or Grafana.

## Quality checks

Each execution is a row in `quality_runs`. Each check is a row in `quality_findings`. A finding records the check id, type, dataset, severity, observed value, expected value, pass or fail, the candidate publication, and a short diagnostic. A business identifier is stored when one check row can name it.

| Check | Severity | Blocks publication |
| --- | --- | --- |
| `orders.unique_order_id` | critical | yes |
| `orders.currency` | critical | yes |
| `items.positive_quantity` | major | yes |
| `orders.revenue_matches_lines` | critical | yes |
| `orders.silver_clickhouse_revenue` | critical | yes |
| `payments.matched_order_exists` | major | yes |
| `freshness.event_age_days` | informational | no |
| `freshness.warehouse_publication_age_seconds` | major when the active publication is older than 6 hours | no |
| `reconciliation.postgres_accepted_orders` | informational | no |

`orders.silver_clickhouse_revenue` compares per-currency sums of the candidate `wh_orders` rows with the revenue already stored on that warehouse load. Currencies are not added together. Money stays at two decimal places.

A payment with `order_matched = 1` must find its order in the same candidate. A payment with `order_matched = 0` is counted and does not fail the check. Partial or repeated payments are not required to equal the order total.

`freshness.event_age_days` is `now` minus the latest `occurred_at`. Historical synthetic orders are dated from 2026-01-01, so a large event age is expected and is not an ingestion failure. `freshness.warehouse_publication_age_seconds` uses `wh_active.published_at`. Ingestion freshness uses `ingested_at`. These clocks are UTC. The host zone is not used for the comparison.

The PostgreSQL gap is accepted orders minus candidate orders. Silver can lag the operational table between runs, so a positive gap does not block the candidate that was already loaded.

## Publication gate

`publish_load` runs the checks before `EXCHANGE TABLES`. A blocking failure marks that warehouse load failed, stores the findings, and leaves `wh_active` on the previous publication. Serving views keep reading that publication. A later successful load can still be promoted.

`TITAN_QUALITY_INJECT=revenue_mismatch` fails `orders.revenue_matches_lines` with the observed value `injected mismatch`. It does not update accepted orders or ClickHouse business rows. The finding says the rows were not modified. That run is an injected test fault, not an incident in the commerce data.

Direct `python -m titan_warehouse` uses the same gate. The Dagster `quality_gate` asset evaluates the tested load, and `analytics_ready` publishes it. The hourly analytics job includes both. Demand training is a separate daily job so it does not run beside every Spark publication.

## Monitoring

New warehouse and quality attempts store `started_at`, `finished_at`, and `duration_ms` when a start time was measured. Older rows are backfilled only when `finished_at` is later than `started_at`. A row whose timestamps are equal stays null. That is a point event, and the duration is not invented as zero.

`/api/v1/monitoring/overview` reports ingestion, Silver, and warehouse freshness from those pipeline timestamps. Ingestion lag is the sum of high watermark minus committed offset for the consumer group `titan-ingestion`. If the broker query fails, the lag is null and the detail names the failure. Spark progress is the latest `spark_job_status` row. It is not the consumer lag.

## Historical backfill

`generate_history` is a separate generator mode. It is not used by the short `--count` demo.

```powershell
uv run python -m event_generator --history-days 28 --seed 51022 --rate 0
```

The scenario starts at 2026-01-01, uses a deterministic seed, six catalog products, twelve customers, weekday demand factors, one line per order, a matching card payment twenty minutes later, and one inventory event per product per day. Event ids are `uuid5` values, so publishing the same seed again is a duplicate delivery and does not add accepted orders. `produced_at` is later than `occurred_at` on purpose. That delay is not an ingestion failure.

These events go through Kafka and the existing ingestion and warehouse path. There is no ML-only CSV. The demand pattern is programmed. A model can learn that pattern. That is not evidence about real commerce.

## Forecasting

The target is daily quantity sold per product, summed from `fct_order_items` of the active publication. Missing product-days inside the observed date range are filled with zero. Features are `lag_1`, `lag_7`, the mean of the previous up to seven days, the weekday, and a product code learned from the training rows only. The target day is not a feature.

The last seven distinct dates are the test range. Earlier rows are the training range. There is no shuffled split. The seasonal naive prediction is `lag_7`. The model is `HistGradientBoostingRegressor` with `max_depth=3`, `learning_rate=0.1`, `max_iter=80`, and `random_state=51022`. Metrics are MAE and WAPE. WAPE is omitted when the sum of absolute actuals is zero. MAPE is not used. `better_than_baseline` is true only when the model MAE is strictly lower on that same held-out week. The hyperparameters above are fixed. They are not searched against the test week.

One-step evaluation uses lags from earlier actuals and stores the actual quantity. The served one-step values follow the selected method. `candidate_one_step` keeps the scikit-learn predictions even when they are not served. The next seven days are recursive. Seasonal naive repeats the value from seven steps earlier, including a prediction it just appended. It does not read a future actual. The gradient-boosting forward path does the same with its own predictions. If a future actual is missing, the recursive step still continues from the appended prediction.

Training needs at least fourteen distinct dates. A shorter publication inserts `ml_runs.status = failed` and leaves the current serving row in place. A completed comparison is inserted as `status = serving` with `serving_strategy` of `seasonal_naive` or `hist_gradient_boosting`. The baseline is selected unless the candidate MAE is strictly lower. Earlier `promoted` or `serving` rows become `evaluated`. Their metrics and artifact URIs stay. The API and the forecast card label `serving_strategy`. The API does not load the joblib file.

Anomaly scores use the previous fourteen days only. The score is `0.6745 * (value - median) / MAD`. A day is flagged when the absolute score is above 3.5. A zero MAD leaves the score null and does not flag the day. These rows are statistical observations. They are not quality-contract findings. On this synthetic series, flags can be the programmed spikes. That is not a labeled production evaluation.

## Recovery

A failed quality load stays in `warehouse_loads` with status `failed`. Republish a newer tested load without `TITAN_QUALITY_INJECT`. The previous `wh_active` row remains the one the API serves until a later exchange.

A failed training run is visible in `/api/v1/ml/models`. The serving row and its predictions stay in place. Analytics do not depend on the model. The serving choice and the cross-database warehouse recovery are described in [reliability.md](reliability.md).

Local training uses the host `uv run --group orchestration` environment. The API image does not install scikit-learn. Expect the ClickHouse, Spark, and API memory limits already set in Compose. A 28-day backfill is enough for the chronological split. A million events are not required.

## Representative forecasting profile

The deep-audit demonstration workload is separate from the legacy `generate_history` mode and is reproducible through the real Kafka and warehouse path:

```powershell
docker compose --profile generate run --rm --build event-generator --profile forecasting --history-days 42 --seed 61226 --rate 0
```

It defines 12 stable products with weekday, weekend, steady, and payday patterns; seeded Gaussian variation; controlled intermittent zeros; EUR/GBP/USD prices; deterministic event IDs; matching payments; cumulative inventory; one duplicate; one conflict; and one malformed quarantine fixture. The 2026-11-26 live run persisted 544 accepted orders, 544 payments, 504 inventory updates, one duplicate operation, one conflict, and one schema-validation quarantine record from this profile. It was then processed by Bronze, Silver, warehouse publication, quality, and ML training. Forecast rows are produced by the model workflow and published with `kind=forecast`; they are never inserted directly into serving tables.

The current audited ML run has 624 training rows (2026-01-01–2026-11-15), 140 chronological evaluation rows (2026-11-16–2026-11-22), 20 products, and a seven-day future horizon (2026-11-23–2026-11-29). Seasonal naive wins fairly on the held-out week at MAE 1.657143/WAPE 0.320442; the retained HistGradientBoosting candidate measures MAE 15.590441/WAPE 3.014726. The earlier extreme candidate error came from the sparse legacy publication and its large one-day spike, not from changing evaluation rules to favor the baseline.
