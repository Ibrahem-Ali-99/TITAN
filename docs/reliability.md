# Reliability, serving policy, and local security

Day 6 hardens the existing path. It does not add a second transaction manager, a metrics product, or an identity provider.

## Consumer offsets

The ingestion consumer persists or quarantines a record, then commits that offset. Automatic commits are off. A database error raises before the commit, so the same `event_id` can be delivered again. The primary key and the duplicate-delivery observation keep that from becoming a second accepted order.

`commit` refuses a partition that the rebalance callback has revoked. Prefetched records for a revoked partition are dropped from memory and stay uncommitted. During backoff the consumer pauses assignment, keeps polling so the session stays alive, and buffers at most 1000 already-fetched records. It does not seek those records.

A producer timeout or a message that is still buffered after `flush` raises `PublishError` with `uncertain=True`. That is not a definitive broker rejection. A definite error such as an unknown topic stays `uncertain=False`.

## Bronze and Silver

Readers use `bronze_batch_commits`. A prefix is visible only after the copy returns and the commit row is written. A partial object prefix without that row is not a completed batch. Reprocessing a batch uses a higher attempt, and a lower attempt cannot replace the commit row.

Silver publication still takes the advisory lock. An older Silver snapshot cannot replace a warehouse built from a later one.

A forced JVM `kill -9` during a microbatch was not executed. The tested Spark boundary remains checkpoint resume and the committed-batch table from the existing lake tests.

## Warehouse pointer

`wh_active` and `warehouse_loads` are not one transaction. `--crash-after exchanged` swaps the ClickHouse pointer and then fails before PostgreSQL marks the load published. The exception marks that load failed. `python -m titan_warehouse --reconcile` points `wh_active` at the newest PostgreSQL-published id that already has rows. It does not serve an unpublished candidate and it does not insert a second copy of the facts. The decision is a `warehouse_reconcile` pipeline row.

If no published id has rows, reconcile refuses and leaves the pointer. The API stays unavailable while the pointer and the newest published id disagree.

## Forecast serving

The target is daily quantity per product. The last seven dates are the test week. One-step evaluation uses lags from earlier actuals. The forward week is recursive: seasonal naive repeats the value from seven steps earlier, including its own previous prediction, and does not read a future actual.

`choose_strategy` serves `hist_gradient_boosting` only when its held-out MAE is strictly lower. Otherwise it serves `seasonal_naive` and keeps the candidate artifact and both metrics. The previous `promoted` or `serving` row becomes `evaluated`. It is not deleted. The Day 5 run remains in `ml_runs`.

The served one-step rows use the selected method. `candidate_one_step` keeps the scikit-learn predictions for comparison. The dashboard title is the serving strategy.

## Local security

`POST /api/v1/ingestion/replays` requires `Authorization: Bearer $TITAN_ADMIN_TOKEN` when that variable is set. When it is unset, replay is open. That is the local demo, not a control plane that is safe to expose on the public internet. REST event publication and CSV import stay open because they are the ingestion surface; they are still bounded by payload size and row limits. Monitoring reports `admin_auth` as `token` or `disabled`.

The development password `titan_dev_password` is a local placeholder. Ports are published for the workstation. Containers are not a locked-down production runtime.

## Benchmarks

`scripts/benchmark_ingestion.py` publishes a seeded burst and records producer acknowledgement time. The default count is 100. A 25,000-event tier was not run. The JSON file is the measurement. It is not a capacity claim.

## Checks that stay local

Spark image tests, dbt against a live ClickHouse, the Day 1–5 scripts, and `scripts/verify_day6.py` need the Compose stack. GitHub Actions runs ruff, mypy, unit tests, the secret scan, Dagster definition validation, and the web build. A green GitHub run is not claimed until one exists.
