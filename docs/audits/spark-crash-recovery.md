# Spark hard-kill and checkpoint recovery

Executed 2026-11-22 from commit `2da8d069f50d9bf09640f860e7f1707fad077082`. The development project `titan` stayed up. Its accepted `order_created` count was 362 before the run and 362 after it (`docker exec titan-postgres-1 psql -U titan -d titan -tAc "select count(*) from commerce_events where event_type='order_created' and disposition='accepted'"`).

**Verdict: PASSED.** The Spark driver container was SIGKILLed during an in-progress Bronze microbatch, after the Parquet copy and before the batch commit. Restart on the same checkpoint and source produced one committed batch. PostgreSQL and ClickHouse then agreed on accepted orders, line items, and currency-separated revenue.

Delivery stays at-least-once. Business persistence stays idempotent. This run does not claim exactly-once.

## Isolation

Project `titan-sparkkill`, files `docker-compose.yml` and `docker-compose.e2e.yml`. The script refused to continue unless Postgres was mounted from `titan-sparkkill_postgres_data` and that mount was neither `titan_postgres_data` nor `titan-e2e_postgres_data`.

| Resource | Isolated value |
| --- | --- |
| Postgres | volume `titan-sparkkill_postgres_data`, host `localhost:15435` |
| Kafka | volume `titan-sparkkill_kafka_data`, host `localhost:19092` |
| ClickHouse | volume `titan-sparkkill_clickhouse_data`, host `localhost:18123` |
| MinIO | volume `titan-sparkkill_minio_data`, host `localhost:19000` |
| Checkpoints | volume `titan-sparkkill_spark_checkpoints` |
| API | `http://localhost:18000` |
| Offset audit group | `titan-sparkkill-audit` (watermark read only) |

`titan_*` and `titan-e2e_*` volumes were not removed. An earlier attempt on this same project committed a batch because the in-process signal did not stop the driver. That checkpoint volume was removed with `docker volume rm titan-sparkkill_spark_checkpoints` before the passing run. The passing run started from an empty checkpoint.

Command:

```powershell
uv run --group orchestration python scripts/verify_spark_kill.py
```

Raw output is `docs/audits/spark-kill-evidence.json`. The script does not delete volumes.

## Dataset

Deterministic seed `221190` produced 4 orders, with the third event late by 20 hours (`ord_f973aa844320`). The first event was posted again. A conflict reused a new `event_id` (`6f0d3e2a-1b7c-4d5e-8f90-a1b2c3d4e5f7`) and changed quantity and total. Seed `221191` produced one more order twice through the Kafka producer. One value `not-json` was produced on `titan.commerce.order-events` with key `ord_killbad01`.

| Check | Persisted result |
| --- | --- |
| Kafka watermark sum | 9 |
| Accepted logical orders | 5, each `order_id` once |
| Conflicts | 1 (`ord_b791b39d27ef` also has the accepted row) |
| Duplicate deliveries | 2 |
| Quarantine | 1 |
| Line items on accepted orders | 10 |

Quarantine row: topic `titan.commerce.order-events`, partition 2, offset 1, key `ord_killbad01`, `error_type=malformed_json`, raw value `not-json`.

Accepted revenue in PostgreSQL, currencies kept separate:

| Currency | Orders | Revenue |
| --- | --- | --- |
| EUR | 1 | 79.96 |
| GBP | 3 | 1214.72 |
| USD | 1 | 372.25 |

## Kill

`maybe_kill_driver("bronze_staged")` runs after `write_bronze_batch` returns and before `commit_bronze_batch`. With `TITAN_SPARK_KILL_HOLD_SECONDS=120` it fsyncs `/var/lib/titan/spark-checkpoints/kill-marker` and waits. The script polls that marker with `docker exec` and then kills the container. If the hold expires, the function raises and the batch does not commit.

An earlier harness called `os.kill` on the driver process. That process printed `spark kill stage=bronze_staged`, then continued, wrote `bronze_batch_commits`, wrote checkpoint `commits/0`, and exited 0. That attempt is not this pass.

Passing interruption:

| Item | Value |
| --- | --- |
| Container | `titan-sparkkill-bronze-kill` |
| Bronze start | `2026-11-22T21:03:30.340136+00:00` |
| Log line inside the batch | `spark kill stage=bronze_staged` (after Spark stages 0 and 2) |
| External kill | `2026-11-22T21:03:41.683265+00:00`, `docker kill --signal=KILL`, docker exit 0 |
| Driver exit | 137 |
| Marker | `bronze_staged` |
| `bronze_batch_commits` | 0 |
| Checkpoint `offsets/` | `.0.crc`, `0` |
| Checkpoint `commits/` | empty |

The offset file was already present. Its body names the range the batch had started, which is 9 order-event records and no payment or inventory records:

```text
{"titan.commerce.order-events":{"0":5,"1":2,"2":2},"titan.commerce.payment-events":{"0":0,"1":0,"2":0},"titan.commerce.inventory-events":{"0":0,"1":0,"2":0}}
```

Spark writes that offset file before `foreachBatch` returns. The commit file is written only after `foreachBatch` returns. The commit directory was empty while the batch was in progress, so the checkpoint had not advanced past the unfinished batch.

## Restart

The next `spark-bronze` run used the same checkpoint volume and the same source options, without `TITAN_SPARK_KILL_AFTER`. Exit code 0.

| Item | Value |
| --- | --- |
| `bronze_batch_commits` | batch 0, attempt 1, 9 records |
| Object prefix | `s3a://titan-lake/bronze/commerce_events/batch_id=0/attempt=1` |
| `committed_at` | `2026-11-22 21:03:56.806305+00` |
| Checkpoint `commits/` | `.0.crc`, `0` |
| Commit file | `v1` / `{"nextBatchWatermarkMs":0}` |
| Parquet rows at that prefix | 9 |
| Distinct `(topic, partition, offset)` | 9 |
| `parse_status=invalid` | 1 |

Attempt 1 is the same prefix the killed copy had started. No commit row existed, so the retry numbered the attempt 1 again and overwrote that prefix before inserting the commit. Readers follow `bronze_batch_commits`. The prefix was not visible as a committed batch at kill time.

A second `availableNow` run exited 0 and left the row at `0|1|9`.

## Publication

`python -m titan_warehouse` on the isolated ports, then `GET /api/v1/warehouse/status`.

| Check | PostgreSQL | ClickHouse |
| --- | --- | --- |
| Publication | `0b63fe78-fed3-48f8-bae0-d927baa69195` | `wh_active.warehouse_publication_id` the same |
| Silver publication | | `1136f0d2-dffc-4c9c-a541-056fce00a6f0` |
| Orders | 5 accepted | `fct_orders` 5, `wh_orders` 5 |
| Line items | 10 | `wh_order_items` 10, `wh_active.item_count` 10 |
| Payments / inventory | none in this dataset | `wh_payments` 0, `wh_inventory` 0 |
| EUR / GBP / USD | 79.96 / 1214.72 / 372.25 | same sums on `fct_orders` and in `wh_active.revenue_json` |
| Ready | `quality_runs.status=passed` (1), `quality_gate` succeeded, `warehouse_models` succeeded | API `ready=true` |

`fct_orders` contains `ord_f973aa844320` once, GBP 324.75. No accepted `order_id` appears twice.

ClickHouse `wh_active.published_at` is `2026-11-22 19:04:46.920725`. The host and Postgres timestamps for this run are around `21:03+00`. Both clocks are recorded as returned.

## What this does and does not change

The production path is unchanged when `TITAN_SPARK_KILL_AFTER` is unset. The hold exists so a test can SIGKILL the container at the commit boundary. A matching stage with a hold that is never killed raises, so the microbatch does not fall through into `commit_bronze_batch`.

`pipeline_runs` rows 1, 2, 3, and 5 for `bronze_stream`, and row 7 for `warehouse_load`, remain `running` with `finished_at` null. SIGKILL cannot write its own terminal row. Later rows for those job names are `succeeded`, and the latest row is the one monitoring reads. The open rows are the killed attempt and the in-progress markers the later runs left behind when they moved on.

Dagster was not materialised in this run. Publication and the quality gate ran through `titan_warehouse`.

## Tests

`uv run pytest -m "not integration" -q`: 58 passed, 1 deselected. The new hold test stubs `time.sleep`, expects `RuntimeError`, and expects no `os.kill` call. `uv run mypy`: 80 files, no issues. `uv run ruff check` passed on the Spark policy, the Bronze job, the kill script, and the lifecycle tests.

After the evidence above was read back from Postgres, ClickHouse, and the checkpoint files, the project containers were stopped without deleting volumes:

```powershell
docker compose -p titan-sparkkill -f docker-compose.yml -f docker-compose.e2e.yml stop
```
