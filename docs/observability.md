# Observability

Optional local profile for Prometheus, Grafana, a Postgres exporter, a Kafka exporter, and request traces. It reads the API, the broker, and PostgreSQL. It does not publish the warehouse, write `commerce_events`, or replace the Next.js Monitoring page.

This is a workstation profile. It is not a production on-call system.

## Architecture

```text
FastAPI /metrics ──────► Prometheus ──► Grafana
postgres-exporter ─────► Prometheus
kafka-exporter ────────► Prometheus
FastAPI OTLP HTTP ─────► OpenTelemetry Collector ──► Tempo ──► Grafana
```

The API gauges are computed from records the API already reads:

| Gauge | Source |
| --- | --- |
| `titan_warehouse_ready`, `titan_warehouse_freshness_seconds` | `load_status` |
| `titan_stage_age_seconds` | `monitoring_freshness` |
| `titan_kafka_consumer_lag_records` | the ingestion-group lag query used by the Monitoring page |
| `titan_pipeline_latest_failed` | the newest `pipeline_runs` row per tracked job |
| `titan_quality_latest_failed`, `titan_quality_failure_count` | the latest `quality_runs` row |
| `titan_spark_job_failed` | `spark_job_status` for `bronze_stream` |

Spark jobs are one-shot. There is no Spark UI scrape. ClickHouse is not given a second scrape job. Warehouse readiness already checks that PostgreSQL and `wh_active` agree.

Kafka 4.4 in this Compose file has no Prometheus endpoint. `kafka-exporter` is a separate client. It does not change the broker process. Consumer lag on the dashboards is the API's ingestion-group lag, not a second consumer group and not the Spark checkpoint.

A scrape that fails inside an exporter, or a trace export that fails, stays in that process. The API still returns the HTTP response. Prometheus keeps the last sample and marks the target down.

## Versions

Checked against the image registries and package index on 2026-11-23.

| Component | Version |
| --- | --- |
| Prometheus | `prom/prometheus:v3.16.0` |
| Grafana OSS | `grafana/grafana:13.3.1` |
| Postgres exporter | `prometheuscommunity/postgres-exporter:v0.21.1` |
| Kafka exporter | `danielqsj/kafka-exporter:v1.11.0` |
| OpenTelemetry Collector | `otel/opentelemetry-collector:0.165.0` |
| Tempo | `grafana/tempo:3.1.1` |
| `prometheus-client` | 0.26.0 |
| OpenTelemetry API, SDK, and OTLP HTTP exporter | 1.45.2 |
| `opentelemetry-instrumentation-fastapi` | 0.66b2 |

## Startup and shutdown

Start the business stack first, then the profile. The profile creates only `prometheus_data`, `grafana_data`, and `tempo_data`.

```powershell
docker compose --profile observability up -d
```

Stop and start the profile without stopping Kafka, PostgreSQL, ClickHouse, MinIO, the API, or the console:

```powershell
docker compose --profile observability stop prometheus grafana tempo otel-collector postgres-exporter kafka-exporter
docker compose --profile observability start prometheus grafana tempo otel-collector postgres-exporter kafka-exporter
```

`docker compose down` is the existing project stop. It keeps named volumes, including the three new ones. `docker compose down -v` deletes every named volume for the project, including the business data.

Tracing is off unless the API process sees both variables. Set them and recreate only the API container:

```powershell
$env:OTEL_ENABLED = "1"
$env:OTEL_EXPORTER_OTLP_ENDPOINT = "http://otel-collector:4318/v1/traces"
docker compose up -d --no-deps --force-recreate api
```

A later recreate without those variables returns the API to `OTEL_ENABLED=0`. The collector and Tempo can stay up either way.

Check the profile:

```powershell
uv run python scripts/verify_observability.py
```

The script stops and starts `postgres-exporter` only. It expects the business stack and the profile to already be running, and it expects tracing to be enabled for the trace search.

## URLs

Bound to loopback:

| Surface | URL |
| --- | --- |
| Prometheus | http://127.0.0.1:9090 |
| Grafana | http://127.0.0.1:3001 |
| Tempo query | http://127.0.0.1:3200 |

Grafana user `titan`. Password `GRAFANA_ADMIN_PASSWORD`, or `titan_dev_password` when that variable is unset. That is the same local placeholder already used for PostgreSQL, ClickHouse, and MinIO. Anonymous access and sign-up are off.

| Dashboard | URL |
| --- | --- |
| Platform health | http://127.0.0.1:3001/d/titan-platform/platform-health |
| Kafka ingestion and lag | http://127.0.0.1:3001/d/titan-kafka/kafka-ingestion-and-lag |
| API traffic and latency | http://127.0.0.1:3001/d/titan-api/api-traffic-and-latency |
| Spark and pipeline execution | http://127.0.0.1:3001/d/titan-pipelines/spark-and-pipeline-execution |
| Data freshness | http://127.0.0.1:3001/d/titan-freshness/data-freshness |
| Warehouse publication readiness | http://127.0.0.1:3001/d/titan-warehouse/warehouse-publication-readiness |
| Quality failures | http://127.0.0.1:3001/d/titan-quality/quality-failures |

The console remains http://localhost:3000. Its Monitoring page still calls `/health/ready` and `/api/v1/monitoring/overview`.

## Metrics

Scrape and rule evaluation are 15 seconds. Prometheus retention is 7 days.

Labels on HTTP metrics are method, route template, and status. The route template avoids one series per URL. Gauges do not use publication ids, order ids, or connection strings.

| Metric | Meaning |
| --- | --- |
| `titan_http_requests_total` | Requests by method, route template, and status |
| `titan_http_request_duration_seconds` | Latency histogram. Buckets are 5 ms, 10 ms, 25 ms, 50 ms, 100 ms, 250 ms, 500 ms, 1 s, 2.5 s, and 5 s |
| `titan_api_ready` | 1 when `SELECT 1` succeeds and the migrated tables exist |
| `titan_metrics_refresh_ok` | 1 when every operational gauge refresh in that scrape succeeded |
| `titan_warehouse_ready` | 1 when `load_status` reports the serving publication ready |
| `titan_warehouse_freshness_seconds` | Age of the published warehouse timestamp |
| `titan_stage_age_seconds` | Age of ingestion, Silver, and warehouse stage timestamps |
| `titan_kafka_consumer_lag_records` | Sum of ingestion-group lag, in records |
| `titan_kafka_lag_measured` | 1 when that lag query succeeded |
| `titan_pipeline_latest_failed` | 1 when the newest attempt for that job is `failed` |
| `titan_quality_latest_failed` | 1 when the latest quality run failed and `injected` is empty |
| `titan_quality_latest_injected` | 1 when the latest quality run is an injected test |
| `titan_quality_failure_count` | Blocking failures on the latest quality run |
| `titan_spark_job_failed` | 1 when `spark_job_status` for `bronze_stream` is `failed` |
| `up` | 1 when the scrape target answered |
| `kafka_brokers` | Brokers reported by the Kafka exporter |
| `pg_stat_database_numbackends` | PostgreSQL backends, from the Postgres exporter |

5xx rate on the API dashboard is `titan_http_requests_total{status=~"5.."}`. There is no second error counter.

Tracked pipeline jobs: `bronze_stream`, `silver_publication`, `clickhouse_load`, `warehouse_load`, `warehouse_models`, `quality_gate`, `analytics_ready`, `warehouse_reconcile`.

## Alerts

| Alert | Expression | For | Severity |
| --- | --- | --- | --- |
| `TitanApiDown` | `up{job="titan-api"} == 0` | 1m | critical |
| `TitanWarehouseNotReady` | `titan_warehouse_ready == 0` | 2m | critical |
| `TitanDataStale` | `titan_warehouse_freshness_seconds > 21600` | 5m | warning |
| `TitanConsumerLag` | lag measured and `titan_kafka_consumer_lag_records > 100` | 5m | warning |
| `TitanPublicationFailed` | newest `quality_gate`, `analytics_ready`, or `warehouse_models` attempt failed | 1m | critical |
| `TitanQualityFailed` | `titan_quality_latest_failed == 1` | 1m | critical |
| `TitanPostgresExporterDown` | `up{job="postgres"} == 0` | 30s | warning |

21600 seconds is the existing quality-gate freshness window. An injected quality failure sets `titan_quality_latest_injected` and does not set `titan_quality_latest_failed`. A lag sample that could not be measured does not page on a stale lag value.

Prometheus evaluates the rules. Grafana does not add a second set. There is no pager or chat receiver in this profile. Firing means the rule is true in Prometheus.

## Tracing

Coverage is FastAPI requests on `titan-api` when `OTEL_ENABLED` is true. The SDK uses a batch exporter to `http://otel-collector:4318/v1/traces`. The collector forwards OTLP HTTP to Tempo. CORS allows `traceparent` and `tracestate`.

Kafka consumers, Spark, dbt, Dagster, and the browser are not instrumented. There is no trace across the ingestion path.

Export uses a 2 second timeout. A collector outage does not fail the HTTP response.

## SLOs

These are local objectives for the alerts above. They are not a measured compliance period.

| Objective | Target | Alert |
| --- | --- | --- |
| API metrics scrape | `up{job="titan-api"}` stays 1 | `TitanApiDown` |
| Serving publication | `titan_warehouse_ready` stays 1 | `TitanWarehouseNotReady` |
| Warehouse freshness | age at or below 6 hours | `TitanDataStale` |
| Ingestion lag | at or below 100 records while the lag query succeeds | `TitanConsumerLag` |
| Publication jobs | newest `quality_gate`, `analytics_ready`, and `warehouse_models` attempts are not `failed` | `TitanPublicationFailed` |

On the 2026-11-23 check, the development publication was about 64403 seconds old. The freshness objective was not met. `TitanDataStale` became pending at `2026-11-23T11:23:20Z` and was firing on the following read, value about 64370 seconds. It was left firing. Clearing it requires a newer publication, which this check did not create. Lag was 0, warehouse ready was 1, and the newest `quality_gate`, `analytics_ready`, and `warehouse_models` rows were `succeeded`. Those three readings are one sample, not an SLO report.

## Security

- Grafana, Prometheus, and Tempo listen on `127.0.0.1` only.
- Exporter and collector ports are not published on the host.
- Grafana sign-up is disabled. The admin password is the existing local placeholder, not a new secret.
- `/metrics` is on the API port that Compose already publishes. This profile does not add authentication to it. Do not publish that port beyond the workstation.
- Metric labels do not include payloads, tokens, or database URLs.
- The Postgres exporter uses the Compose database user inside the Docker network. `sslmode=disable` matches the local Postgres listener, which has no TLS.

## Limits

- One Kafka broker and one Postgres instance. The exporter memory limits are 128 MiB each. Prometheus is 512 MiB, Grafana 384 MiB, Tempo 384 MiB, and the collector 192 MiB.
- Every API scrape recalculates warehouse status and ingestion lag. One measured `GET /metrics` took 118.4 ms. `GET /health/ready` in the same run, 10 sequential calls, was 4.3 / 5.0 / 19.4 ms min / p50 / max. That ready sample used `urllib` after the process was warm. It is not a before-and-after against the earlier 46 / 48 / 52 ms audit sample, and it is not a capacity test.
- The Postgres exporter config only sets `auth_modules: {}`. The DSN comes from `DATA_SOURCE_NAME`. `stat_statements` is disabled because this Postgres image does not load that extension.
- Tempo logs an idle scheduler `NotFound` while it has no compaction job. Traces were still stored and queried.
- Day 5 and Day 6 notes that say those milestones did not add Prometheus describe those milestones. This profile is the later addition.
- The profile does not make the stack production-ready.

## Verification on 2026-11-23

Command: `uv run python scripts/verify_observability.py` against the existing `titan` volumes. The script exited 0 in 115.3 seconds.

| Check | Result |
| --- | --- |
| Prometheus targets `prometheus`, `titan-api`, `postgres`, `kafka` | all `up` |
| Grafana `/api/health` | database `ok`, version 13.3.1 |
| Provisioned dashboards | the seven titles in the URL table |
| Grafana query for `titan_warehouse_ready` | `1` for `api:8000` |
| `titan_http_requests_total` for `GET /health/ready` status 200 | 24, then 32 after the script's requests and concurrent console polling |
| Ingestion lag | Monitoring API `total_lag` 0 and `titan_kafka_consumer_lag_records` 0 |
| Warehouse | `GET /api/v1/warehouse/status` `ready: true` and `titan_warehouse_ready` 1 |
| `kafka_brokers` | 1 |
| `pg_stat_database_numbackends` for `titan` | 7 |
| `TitanPostgresExporterDown` | `inactive`, then `firing` while the exporter was stopped, then `inactive` after it was started |
| `TitanDataStale` | pending at the end of the script, then `firing` on the next Prometheus read. The publication was still `50d22fe5-8db0-4f2f-bc00-47bda48152fc`, 362 orders, `ready: true` |
| Trace | Tempo trace `6f3c5e1876e3bddf9b4957d2bea08e`, root service `titan-api`, root span `GET /api/v1/ingestion/stats` |

The controlled alert stops the exporter container. It does not stop PostgreSQL.

Same-day suites after the change:

| Suite | Result |
| --- | --- |
| `ruff check` and `ruff format --check` | passed, 143 files |
| `mypy` | passed, 88 files |
| `pytest -m "not integration"` | 63 passed, 1 deselected, 14.68 s |
| `pytest -m integration` | 1 passed, 63 deselected, 3.07 s |
| `npm test` in `apps/web` | 10 passed |

The integration test publishes through the live broker into the existing PostgreSQL volume. It does not reset volumes and it does not republish the warehouse. The serving snapshot stayed ready.
