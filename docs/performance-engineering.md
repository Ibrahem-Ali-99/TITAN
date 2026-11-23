# TITAN performance engineering

This document records measured results only. A missing measurement is marked
as unavailable; no throughput or latency is inferred from configuration.

## Workstation and runtime

- NVIDIA GeForce RTX 5070, 12,227 MiB, driver 616.92, compute capability 12.0.
- Windows workstation, 32 GB RAM (user-provided).
- Python 3.12 virtual environment and the repository test dependencies.
- CUDA frameworks detected in the local environment: PyTorch, cuDF, and XGBoost were not installed.
- Docker Engine 29.4.3 was reachable through Codex's approved elevated execution path.

## Reproduction

```powershell
.venv\Scripts\python.exe -m pytest -q
$env:PYTHONPATH = 'pipelines/ml/src'
.venv\Scripts\python.exe scripts\benchmark_performance.py
```

The service benchmark requires the isolated Compose profile. Use a distinct
Compose project and `.env.clone.example`; never add `-v` to a development
project command. The existing `scripts/benchmark_ingestion.py` measures Kafka
acknowledgements on `titan.benchmark.acks` only. Its result is not persisted
ingestion throughput, warehouse publication throughput, or API latency.

## Baseline and implemented result

The existing baseline feature implementation copied the complete historical
prefix for every row, giving it quadratic behavior per product. The optimized
implementation maintains a bounded seven-day rolling sum and direct lag
lookups, making feature generation linear in the number of rows.

The current machine-readable service-free result is in
`benchmarks/feature-generation.json`. The isolated stateful run is recorded in
`benchmarks/performance-engineering-2026-11-23.json`.

Measured isolated results using project `titan-clone` and `titan-clone_*`
volumes:

- Kafka acknowledgements: 250 messages at 196.65 ack/s; p50 0.907 ms, p95
  1.158 ms, p99 1.579 ms. This topic is not consumed by ingestion.
- Kafka-to-PostgreSQL persistence: 30 deterministic orders acknowledged in
  0.913 s; logical orders increased from 187 to 217.
- Spark Bronze availableNow succeeded and processed 576 distinct Kafka offsets;
  its wrapper timer output was not captured.
- Spark Silver succeeded in 14.555 s and produced publication
  `4992f5fd-d946-4342-9eb4-75b97ff64a89`.
- Warehouse publication including ClickHouse, dbt, tests, and promotion took
  16.413 s; dbt/model execution accounted for 15.786 s.
- Instrumented repeat publications on the same Silver snapshot measured
  14.707 s, 14.376 s, and 15.686 s wall time. The dbt phases were respectively
  4.839/4.725/4.562 s, 4.746/4.589/4.476 s, and 5.125/5.039/4.895 s for
  serving build/candidate build/candidate tests. ClickHouse phases stayed below
  0.15 s each, and quality gates stayed below 0.09 s.
- API `/api/v1/ingestion/stats` over 100 requests: p50 10.717 ms, p95
  11.339 ms, p99 12.171 ms.
- Resource snapshot: Kafka 376.9 MiB/768 MiB, ClickHouse 244.5 MiB/1.5 GiB,
  API 106.4 MiB/512 MiB, ingestion 57.36 MiB/512 MiB, PostgreSQL 34.89
  MiB/512 MiB, and MinIO 84.72 MiB/256 MiB.

Serving reported 217 orders, 217 items, 187 payments, and currency-separated
revenue of EUR 9,215.39, GBP 7,640.95, and USD 9,029.21. No duplicate
analytical facts or currency merging was observed. Counts include clone seed
data plus the 30 new orders.

The Bronze/persistence difference is intentional and accounted for: 576 Bronze
rows contain 575 valid rows and one malformed JSON row. The 575 valid rows have
574 distinct event IDs because one event ID was delivered twice. PostgreSQL
therefore contains 574 canonical commerce events plus one quarantine row;
Silver's pending count is zero. ClickHouse EXPLAIN on the serving staging view
uses the `warehouse_publication_id` primary key and reads 1 of 3 granules.

The phase evidence identifies dbt process/orchestration startup and repeated
dbt invocation overhead as the dominant cost, not ClickHouse query execution or
network transfer. The three dbt invocations each take roughly 4.5–5.1 seconds
while the ClickHouse load phases are roughly 0.3 seconds in total. No SQL,
ClickHouse schema, dbt model, or pointer-publication optimization was made:
the measurements do not justify a speculative change, and combining dbt phases
would risk changing model/test ordering and publication safety.

## GPU compatibility

`nvidia-smi` confirms that the host can see the RTX 5070. Docker GPU passthrough
was also verified with an ephemeral CUDA 12.8 container. Host Python still
lacks PyTorch, cuDF, and XGBoost, so no equivalent CPU/GPU training comparison
was run. No GPU model was integrated and the existing scikit-learn serving
policy is unchanged.

## Correctness and limitations

The feature optimization preserves the existing lag, rolling-window, ordering,
and zero-history semantics; the full test suite remains the correctness gate.
The observed bottleneck is dbt/model execution at 15.786 s of the 16.413 s
warehouse publication. Kafka acknowledgement is not the bottleneck in this
sample. GPU Spark/RAPIDS integration should not be attempted until official
version compatibility is established for the deployed Spark, Java, CUDA, and
GPU stack.
