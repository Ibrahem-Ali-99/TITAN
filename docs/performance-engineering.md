# TITAN performance engineering

This document records measured results only. A missing measurement is marked
as unavailable; no throughput or latency is inferred from configuration.

## Workstation and runtime

- NVIDIA GeForce RTX 5070, 12,227 MiB, driver 616.92, compute capability 12.0.
- Windows workstation, 32 GB RAM (user-provided).
- Python 3.12 virtual environment and the repository test dependencies.
- CUDA frameworks detected in the local environment: PyTorch, cuDF, and XGBoost were not installed.
- Docker Engine was unavailable during this run, so no stateful benchmark was started.

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
`benchmarks/feature-generation.json`. Stateful Kafka, PostgreSQL, Spark,
MinIO, ClickHouse, dbt, API, and end-to-end results remain unavailable because
Docker Engine was not reachable. No figures are fabricated for those stages.

## GPU compatibility

`nvidia-smi` confirms that the host can see the RTX 5070. That does not prove
Python or Docker CUDA access. Because the local CUDA ML frameworks were absent
and Docker Engine was unavailable, no CPU/GPU training comparison was run and
no GPU model was integrated. The existing scikit-learn serving policy is
unchanged.

## Correctness and limitations

The feature optimization preserves the existing lag, rolling-window, ordering,
and zero-history semantics; the full test suite remains the correctness gate.
The remaining bottlenecks to measure in an isolated environment are producer
acknowledgement versus persistence, consumer commit/persistence throughput,
Spark Bronze/Silver, object-store writes, ClickHouse publication, dbt, API
latency, and ML training. GPU Spark/RAPIDS integration should not be attempted
until official version compatibility is established for the deployed Spark,
Java, CUDA, and GPU stack.
