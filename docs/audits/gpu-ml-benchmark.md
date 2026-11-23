# TITAN CPU versus GPU ML benchmark

Measured 2026-11-23 from commit `7b05bb36bb629416cb3842ca09af400b3576e391`.
The benchmark is isolated and does not read or write TITAN databases, MinIO,
ClickHouse, ML artifacts, or serving metadata.

## Compatibility

- Host GPU: NVIDIA GeForce RTX 5070, 12,227 MiB, driver 616.92, compute
  capability 12.0 as reported by `nvidia-smi`.
- Docker GPU passthrough: verified with CUDA 12.8.0.
- CPU environment: Python 3.12.13, scikit-learn 1.7.2, NumPy 2.5.5,
  joblib 1.6.0.
- Experimental GPU environment: CUDA 12.8.0 container, Python 3.10.12,
  XGBoost 3.1.3, NumPy 2.1.3, scikit-learn 1.7.2, joblib 1.4.2.
- Current XGBoost documentation says CUDA >=12.9 is required for the current
  3.4.x line; the experiment therefore used the older 3.1.3 wheel, which ran
  successfully on CUDA 12.8. No experimental dependency was added to TITAN's
  normal runtime.

Official references: [XGBoost GPU support](https://xgboost.readthedocs.io/en/stable/gpu/),
[XGBoost installation](https://xgboost.readthedocs.io/en/stable/install.html),
and [XGBoost parameters](https://xgboost.readthedocs.io/en/stable/parameter.html).

## Fairness and workload

The deterministic seed is 51022. Each dataset has TITAN's five features:
`lag_1`, `lag_7`, `rolling_7`, `day_of_week`, and product code. The target is
generated from the same lag features. The final seven chronological days are
held out; no future target is used to construct features.

The CPU candidate is the production
`HistGradientBoostingRegressor(max_depth=3, learning_rate=0.1, max_iter=80)`.
The GPU candidate is XGBoost `XGBRegressor(max_depth=3, learning_rate=0.1,
n_estimators=80, objective=reg:squarederror, tree_method=hist, device=cuda)`.
They are comparable histogram tree candidates, not the same implementation.
The seasonal-naive serving policy was not changed or bypassed.

Datasets were 180 rows / 138 train / 42 test, 100,000 rows / 93,000 train /
7,000 test, and 1,000,000 rows / 993,000 train / 7,000 test. Three timed
repetitions followed one cold fit. The cold fit includes initialization and
host-to-device transfer where applicable. Warm timings exclude that first fit.

## Results

| Rows | CPU warm train | GPU warm train | CPU cold fit | GPU cold fit | CPU MAE / WAPE | GPU MAE / WAPE | GPU VRAM |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 180 | 0.0253 s | 0.1805 s | 0.0364 s | 0.5246 s | 0.43320 / 0.05392 | 0.34035 / 0.04236 | 3,303 MiB |
| 100,000 | 0.0795 s | 0.1352 s | 0.0800 s | 0.1385 s | 0.06060 / 0.00777 | 0.06068 / 0.00778 | 3,305 MiB |
| 1,000,000 | 0.5254 s | 0.3070 s | 0.5202 s | 0.3011 s | 0.05599 / 0.00718 | 0.05296 / 0.00679 | 3,216 MiB |

Feature preparation was 0.00006–0.058 s CPU and 0.00007–0.058 s in the GPU
container. Warm inference was 0.00050 s, 0.00384 s, and 0.00507 s on CPU,
versus 0.00292 s, 0.00328 s, and 0.00353 s on GPU for the three scales.
Serialization remained below 0.006 s in all reported CPU runs and below 0.001
s for the GPU warm runs after the first serialization.

CPU process peak RAM was not captured by the harness and is therefore not
invented here. GPU memory is a `nvidia-smi` sample during the run, not a formal
allocator high-water mark. XGBoost emitted a device-mismatch warning during
prediction because the benchmark intentionally retained NumPy host test data;
the warning means inference can fall back through a DMatrix and may add memory
or latency. It does not invalidate the training measurement, but it makes the
GPU serving path unsuitable for automatic adoption without a device-resident
prediction design.

## Decision

Do not replace TITAN's current scikit-learn model and do not add GPU runtime
dependencies. The real TITAN-like workload is far below the crossover point:
GPU training was about 7× slower warm at 180 rows and about 1.7× slower at
100,000 rows. Only the synthetic 1M-row case showed a meaningful ~1.7× warm
training advantage, and it used a different model implementation.

The production model-selection rule remains evaluation-driven: compare the
candidate with seasonal naive on the same chronological holdout and serve the
lower-MAE strategy. GPU training speed is not a serving-selection criterion.
The benchmark harness is the only implementation addition; no model artifact,
database, or serving policy changed.

## Reproduction

```powershell
$env:PYTHONPATH = 'pipelines/ml/src'
.venv\Scripts\python.exe scripts\benchmark_ml_cpu_gpu.py --backend cpu --rows 180 100000 1000000 --repetitions 3

# GPU: ephemeral CUDA 12.8 container with experimental XGBoost 3.1.3.
# The container is not part of TITAN's normal runtime.
docker run --rm --gpus all -v D:\TITAN:/workspace:ro nvidia/cuda:12.8.0-base-ubuntu22.04 ...
```

Machine-readable results are in `benchmarks/gpu-ml-benchmark-2026-11-23.json`.
