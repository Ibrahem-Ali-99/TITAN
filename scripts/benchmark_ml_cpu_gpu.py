"""Benchmark TITAN's CPU model against an isolated XGBoost CUDA candidate.

This script is intentionally standalone: it never reads or writes TITAN
databases, MinIO, ClickHouse, model artifacts, or serving metadata. The GPU
path is only enabled when an experimental XGBoost installation is supplied.
"""

from __future__ import annotations

import argparse
import json
import platform
import statistics
import subprocess
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import joblib  # type: ignore[import-untyped]
import numpy as np
from sklearn.ensemble import HistGradientBoostingRegressor  # type: ignore[import-untyped]

ROOT = Path(__file__).resolve().parents[1]
CPU_PARAMS = {
    "max_depth": 3,
    "learning_rate": 0.1,
    "max_iter": 80,
    "random_state": 51022,
}
GPU_PARAMS = {
    "max_depth": 3,
    "learning_rate": 0.1,
    "n_estimators": 80,
    "random_state": 51022,
    "objective": "reg:squarederror",
    "tree_method": "hist",
    "device": "cuda",
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--backend", choices=("cpu", "gpu"), required=True)
    parser.add_argument("--rows", type=int, nargs="+", default=[180, 100_000, 1_000_000])
    parser.add_argument("--repetitions", type=int, default=3)
    args = parser.parse_args()
    output = {
        "backend": args.backend,
        "python": platform.python_version(),
        "platform": platform.platform(),
        "seed": 51022,
        "repetitions": args.repetitions,
        "datasets": [],
    }
    for rows in args.rows:
        output["datasets"].append(_benchmark(rows, args.backend, args.repetitions))
    print(json.dumps(output, indent=2))


def _benchmark(rows: int, backend: str, repetitions: int) -> dict[str, object]:
    x, y, train_mask = _dataset(rows)
    train_x, test_x = x[train_mask], x[~train_mask]
    train_y, test_y = y[train_mask], y[~train_mask]
    prep_seconds = _timed_dataset(rows)
    factory = _factory(backend)
    cold_model = factory()
    started = time.perf_counter()
    cold_model.fit(train_x, train_y)
    cold_fit_seconds = time.perf_counter() - started
    cold_model.predict(test_x)
    timings: list[dict[str, float]] = []
    metrics: list[dict[str, float]] = []
    serialized_sizes: list[int] = []
    for _ in range(repetitions):
        model = factory()
        started = time.perf_counter()
        model.fit(train_x, train_y)
        train_seconds = time.perf_counter() - started
        started = time.perf_counter()
        predicted = np.maximum(0.0, np.asarray(model.predict(test_x), dtype=np.float64))
        inference_seconds = time.perf_counter() - started
        started = time.perf_counter()
        import io

        buffer = io.BytesIO()
        joblib.dump(model, buffer)
        serialized_bytes = buffer.getvalue()
        serialization_seconds = time.perf_counter() - started
        serialized_sizes.append(len(serialized_bytes))
        actual = test_y.astype(np.float64)
        absolute = np.abs(actual - predicted)
        metrics.append(
            {
                "mae": float(np.mean(absolute)),
                "wape": float(np.sum(absolute) / np.sum(np.abs(actual))),
            }
        )
        timings.append(
            {
                "train_seconds": train_seconds,
                "inference_seconds": inference_seconds,
                "serialization_seconds": serialization_seconds,
            }
        )
    return {
        "rows": rows,
        "features": int(x.shape[1]),
        "train_rows": int(train_x.shape[0]),
        "test_rows": int(test_x.shape[0]),
        "feature_preparation_seconds": prep_seconds,
        "cold_fit_seconds_including_initialization_transfer": cold_fit_seconds,
        "timings": timings,
        "timing_medians": {
            key: statistics.median(item[key] for item in timings) for key in timings[0]
        },
        "metrics": metrics,
        "metric_medians": {
            "mae": statistics.median(item["mae"] for item in metrics),
            "wape": statistics.median(item["wape"] for item in metrics),
        },
        "serialized_bytes_median": int(statistics.median(serialized_sizes)),
        "peak_vram_mib": _vram_mib() if backend == "gpu" else None,
        "model_parameters": GPU_PARAMS if backend == "gpu" else CPU_PARAMS,
        "split": "chronological final 7 days; same deterministic data and targets",
    }


def _dataset(rows: int) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    products = max(1, min(1000, rows // 30))
    days = max(14, (rows + products - 1) // products)
    actual_rows = products * days
    product = np.repeat(np.arange(products, dtype=np.float32), days)
    day = np.tile(np.arange(days, dtype=np.float32), products)
    lag_1 = ((product * 3 + day * 5) % 17).astype(np.float32)
    lag_7 = ((product * 7 + np.maximum(day - 7, 0) * 5) % 17).astype(np.float32)
    rolling_7 = ((lag_1 + lag_7) / 2.0).astype(np.float32)
    day_of_week = np.mod(day, 7).astype(np.float32)
    features = np.column_stack((lag_1, lag_7, rolling_7, day_of_week, product))
    target = np.maximum(0.0, 0.55 * lag_1 + 0.35 * lag_7 + 0.2 * day_of_week)
    train_mask = day < days - 7
    assert actual_rows == len(target)
    return features, target.astype(np.float32), train_mask


def _timed_dataset(rows: int) -> float:
    started = time.perf_counter()
    _dataset(rows)
    return time.perf_counter() - started


def _factory(backend: str) -> Callable[[], Any]:
    if backend == "cpu":
        return lambda: HistGradientBoostingRegressor(**CPU_PARAMS)
    import xgboost  # type: ignore[import-not-found]

    print(f"xgboost={xgboost.__version__}", file=sys.stderr)
    return lambda: xgboost.XGBRegressor(**GPU_PARAMS)


def _vram_mib() -> int | None:
    try:
        value = subprocess.check_output(
            ["nvidia-smi", "--query-gpu=memory.used", "--format=csv,noheader,nounits"],
            text=True,
        ).strip().splitlines()[0]
        return int(value)
    except (OSError, subprocess.CalledProcessError, ValueError, IndexError):
        return None


if __name__ == "__main__":
    main()
