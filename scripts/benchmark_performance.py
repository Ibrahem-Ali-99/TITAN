"""Run a repeatable, service-free performance probe for ML feature generation.

This deliberately does not touch Kafka, databases, object storage, or Compose
volumes. It is useful on developer machines where the full isolated stack is
not available, and its output can be compared across commits.
"""

from __future__ import annotations

import json
import platform
import time
from datetime import date, timedelta
from pathlib import Path

from titan_ml.features import feature_rows

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    products = 100
    days = 365
    rows = [
        (f"product-{product}", date(2025, 1, 1) + timedelta(days=day), (product + day) % 17)
        for product in range(products)
        for day in range(days)
    ]
    started = time.perf_counter()
    result_rows = feature_rows(rows)
    elapsed = time.perf_counter() - started
    result = {
        "benchmark": "ml_feature_generation",
        "workload": {"products": products, "days_per_product": days, "input_rows": len(rows)},
        "output_rows": len(result_rows),
        "elapsed_seconds": round(elapsed, 6),
        "rows_per_second": round(len(result_rows) / elapsed, 3) if elapsed else None,
        "python": platform.python_version(),
        "platform": platform.platform(),
        "gpu": {"status": "not evaluated", "reason": "feature benchmark is CPU-only"},
    }
    destination = ROOT / "benchmarks" / "feature-generation.json"
    destination.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
