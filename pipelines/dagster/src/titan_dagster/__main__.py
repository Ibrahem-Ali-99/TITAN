"""Materialize the warehouse publication assets in this process."""

import sys

from dagster import materialize

from titan_dagster.definitions import (
    analytics_ready,
    bronze_evidence,
    clickhouse_load,
    demand_forecast,
    quality_gate,
    silver_publication,
    warehouse_models,
)

ASSETS = {
    "bronze_evidence": bronze_evidence,
    "silver_publication": silver_publication,
    "clickhouse_load": clickhouse_load,
    "warehouse_models": warehouse_models,
    "quality_gate": quality_gate,
    "analytics_ready": analytics_ready,
    "demand_forecast": demand_forecast,
}


def main() -> None:
    names = sys.argv[1:] or [
        "silver_publication",
        "clickhouse_load",
        "warehouse_models",
        "quality_gate",
        "analytics_ready",
    ]
    unknown = [name for name in names if name not in ASSETS]
    if unknown:
        raise SystemExit(f"unknown assets: {', '.join(unknown)}")
    result = materialize([ASSETS[name] for name in names])
    if not result.success:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
