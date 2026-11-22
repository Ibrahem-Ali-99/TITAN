"""Dagster assets for the published Silver to warehouse path.

Bronze is a separate job. Silver reads authoritative PostgreSQL dispositions, so a
warehouse run does not wait for a Bronze completion flag.
"""

import subprocess
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    AssetSelection,
    Definitions,
    RetryPolicy,
    ScheduleDefinition,
    asset,
    define_asset_job,
)

ROOT = Path(__file__).resolve().parents[4]
RETRY = RetryPolicy(max_retries=1, delay=20)


def _compose(service: str) -> None:
    completed = subprocess.run(
        ["docker", "compose", "--profile", "processing", "run", "--rm", service],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        detail = (completed.stdout + completed.stderr)[-4000:]
        raise RuntimeError(f"{service} failed: {detail}")


@asset(retry_policy=RETRY, group_name="lake")
def bronze_evidence(context: AssetExecutionContext) -> str:
    """Run the Spark Bronze availableNow job. This is not a warehouse dependency."""
    _compose("spark-bronze")
    context.add_output_metadata({"service": "spark-bronze"})
    return "spark-bronze"


@asset(retry_policy=RETRY, group_name="warehouse")
def silver_publication(context: AssetExecutionContext) -> str:
    """Publish the next Silver version from accepted PostgreSQL rows."""
    _compose("spark-silver")
    context.add_output_metadata({"service": "spark-silver"})
    return "spark-silver"


@asset(deps=[silver_publication], retry_policy=RETRY, group_name="warehouse")
def clickhouse_load(context: AssetExecutionContext) -> str:
    """Insert the current Silver version and validate counts. Do not promote it yet."""
    from titan_warehouse.load import run_warehouse_pipeline

    load_id = run_warehouse_pipeline(stop_after="validated")
    context.add_output_metadata({"load_id": load_id})
    return load_id


@asset(retry_policy=RETRY, group_name="warehouse")
def warehouse_models(context: AssetExecutionContext, clickhouse_load: str) -> str:
    """Build and test the candidate dbt models for the validated load."""
    from titan_warehouse.load import test_load

    tested = test_load(clickhouse_load)
    context.add_output_metadata({"load_id": tested})
    return tested


@asset(retry_policy=RETRY, group_name="warehouse")
def analytics_ready(context: AssetExecutionContext, warehouse_models: str) -> str:
    """Promote the tested load. A failed dbt test never reaches this asset."""
    from titan_warehouse.load import publish_load

    publication_id = publish_load(warehouse_models)
    context.add_output_metadata({"warehouse_publication_id": publication_id})
    return publication_id


analytics_publication = define_asset_job(
    name="analytics_publication",
    selection=AssetSelection.assets(
        silver_publication,
        clickhouse_load,
        warehouse_models,
        analytics_ready,
    ),
)

bronze_refresh = define_asset_job(
    name="bronze_refresh",
    selection=AssetSelection.assets(bronze_evidence),
)

hourly_analytics = ScheduleDefinition(
    job=analytics_publication,
    cron_schedule="15 * * * *",
    name="hourly_analytics",
)

defs = Definitions(
    assets=[
        bronze_evidence,
        silver_publication,
        clickhouse_load,
        warehouse_models,
        analytics_ready,
    ],
    jobs=[analytics_publication, bronze_refresh],
    schedules=[hourly_analytics],
)
