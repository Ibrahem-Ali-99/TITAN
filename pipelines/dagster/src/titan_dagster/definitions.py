"""Dagster assets for the published Silver to warehouse path.

Bronze is a separate job. Silver reads authoritative PostgreSQL dispositions, so a
warehouse run does not wait for a Bronze completion flag.
"""

import os
import subprocess
from datetime import UTC, datetime
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    AssetSelection,
    DefaultScheduleStatus,
    Definitions,
    RetryPolicy,
    ScheduleDefinition,
    asset,
    define_asset_job,
)

from titan_dagster.launcher import compose_command
from titan_warehouse.dotenv import load_cwd_env

ROOT = Path(__file__).resolve().parents[4]
RETRY = RetryPolicy(max_retries=1, delay=20)

load_cwd_env()


def _compose(service: str) -> None:
    command = (
        compose_command(service)
        if os.environ.get("TITAN_PROTECTED_STAGING", "").lower() == "true"
        else ["docker", "compose", "--profile", "processing", "run", "--rm", service]
    )
    completed = subprocess.run(
        command,
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
def quality_gate(context: AssetExecutionContext, warehouse_models: str) -> str:
    """Blocking checks for the tested candidate. Promotion has not happened yet."""
    from titan_warehouse.load import run_quality_gate

    run_id = run_quality_gate(warehouse_models)
    context.add_output_metadata({"quality_run_id": run_id})
    return warehouse_models


@asset(deps=[quality_gate], retry_policy=RETRY, group_name="warehouse")
def analytics_ready(context: AssetExecutionContext, warehouse_models: str) -> str:
    """Promote the tested load after the quality gate. A failed check never arrives here."""
    from titan_warehouse.load import publish_load

    publication_id = publish_load(warehouse_models)
    context.add_output_metadata({"warehouse_publication_id": publication_id})
    return publication_id


@asset(retry_policy=RETRY, group_name="ml")
def demand_forecast(context: AssetExecutionContext) -> str:
    """Train from the active publication. A training failure leaves that publication in place."""
    from titan_ml.train import train_demand_model

    run_id = train_demand_model()
    context.add_output_metadata({"ml_run_id": run_id})
    return run_id


analytics_publication = define_asset_job(
    name="analytics_publication",
    selection=AssetSelection.assets(
        silver_publication,
        clickhouse_load,
        warehouse_models,
        quality_gate,
        analytics_ready,
    ),
)

demand_training = define_asset_job(
    name="demand_training",
    selection=AssetSelection.assets(demand_forecast),
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

daily_training = ScheduleDefinition(
    job=demand_training,
    cron_schedule="30 2 * * *",
    name="daily_training",
)


@asset(group_name="monitoring")
def reliability_heartbeat(context: AssetExecutionContext) -> str:
    """Record one short run. This asset does not publish the warehouse or train a model."""
    from titan_warehouse.load import _record

    started = datetime.now(UTC)
    _record(
        "reliability_heartbeat",
        "succeeded",
        "scheduled heartbeat",
        started_at=started,
    )
    context.add_output_metadata({"job_name": "reliability_heartbeat"})
    return "reliability_heartbeat"


reliability_heartbeat_job = define_asset_job(
    name="reliability_heartbeat_job",
    selection=AssetSelection.assets(reliability_heartbeat),
)

heartbeat_schedule = ScheduleDefinition(
    job=reliability_heartbeat_job,
    cron_schedule="*/5 * * * *",
    name="heartbeat_schedule",
    default_status=DefaultScheduleStatus.STOPPED,
)

defs = Definitions(
    assets=[
        bronze_evidence,
        silver_publication,
        clickhouse_load,
        warehouse_models,
        quality_gate,
        analytics_ready,
        demand_forecast,
        reliability_heartbeat,
    ],
    jobs=[analytics_publication, bronze_refresh, demand_training, reliability_heartbeat_job],
    schedules=[hourly_analytics, daily_training, heartbeat_schedule],
)
