"""Authoritative inclusion rules. These functions do not see Spark arrival order.

PostgreSQL ``disposition`` decides whether an event is an accepted business
fact. A streaming watermark can drop a row from a preliminary window. It is
not a reason to leave an accepted order out of a historical recomputation.
"""

import os
import signal
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

STALE_AFTER_SECONDS = 90
DRIVER_SIGKILL = getattr(signal, "SIGKILL", 9)


def logical_order(event_type: str, disposition: str) -> bool:
    return event_type == "order_created" and disposition == "accepted"


def include_payment(event_type: str, disposition: str) -> bool:
    return event_type == "payment_completed" and disposition == "accepted"


def include_inventory(event_type: str, disposition: str) -> bool:
    return event_type == "inventory_updated" and disposition == "accepted"


def streaming_keeps_event(event_time: datetime, watermark: datetime | None) -> bool:
    """A row older than the watermark is dropped from the streaming aggregate."""
    if watermark is None:
        return True
    return event_time >= watermark


def batch_keeps_accepted_order(
    event_type: str,
    disposition: str,
    event_time: datetime,
    watermark: datetime | None,
) -> bool:
    """Canonical totals include every accepted order, including late ones."""
    del event_time, watermark
    return logical_order(event_type, disposition)


def current_publication(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    """The current Silver version is the latest row that finished as published."""
    published = [
        row
        for row in rows
        if row.get("status") == "published" and row.get("finished_at") is not None
    ]
    if not published:
        return None
    return max(published, key=lambda row: row["finished_at"])


def maybe_kill_driver(stage: str) -> None:
    """SIGKILL the driver when a crash test names this stage.

    A streaming microbatch commits its checkpoint only after foreachBatch
    returns. Killing after the Parquet copy and before that return leaves the
    checkpoint commit unwritten. Runs without ``TITAN_SPARK_KILL_AFTER`` continue.
    """
    if os.environ.get("TITAN_SPARK_KILL_AFTER") != stage:
        return
    marker = os.environ.get("TITAN_SPARK_KILL_MARKER")
    if marker:
        path = Path(marker)
        path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(marker, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        os.write(fd, stage.encode())
        os.fsync(fd)
        os.close(fd)
    print(f"spark kill stage={stage}", file=sys.stderr, flush=True)
    hold = int(os.environ.get("TITAN_SPARK_KILL_HOLD_SECONDS", "0"))
    if hold > 0:
        time.sleep(hold)
        raise RuntimeError(f"{stage} stayed alive for {hold}s; the driver was not killed")
    os.kill(os.getpid(), DRIVER_SIGKILL)


def pipeline_finished_at(state: str, now: datetime) -> datetime | None:
    """An open running report is not a completed execution."""
    if state == "running":
        return None
    return now


def display_state(
    state: str | None,
    updated_at: datetime | None,
    now: datetime,
    *,
    stale_after_seconds: int = STALE_AFTER_SECONDS,
) -> str:
    """Job state comes from the last report, not from whether a container exists."""
    if state is None or updated_at is None:
        return "not_reported"
    if state == "failed":
        return "failed"
    if state == "succeeded":
        return "succeeded"
    if state == "running":
        age = (now - updated_at).total_seconds()
        if age > stale_after_seconds:
            return "stale"
        return "running"
    return "not_reported"
