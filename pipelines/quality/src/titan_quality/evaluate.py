"""Run candidate checks and store the execution. A failure does not exchange wh_active."""

import json
import os
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import psycopg

from titan_quality.checks import Finding, blocking_failed, build_findings


class QualityGateError(RuntimeError):
    pass


def gate_load(database_url: str, client: Any, load: dict[str, object]) -> str:
    """Persist findings for a tested load. Raise when a blocking check fails."""
    publication_id = str(load["warehouse_publication_id"])
    orders = _rows(
        client,
        """
        select order_id, currency, total_amount, occurred_at
        from wh_orders
        where warehouse_publication_id = {pub:String}
        """,
        publication_id,
    )
    items = _rows(
        client,
        """
        select order_id, currency, quantity, line_amount
        from wh_order_items
        where warehouse_publication_id = {pub:String}
        """,
        publication_id,
    )
    payments = _rows(
        client,
        """
        select payment_id, order_id, order_matched
        from wh_payments
        where warehouse_publication_id = {pub:String}
        """,
        publication_id,
    )
    expected = json.loads(str(load["revenue_json"] or "{}"))
    if not isinstance(expected, dict):
        expected = {}
    findings = build_findings(
        orders,
        items,
        payments,
        {str(key): str(value) for key, value in expected.items()},
        injected=os.environ.get("TITAN_QUALITY_INJECT"),
        event_age_days=_event_age(orders),
        warehouse_age_seconds=_active_age(client),
        accepted_order_count=_accepted_orders(database_url),
    )
    run_id = _persist(database_url, load, findings)
    if blocking_failed(findings):
        failed = [item.check_id for item in findings if item.blocks_publication and not item.passed]
        raise QualityGateError("blocking quality checks failed: " + ", ".join(failed))
    return run_id


def _rows(client: Any, sql: str, publication_id: str) -> list[dict[str, object]]:
    result = client.query(sql, parameters={"pub": publication_id})
    names = list(result.column_names)
    return [dict(zip(names, row, strict=True)) for row in result.result_rows]


def _event_age(orders: list[dict[str, object]]) -> int | None:
    moments = [
        value for value in (row.get("occurred_at") for row in orders) if isinstance(value, datetime)
    ]
    if not moments:
        return None
    latest = max(moments)
    if latest.tzinfo is None:
        latest = latest.replace(tzinfo=UTC)
    return max(0, (datetime.now(UTC) - latest).days)


def _active_age(client: Any) -> int | None:
    result = client.query("select published_at from wh_active limit 1")
    if not result.result_rows:
        return None
    published = result.result_rows[0][0]
    if not isinstance(published, datetime):
        return None
    if published.tzinfo is None:
        published = published.replace(tzinfo=UTC)
    return max(0, int((datetime.now(UTC) - published).total_seconds()))


def _accepted_orders(database_url: str) -> int:
    with psycopg.connect(database_url) as connection:
        row = connection.execute(
            """
            select count(*)
            from commerce_events
            where event_type = 'order_created' and disposition = 'accepted'
            """
        ).fetchone()
    return int(row[0]) if row is not None else 0


def _persist(database_url: str, load: dict[str, object], findings: list[Finding]) -> str:
    run_id = str(uuid4())
    now = datetime.now(UTC)
    failed = sum(1 for item in findings if not item.passed)
    status = "failed" if blocking_failed(findings) else "passed"
    injected = os.environ.get("TITAN_QUALITY_INJECT")
    with psycopg.connect(database_url) as connection:
        connection.execute(
            """
            insert into quality_runs (
                run_id, warehouse_publication_id, silver_publication_id, status,
                blocking, injected, started_at, finished_at, failure_count
            ) values (%s, %s, %s, %s, 1, %s, %s, %s, %s)
            """,
            (
                run_id,
                load["warehouse_publication_id"],
                load["silver_publication_id"],
                status,
                injected,
                now,
                now,
                failed,
            ),
        )
        for item in findings:
            connection.execute(
                """
                insert into quality_findings (
                    run_id, check_id, check_type, dataset, severity, blocks_publication,
                    observed, expected, passed, business_id, detail
                ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    run_id,
                    item.check_id,
                    item.check_type,
                    item.dataset,
                    item.severity,
                    1 if item.blocks_publication else 0,
                    item.observed,
                    item.expected,
                    1 if item.passed else 0,
                    item.business_id,
                    item.detail,
                ),
            )
        connection.commit()
    return run_id
