"""Decide how to repair a ClickHouse pointer that disagrees with PostgreSQL.

ClickHouse and PostgreSQL cannot commit this pointer in one transaction.
A target is eligible only when the published load, dbt run, quality run, Silver
lineage, and ClickHouse fact counts all agree. An orders row by itself is not
enough. Recovery leaves the pointer unchanged when nothing verified remains.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class PublicationEvidence:
    publication_id: str
    published: bool
    dbt_passed: bool
    quality_passed: bool
    silver_published: bool
    order_count: int
    payment_count: int
    inventory_count: int
    item_count: int
    clickhouse_orders: int
    clickhouse_payments: int
    clickhouse_inventory: int
    clickhouse_items: int


@dataclass(frozen=True)
class RecoveryDecision:
    action: str
    publication_id: str | None
    reason: str


def is_verified(item: PublicationEvidence) -> bool:
    """A serving target needs the full publication record, not one fact table."""
    if not item.published or not item.dbt_passed or not item.quality_passed:
        return False
    if not item.silver_published or item.order_count < 1:
        return False
    return (
        item.order_count == item.clickhouse_orders
        and item.payment_count == item.clickhouse_payments
        and item.inventory_count == item.clickhouse_inventory
        and item.item_count == item.clickhouse_items
    )


def decide_recovery(
    clickhouse_publication_id: str | None,
    newest_first: list[PublicationEvidence],
) -> RecoveryDecision:
    """Return ``none``, ``exchange``, or ``refuse``.

    ``newest_first`` is ordered by publication time, newest at index 0.
    An unverified newer id cannot hide an older verified publication, and a
    verified older id cannot replace a newer verified one.
    """
    verified = [item for item in newest_first if is_verified(item)]
    if not verified:
        return RecoveryDecision(
            action="refuse",
            publication_id=clickhouse_publication_id,
            reason=("no verified publication is recoverable; the pointer was left unchanged"),
        )
    newest = verified[0]
    if clickhouse_publication_id == newest.publication_id:
        return RecoveryDecision(
            action="none",
            publication_id=newest.publication_id,
            reason="the ClickHouse pointer matches the newest verified publication",
        )
    return RecoveryDecision(
        action="exchange",
        publication_id=newest.publication_id,
        reason=(
            "the pointer is moved to the newest verified publication; "
            "incomplete and unpublished candidates are not served"
        ),
    )
