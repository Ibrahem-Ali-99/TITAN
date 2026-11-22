"""Decide how to repair a ClickHouse pointer that disagrees with PostgreSQL.

ClickHouse and PostgreSQL cannot commit this pointer in one transaction.
Recovery never points the serving views at a load PostgreSQL has not published,
and it never points them at a published id that has no inserted rows.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class PublishedPointer:
    publication_id: str
    has_rows: bool


@dataclass(frozen=True)
class RecoveryDecision:
    action: str
    publication_id: str | None
    reason: str


def decide_recovery(
    clickhouse_publication_id: str | None,
    published_newest_first: list[PublishedPointer],
) -> RecoveryDecision:
    """Return ``none``, ``exchange``, or ``refuse``."""
    servable = [item for item in published_newest_first if item.has_rows]
    if not servable:
        return RecoveryDecision(
            action="refuse",
            publication_id=clickhouse_publication_id,
            reason="no published warehouse id has inserted rows; the pointer was left unchanged",
        )
    newest = servable[0]
    if clickhouse_publication_id == newest.publication_id:
        return RecoveryDecision(
            action="none",
            publication_id=newest.publication_id,
            reason="the ClickHouse pointer matches the newest published warehouse id",
        )
    return RecoveryDecision(
        action="exchange",
        publication_id=newest.publication_id,
        reason=(
            "the pointer is moved to the newest published id that already has rows; "
            "unpublished candidates are not served"
        ),
    )
