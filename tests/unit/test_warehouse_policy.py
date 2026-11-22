from datetime import UTC, datetime, timedelta

from titan_warehouse.policy import can_promote, warehouse_is_aligned


def test_an_older_silver_snapshot_cannot_replace_a_newer_one() -> None:
    newer = datetime(2026, 1, 2, tzinfo=UTC)
    older = newer - timedelta(hours=1)
    assert can_promote(newer, None)
    assert can_promote(newer, older)
    assert can_promote(newer, newer)
    assert not can_promote(older, newer)


def test_readers_require_the_same_publication_id() -> None:
    assert warehouse_is_aligned("published", "published")
    assert not warehouse_is_aligned("published", "other")
    assert not warehouse_is_aligned(None, None)
    assert not warehouse_is_aligned("published", None)
