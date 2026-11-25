from datetime import UTC, datetime, timedelta

from scripts.sustained_load import _tracking_summary


def test_tracking_distinguishes_preexisting_ids_from_new_persistence() -> None:
    now = datetime.now(UTC)
    event_ids = ["old-1", "old-2", "new-1", "new-2"]
    before = {
        event_id: {"disposition": "accepted", "ingested_at": now} for event_id in event_ids[:2]
    }
    after = {
        **before,
        "new-1": {"disposition": "accepted", "ingested_at": now + timedelta(seconds=1)},
        "new-2": {"disposition": "accepted", "ingested_at": now + timedelta(seconds=2)},
    }
    result = _tracking_summary(
        event_ids,
        before,
        after,
        {event_id: now for event_id in event_ids[2:]},
    )

    assert result["preexisting_event_ids"] == 2
    assert result["newly_persisted_event_ids"] == 2
    assert result["accounted_event_ids"] == 4
    assert result["unaccounted_event_ids"] == []
    latency = result["ack_to_persistence_seconds"]
    assert isinstance(latency, dict)
    assert latency["count"] == 2
