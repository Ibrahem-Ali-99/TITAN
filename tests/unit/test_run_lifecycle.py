from datetime import UTC, datetime

from titan_api.platform import _publication_scope
from titan_spark.policy import pipeline_finished_at
from titan_warehouse.policy import recorded_run


def test_a_running_warehouse_attempt_is_not_closed() -> None:
    now = datetime(2026, 11, 22, 17, 34, 50, tzinfo=UTC)
    started, finished, duration_ms = recorded_run("running", now, None)
    assert started == now
    assert finished is None
    assert duration_ms is None


def test_a_terminal_warehouse_attempt_keeps_its_duration() -> None:
    started_at = datetime(2026, 11, 22, 17, 33, 6, tzinfo=UTC)
    now = datetime(2026, 11, 22, 17, 33, 7, tzinfo=UTC)
    started, finished, duration_ms = recorded_run("failed", now, started_at)
    assert started == started_at
    assert finished == now
    assert duration_ms == 1000


def test_a_running_spark_report_has_no_finish_time() -> None:
    now = datetime(2026, 11, 22, 17, 33, 48, tzinfo=UTC)
    assert pipeline_finished_at("running", now) is None
    assert pipeline_finished_at("succeeded", now) == now


def test_invalid_metadata_does_not_share_the_unscoped_publication() -> None:
    assert _publication_scope(None) is None
    assert _publication_scope("") is None
    invalid = _publication_scope("{not-json")
    other = _publication_scope("[1, 2]")
    assert invalid is not None
    assert invalid.startswith("invalid-metadata-")
    assert invalid != other
    assert _publication_scope('{"publication_id": "pub-a"}') == "pub-a"
