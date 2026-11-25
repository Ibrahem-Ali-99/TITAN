"""Publish a known order event, persist it once, and read it back through the API.

The running ingestion service must already be consuming. This module does not
start Docker. A second publish of the same event_id must leave one row.
"""

import json
import os
import time
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from sqlalchemy import create_engine, func, select, text
from sqlalchemy.orm import Session

from scripts.envfile import load_env_file
from event_generator.generator import generate_orders
from event_generator.producer import connect_producer, publish_one
from titan_contracts.topics import ORDER_EVENTS_TOPIC
from titan_store.models import CommerceEvent, QuarantineRecord

ROOT = Path(__file__).resolve().parents[1]


@dataclass
class PipelineReport:
    ok: bool
    detail: dict[str, object]


def _database_url() -> str:
    return os.environ.get(
        "DATABASE_URL",
        "postgresql+psycopg://titan:titan_dev_password@localhost:5435/titan",
    )


def _brokers() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def _api_base() -> str:
    return os.environ.get("API_BASE_URL", "http://localhost:8000")


def _wait_until(predicate, timeout: float, interval: float = 0.5):  # type: ignore[no-untyped-def]
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(interval)
    return last


def _count_event(session_factory, event_id) -> int:  # type: ignore[no-untyped-def]
    with Session(session_factory) as session:
        value = session.scalar(
            select(func.count())
            .select_from(CommerceEvent)
            .where(CommerceEvent.event_id == event_id)
        )
    return int(value or 0)


def _get_json(path: str) -> dict[str, object]:
    request = urllib.request.Request(
        f"{_api_base()}{path}",
        headers={"Authorization": f"Bearer {os.environ['TITAN_READ_TOKEN']}"},
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        body = json.loads(response.read().decode("utf-8"))
    if not isinstance(body, dict):
        raise RuntimeError(f"{path} did not return an object")
    return body


def assert_state_change_is_isolated() -> None:
    """Refuse the development project and its published ports.

    Isolated projects use their own volumes, broker, and database. A longer
    port such as 15435 or 19092 is not treated as the development port.
    """
    project = os.environ.get("COMPOSE_PROJECT_NAME", "").strip()
    if project in {"", "titan"}:
        raise RuntimeError(
            "state-changing checks require COMPOSE_PROJECT_NAME to name an isolated project"
        )
    if _uses_development_endpoint(_database_url(), _brokers(), _api_base()):
        raise RuntimeError("state-changing checks refuse development ports 5435, 9092, and 8000")


def _uses_development_endpoint(database: str, brokers: str, api: str) -> bool:
    return _has_port(database, 5435) or _has_port(brokers, 9092) or _has_port(api, 8000)


def _has_port(value: str, port: int) -> bool:
    needle = f":{port}"
    start = 0
    while True:
        found = value.find(needle, start)
        if found < 0:
            return False
        end = found + len(needle)
        if end >= len(value) or not value[end].isdigit():
            return True
        start = end


def run_pipeline_check() -> PipelineReport:
    load_env_file(ROOT / ".env")
    assert_state_change_is_isolated()
    engine = create_engine(_database_url(), pool_pre_ping=True)
    detail: dict[str, object] = {}
    try:
        with engine.connect() as connection:
            tables = connection.execute(
                text(
                    "SELECT to_regclass('public.commerce_events'), "
                    "to_regclass('public.ingestion_quarantine')"
                )
            ).one()
        if tables[0] is None or tables[1] is None:
            return PipelineReport(
                False,
                {
                    "error": "migrations have not created the operational tables",
                    "tables": list(tables),
                },
            )

        event = generate_orders(1, seed=int(uuid4().int % 1_000_000_000))[0]
        producer = connect_producer(_brokers())
        try:
            from event_generator.admin import ensure_order_topic

            ensure_order_topic(_brokers())
            first = publish_one(producer, event)
            found = _wait_until(lambda: _count_event(engine, event.event_id) == 1, 60)
            if not found:
                return PipelineReport(
                    False, {"error": "event was not persisted", "event_id": str(event.event_id)}
                )
            stats_before = _get_json("/api/v1/ingestion/stats")
            second = publish_one(producer, event)
            consumed_again = _wait_until(
                lambda: (
                    _committed_past(second.partition, second.offset)
                    and _count_event(engine, event.event_id) == 1
                ),
                60,
            )
            persisted = _count_event(engine, event.event_id)
            stats_after = _get_json("/api/v1/ingestion/stats")
            recent = _get_json("/api/v1/events/recent?limit=100")
            items = recent.get("items")
            matches = []
            if isinstance(items, list):
                matches = [
                    item
                    for item in items
                    if isinstance(item, dict) and item.get("event_id") == str(event.event_id)
                ]

            token = f"day1-invalid-{uuid4().hex}"
            with Session(engine) as session:
                quarantine_before = int(
                    session.scalar(select(func.count()).select_from(QuarantineRecord)) or 0
                )
            producer.produce(
                topic=ORDER_EVENTS_TOPIC,
                key=b"ord_badbadbadbad",
                value=token.encode("utf-8"),
            )
            producer.flush(30)
            quarantine_seen = _wait_until(
                lambda: _quarantine_contains(engine, token),
                60,
            )
            follower = generate_orders(1, seed=int(uuid4().int % 1_000_000_000))[0]
            follower_result = publish_one(producer, follower)
            follower_seen = _wait_until(lambda: _count_event(engine, follower.event_id) == 1, 60)
        finally:
            producer.flush(10)

        detail = {
            "event_id": str(event.event_id),
            "order_id": event.payload.order_id,
            "first_ack": {"partition": first.partition, "offset": first.offset},
            "second_ack": {"partition": second.partition, "offset": second.offset},
            "second_delivery_observed": bool(consumed_again),
            "persisted_rows_for_event_id": persisted,
            "stats_before_republish": stats_before.get("persisted_events"),
            "stats_after_republish": stats_after.get("persisted_events"),
            "recent_matches": len(matches),
            "invalid_token_quarantined": bool(quarantine_seen),
            "quarantine_before": quarantine_before,
            "follower_event_id": str(follower.event_id),
            "follower_ack": {
                "partition": follower_result.partition,
                "offset": follower_result.offset,
            },
            "follower_persisted": bool(follower_seen),
            "semantics": (
                "at-least-once consumption with idempotent persistence; not end-to-end exactly-once"
            ),
        }
        ok = (
            persisted == 1
            and bool(consumed_again)
            and stats_before.get("persisted_events") == stats_after.get("persisted_events")
            and len(matches) == 1
            and bool(quarantine_seen)
            and bool(follower_seen)
        )
        if not ok:
            detail["error"] = "pipeline assertions failed"
        return PipelineReport(ok, detail)
    finally:
        engine.dispose()


def _committed_past(partition: int, offset: int) -> bool:
    """True when the ingestion group has committed past this record.

    Kafka stores the next offset, so a commit after ``offset`` is ``offset + 1``
    or greater. This observes the running consumer; it does not join the group.
    """
    from confluent_kafka import ConsumerGroupTopicPartitions
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": _brokers()})
    futures = admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions("titan-ingestion")])
    described = futures["titan-ingestion"].result(timeout=10)
    for topic_partition in described.topic_partitions:
        if (
            topic_partition.topic == ORDER_EVENTS_TOPIC
            and topic_partition.partition == partition
            and topic_partition.offset > offset
        ):
            return True
    return False


def _quarantine_contains(engine, token: str) -> bool:  # type: ignore[no-untyped-def]
    with Session(engine) as session:
        rows = session.scalars(select(QuarantineRecord.raw_value)).all()
    return any(value is not None and token in value for value in rows)


def main() -> None:
    report = run_pipeline_check()
    print(json.dumps(report.detail, indent=2, default=str))
    if not report.ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
