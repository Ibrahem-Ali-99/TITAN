"""End-to-end Kafka export/replay verification against the real ingestion service."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from confluent_kafka import ConsumerGroupTopicPartitions, Producer, admin
from sqlalchemy import create_engine, text

from event_generator.generator import generate_orders

ROOT = Path(__file__).resolve().parents[1]
SOURCE_TOPIC = "titan.recovery.source"
RESTORED_TOPIC = "recovery.titan.recovery.source"
GROUP = "titan-recovery-ingestion"


def _bootstrap() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:19092")


def _database() -> str:
    return os.environ.get(
        "DATABASE_URL", "postgresql+psycopg://titan:titan_ci_password@127.0.0.1:15435/titan"
    )


def _ensure_topic(topic: str) -> None:
    client = admin.AdminClient({"bootstrap.servers": _bootstrap()})
    result = client.create_topics([admin.NewTopic(topic, 3, 1)])  # type: ignore[attr-defined]
    try:
        result[topic].result(10)
    except Exception as exc:
        if "TOPIC_ALREADY_EXISTS" not in str(exc):
            raise


def _prepare(args: argparse.Namespace) -> None:
    started = time.perf_counter()
    _ensure_topic(SOURCE_TOPIC)
    _ensure_topic(RESTORED_TOPIC)
    events = generate_orders(
        2, seed=424242, deterministic_event_ids=True, produced_at=datetime(2026, 1, 1, tzinfo=UTC)
    )
    conflict_data = events[0].model_dump(mode="json")
    conflict_data["event_id"] = str(uuid.uuid5(uuid.NAMESPACE_URL, "titan-recovery-conflict"))
    conflict_data["payload"]["items"][0]["unit_price"] = "99.99"
    conflict_data["payload"]["total_amount"] = format(
        sum(
            item["quantity"] * float(item["unit_price"])
            for item in conflict_data["payload"]["items"]
        ),
        ".2f",
    )
    records = [
        (0, events[0].model_dump_json().encode(), events[0].payload.order_id.encode()),
        (1, events[0].model_dump_json().encode(), events[0].payload.order_id.encode()),
        (2, events[1].model_dump_json().encode(), events[1].payload.order_id.encode()),
        (0, json.dumps(conflict_data).encode(), events[0].payload.order_id.encode()),
        (1, b"{malformed-json", b"bad-recovery-key"),
    ]
    producer = Producer(
        {"bootstrap.servers": _bootstrap(), "acks": "all", "enable.idempotence": True}
    )
    try:
        for partition, value, key in records:
            producer.produce(
                SOURCE_TOPIC,
                partition=partition,
                key=key,
                value=value,
                headers=[("fixture", b"kafka-postgres-recovery")],
            )
        producer.flush(30)
    finally:
        producer.flush(10)
    backup = ROOT / ".tmp-kafka-postgres-recovery"
    if backup.exists():
        shutil.rmtree(backup)
    command = [
        sys.executable,
        os.fspath(ROOT / "scripts" / "kafka_dr.py"),
        "--bootstrap",
        _bootstrap(),
    ]
    subprocess.run(
        [*command, "backup", "--topic", SOURCE_TOPIC, "--output", os.fspath(backup)], check=True
    )
    subprocess.run(
        [*command, "restore", "--backup", os.fspath(backup), "--topic-prefix", "recovery."],
        check=True,
    )
    evidence = {
        "fixture_messages": 5,
        "source_topic": SOURCE_TOPIC,
        "restored_topic": RESTORED_TOPIC,
        "prepare_duration_ms": round((time.perf_counter() - started) * 1000),
    }
    Path(args.evidence).write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(evidence, indent=2))


def _lag() -> int:
    client = admin.AdminClient({"bootstrap.servers": _bootstrap()})
    offsets = client.list_consumer_group_offsets([ConsumerGroupTopicPartitions(GROUP)])
    described = offsets[GROUP].result(timeout=10)
    total = 0
    for item in described.topic_partitions:
        if item.topic != RESTORED_TOPIC:
            continue
        from confluent_kafka import Consumer, TopicPartition

        probe = Consumer({"bootstrap.servers": _bootstrap(), "group.id": f"lag-{time.time_ns()}"})
        try:
            _, high = probe.get_watermark_offsets(
                TopicPartition(item.topic, item.partition), timeout=10
            )
        finally:
            probe.close()
        total += max(0, int(high) - int(item.offset))
    return total


def _wait_dispositions(timeout: float = 90) -> dict[str, Any]:
    engine = create_engine(_database(), pool_pre_ping=True)
    deadline = time.monotonic() + timeout
    try:
        while time.monotonic() < deadline:
            with engine.connect() as connection:
                commerce = connection.execute(
                    text(
                        "SELECT disposition, count(*) FROM commerce_events "
                        "WHERE kafka_topic=:topic GROUP BY disposition"
                    ),
                    {"topic": RESTORED_TOPIC},
                ).all()
                quarantine = connection.execute(
                    text("SELECT count(*) FROM ingestion_quarantine WHERE kafka_topic=:topic"),
                    {"topic": RESTORED_TOPIC},
                ).scalar_one()
                observations = connection.execute(
                    text("SELECT count(*) FROM ingestion_observations WHERE kafka_topic=:topic"),
                    {"topic": RESTORED_TOPIC},
                ).scalar_one()
            dispositions = {str(row[0]): int(row[1]) for row in commerce}
            if (
                sum(dispositions.values()) + int(quarantine) + int(observations) == 5
                and _lag() == 0
            ):
                return {
                    "commerce_dispositions": dispositions,
                    "quarantined": int(quarantine),
                    "duplicate_observations": int(observations),
                    "lag": 0,
                }
            time.sleep(1)
        raise RuntimeError("restored Kafka records did not reach durable disposition and zero lag")
    finally:
        engine.dispose()


def _verify(args: argparse.Namespace) -> None:
    started = time.perf_counter()
    before = _wait_dispositions()
    compose = [
        "docker",
        "compose",
        "--project-name",
        os.environ["COMPOSE_PROJECT_NAME"],
        "-f",
        "docker-compose.yml",
        "-f",
        ".github/docker-compose.integration.yml",
        "-f",
        ".github/docker-compose.kafka-recovery.yml",
    ]
    subprocess.run([*compose, "restart", "ingestion"], cwd=ROOT, check=True)
    after = _wait_dispositions()
    if (
        before["commerce_dispositions"] != after["commerce_dispositions"]
        or before["quarantined"] != after["quarantined"]
    ):
        raise RuntimeError(f"consumer restart changed durable outcomes: {before} -> {after}")
    prior = json.loads(args.evidence.read_text(encoding="utf-8")) if args.evidence.exists() else {}
    evidence = {
        **prior,
        "before_restart": before,
        "after_restart": after,
        "restart_idempotent": True,
        "verify_duration_ms": round((time.perf_counter() - started) * 1000),
    }
    Path(args.evidence).write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(evidence, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    for name, function in (("prepare", _prepare), ("verify", _verify)):
        command = sub.add_parser(name)
        command.add_argument("--evidence", required=True, type=Path)
        command.set_defaults(func=function)
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
