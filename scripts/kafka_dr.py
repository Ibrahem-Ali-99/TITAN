"""Portable Kafka event-log backup, verification, and isolated replay.

The backup is a bounded export of records in ``[low_watermark, high_watermark)``.
Source offsets are evidence only: Kafka assigns new offsets during replay.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any, cast

from confluent_kafka import Consumer, Producer, TopicPartition, admin

MANIFEST = "manifest.json"
RECORDS = "records.jsonl"


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _client(bootstrap: str) -> admin.AdminClient:
    return admin.AdminClient({"bootstrap.servers": bootstrap})


def _metadata(bootstrap: str, topics: list[str]) -> dict[str, Any]:
    md = _client(bootstrap).list_topics(timeout=10)
    result: dict[str, Any] = {}
    for topic in topics:
        info = md.topics.get(topic)
        if info is None or info.error is not None:
            raise SystemExit(f"topic is unavailable: {topic}")
        result[topic] = {"partitions": sorted(int(p) for p in info.partitions)}
    return result


def _watermarks(bootstrap: str, topic: str, partition: int) -> tuple[int, int]:
    consumer = Consumer(
        {"bootstrap.servers": bootstrap, "group.id": f"titan-dr-probe-{time.time_ns()}"}
    )
    try:
        return tuple(
            int(x)
            for x in consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)
        )  # type: ignore[return-value]
    finally:
        consumer.close()


def backup(args: argparse.Namespace) -> None:
    started = time.perf_counter()
    topics = args.topic or list(_metadata(args.bootstrap, []).keys())
    if not topics:
        raise SystemExit("provide at least one --topic")
    metadata = _metadata(args.bootstrap, topics)
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    records: list[dict[str, Any]] = []
    ranges: list[dict[str, Any]] = []
    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": f"titan-dr-export-{time.time_ns()}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "enable.partition.eof": True,
        }
    )
    try:
        assignments: list[TopicPartition] = []
        for topic in topics:
            for partition in metadata[topic]["partitions"]:
                low, high = _watermarks(args.bootstrap, topic, partition)
                assignments.append(TopicPartition(topic, partition, low))
                ranges.append({"topic": topic, "partition": partition, "low": low, "high": high})
        consumer.assign(assignments)
        remaining = {(r["topic"], r["partition"]): r["high"] for r in ranges}
        while remaining:
            message = consumer.poll(1.0)
            if message is None:
                continue
            error = message.error()
            if error is not None:
                if error.code() == -191:
                    remaining.pop((message.topic(), message.partition()), None)
                    continue
                raise SystemExit(str(message.error()))
            key = (message.topic(), message.partition())
            if message.offset() >= remaining[key]:
                del remaining[key]
                continue
            value = message.value() or b""
            headers = [
                [k, base64.b64encode(v or b"").decode()]
                for k, v in cast(list[tuple[str, bytes | None]], message.headers() or [])
            ]
            records.append(
                {
                    "topic": message.topic(),
                    "partition": message.partition(),
                    "offset": message.offset(),
                    "timestamp": message.timestamp()[1],
                    "key": base64.b64encode(message.key() or b"").decode()
                    if message.key() is not None
                    else None,
                    "value": base64.b64encode(value).decode(),
                    "headers": headers,
                }
            )
    finally:
        consumer.close()
    records.sort(key=lambda r: (r["topic"], r["partition"], r["offset"]))
    payload = "".join(
        json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n" for record in records
    ).encode()
    (output / RECORDS).write_bytes(payload)
    manifest = {
        "backup_id": output.name,
        "created_at": time.time(),
        "bootstrap_source": args.bootstrap,
        "topics": metadata,
        "ranges": ranges,
        "record_count": len(records),
        "records_sha256": _sha256(payload),
        "records_bytes": len(payload),
        "source_offsets_preserved": False,
        "tool": "scripts/kafka_dr.py",
        "backup_duration_ms": round((time.perf_counter() - started) * 1000),
    }
    (output / MANIFEST).write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(manifest, indent=2))


def verify_backup(directory: Path) -> dict[str, Any]:
    manifest = json.loads((directory / MANIFEST).read_text(encoding="utf-8"))
    payload = (directory / RECORDS).read_bytes()
    if len(payload) != manifest["records_bytes"] or _sha256(payload) != manifest["records_sha256"]:
        raise SystemExit("Kafka backup checksum mismatch")
    rows = [json.loads(line) for line in payload.decode().splitlines() if line]
    if len(rows) != manifest["record_count"]:
        raise SystemExit("Kafka backup record count mismatch")
    expected = {(r["topic"], r["partition"], r["offset"]) for r in rows}
    if len(expected) != len(rows):
        raise SystemExit("Kafka backup contains duplicate source positions")
    return cast(dict[str, Any], manifest)


def verify(args: argparse.Namespace) -> None:
    started = time.perf_counter()
    manifest = verify_backup(args.backup.resolve())
    print(
        json.dumps(
            {
                "verified": True,
                "record_count": manifest["record_count"],
                "verification_duration_ms": round((time.perf_counter() - started) * 1000),
            },
            indent=2,
        )
    )


def restore(args: argparse.Namespace) -> None:
    started = time.perf_counter()
    manifest = verify_backup(args.backup.resolve())
    topic_map = {
        topic: f"{args.topic_prefix}{topic}" for topic in manifest["topics"]
    }
    destination = _metadata(args.bootstrap, list(topic_map.values()))
    for source, target in topic_map.items():
        if destination[target] != manifest["topics"][source]:
            raise SystemExit("destination topic partitions do not match backup")
    consumer = Consumer(
        {"bootstrap.servers": args.bootstrap, "group.id": f"titan-dr-nonempty-{time.time_ns()}"}
    )
    try:
        for source_topic, target_topic in topic_map.items():
            for partition in manifest["topics"][source_topic]["partitions"]:
                low, high = _watermarks(args.bootstrap, target_topic, partition)
                if high != low:
                    raise SystemExit("refusing to restore into nonempty destination topics")
    finally:
        consumer.close()
    producer = Producer(
        {"bootstrap.servers": args.bootstrap, "acks": "all", "enable.idempotence": True}
    )
    mappings: list[dict[str, Any]] = []
    for line in (args.backup.resolve() / RECORDS).read_text(encoding="utf-8").splitlines():
        record = json.loads(line)
        key = base64.b64decode(record["key"]) if record["key"] is not None else None
        value = base64.b64decode(record["value"])
        headers: list[tuple[str, bytes]] = [
            (str(item[0]), base64.b64decode(item[1])) for item in record["headers"]
        ]
        box: dict[str, Any] = {}
        def on_delivery(error: Any, delivered: Any, box: dict[str, Any] = box) -> None:
            box.update(error=error, message=delivered)

        target_topic = topic_map[record["topic"]]
        producer.produce(
            target_topic,
            key=key,
            value=value,
            headers=cast(Any, headers),
            partition=record["partition"],
            on_delivery=on_delivery,
        )
        producer.flush(30)
        if box.get("error") is not None:
            raise SystemExit(str(box["error"]))
        message = box["message"]
        mappings.append(
            {
                "source_topic": record["topic"],
                "restored_topic": target_topic,
                "partition": record["partition"],
                "source_offset": record["offset"],
                "restored_offset": message.offset(),
            }
        )
    print(
        json.dumps(
            {
                "restored_records": len(mappings),
                "offset_mapping": mappings,
                "restore_duration_ms": round((time.perf_counter() - started) * 1000),
            },
            indent=2,
        )
    )


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    root.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    sub = root.add_subparsers(dest="command", required=True)
    b = sub.add_parser("backup")
    b.add_argument("--topic", action="append")
    b.add_argument("--output", type=Path, required=True)
    b.set_defaults(func=backup)
    v = sub.add_parser("verify")
    v.add_argument("--backup", type=Path, required=True)
    v.set_defaults(func=verify)
    r = sub.add_parser("restore")
    r.add_argument("--backup", type=Path, required=True)
    r.add_argument("--topic-prefix", default="restored.")
    r.set_defaults(func=restore)
    return root


if __name__ == "__main__":
    parsed = parser().parse_args()
    parsed.func(parsed)
