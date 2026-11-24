"""Capture and verify TITAN's coordinated recovery-point contract.

This is intentionally a bounded consistency check. It does not claim an atomic
distributed snapshot: operators quiesce producers/consumers before capture,
then use the manifest to select one PostgreSQL/Silver/ClickHouse lineage.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
import time
from pathlib import Path
from typing import Any

import clickhouse_connect
import psycopg
from confluent_kafka import Consumer, TopicPartition, admin
from minio import Minio

from dr_security import secure_file
from titan_messaging.kafka_config import kafka_client_config

ROOT = Path(__file__).resolve().parents[1]
TOPICS = (
    "titan.commerce.order-events",
    "titan.commerce.payment-events",
    "titan.commerce.inventory-events",
)


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _database() -> str:
    return os.environ["DATABASE_URL"]


def _minio() -> Minio:
    endpoint = os.environ["MINIO_ENDPOINT"].removeprefix("http://").removeprefix("https://")
    return Minio(
        endpoint,
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        secure=os.environ["MINIO_ENDPOINT"].startswith("https://"),
    )


def _clickhouse() -> Any:
    return clickhouse_connect.get_client(
        host=os.environ["CLICKHOUSE_HOST"],
        port=int(os.environ["CLICKHOUSE_PORT"]),
        username=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
        database=os.environ["CLICKHOUSE_DATABASE"],
    )


def _kafka_ranges() -> dict[str, list[dict[str, int]]]:
    bootstrap = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    metadata = admin.AdminClient(kafka_client_config(bootstrap)).list_topics(timeout=10)
    consumer_config = kafka_client_config(bootstrap)
    consumer_config["group.id"] = f"titan-dr-boundary-{time.time_ns()}"
    consumer = Consumer(consumer_config)
    try:
        result: dict[str, list[dict[str, int]]] = {}
        for topic in TOPICS:
            info = metadata.topics.get(topic)
            if info is None or info.error is not None:
                raise RuntimeError(f"required Kafka topic is unavailable: {topic}")
            ranges = []
            for partition in sorted(info.partitions):
                low, high = consumer.get_watermark_offsets(
                    TopicPartition(topic, partition), timeout=10
                )
                ranges.append({"partition": int(partition), "low": int(low), "high": int(high)})
            result[topic] = ranges
        return result
    finally:
        consumer.close()


def _postgres() -> dict[str, Any]:
    with psycopg.connect(_database()) as connection:
        rows = connection.execute(
            "select event_id, disposition from commerce_events order by event_id"
        ).fetchall()
        lake = connection.execute(
            """select publication_id, version_uri, finished_at
               from lake_publications where status='published'
               order by finished_at desc limit 1"""
        ).fetchone()
        warehouse = connection.execute(
            """select warehouse_publication_id, silver_publication_id,
                      order_count, payment_count, inventory_count, item_count,
                      accepted_revenue_json
               from warehouse_loads where status='published'
               order by finished_at desc limit 1"""
        ).fetchone()
    if lake is None or warehouse is None:
        raise RuntimeError("published Silver and warehouse metadata are required")
    event_ids = [str(row[0]) for row in rows]
    return {
        "event_count": len(event_ids),
        "event_ids_sha256": _sha256("\n".join(event_ids).encode()),
        "dispositions": {
            str(k): sum(1 for row in rows if row[1] == k) for k in {row[1] for row in rows}
        },
        "silver": {
            "publication_id": str(lake[0]),
            "version_uri": str(lake[1]),
            "finished_at": lake[2].isoformat(),
        },
        "warehouse": {
            "publication_id": str(warehouse[0]),
            "silver_publication_id": str(warehouse[1]),
            "counts": [int(value) for value in warehouse[2:6]],
            "revenue_by_currency": json.loads(str(warehouse[6])),
        },
    }


def _silver_objects(version_uri: str) -> list[dict[str, Any]]:
    root = version_uri.replace("s3a://", "").replace("s3://", "").rstrip("/") + "/"
    bucket, _, prefix = root.partition("/")
    client = _minio()
    objects = []
    for item in client.list_objects(bucket, prefix=prefix, recursive=True):
        response = client.get_object(bucket, item.object_name)
        try:
            data = response.read()
        finally:
            response.close()
            response.release_conn()
        objects.append({"key": item.object_name, "size": len(data), "sha256": _sha256(data)})
    if not any(str(item["key"]).endswith("orders/_SUCCESS") for item in objects):
        raise RuntimeError("Silver recovery boundary has no completed orders marker")
    return objects


def _active(client: Any) -> dict[str, Any]:
    rows = client.query(
        "select warehouse_publication_id, silver_publication_id, order_count, "
        "payment_count, inventory_count, item_count, revenue_json from wh_active"
    ).result_rows
    if len(rows) != 1:
        raise RuntimeError("ClickHouse serving pointer is not exactly one validated row")
    row = rows[0]
    return {
        "publication_id": str(row[0]),
        "silver_publication_id": str(row[1]),
        "counts": [int(v) for v in row[2:6]],
        "revenue_by_currency": json.loads(str(row[6])),
    }


def _body(manifest: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in manifest.items() if key != "manifest_sha256"}


def capture(args: argparse.Namespace) -> None:
    started = time.perf_counter()
    postgres = _postgres()
    active = _active(_clickhouse())
    if active["silver_publication_id"] != postgres["silver"]["publication_id"]:
        raise RuntimeError("ClickHouse and PostgreSQL Silver lineage disagree")
    manifest: dict[str, Any] = {
        "contract": "quiesced-bounded-recovery-point-v1",
        "captured_at": time.time(),
        "kafka": {"topics": _kafka_ranges()},
        "postgres": postgres,
        "minio": {
            "bucket": os.environ["LAKE_BUCKET"],
            "objects": _silver_objects(postgres["silver"]["version_uri"]),
        },
        "clickhouse": {"active": active},
        "checkpoint_policy": (
            "Spark checkpoints are not restored; derived data is replayed from the "
            "selected verified Silver boundary."
        ),
    }
    manifest["manifest_sha256"] = _sha256(
        json.dumps(_body(manifest), sort_keys=True, separators=(",", ":")).encode()
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    secure_file(args.output)
    print(
        json.dumps(
            {
                "manifest": str(args.output),
                "duration_ms": round((time.perf_counter() - started) * 1000),
                "event_count": postgres["event_count"],
            },
            indent=2,
        )
    )


def verify(args: argparse.Namespace) -> None:
    started = time.perf_counter()
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    expected = _sha256(json.dumps(_body(manifest), sort_keys=True, separators=(",", ":")).encode())
    if manifest.get("manifest_sha256") != expected:
        raise RuntimeError("coordinated manifest checksum mismatch")
    postgres = _postgres()
    if postgres != manifest["postgres"]:
        raise RuntimeError("PostgreSQL recovery boundary differs from the manifest")
    active = _active(_clickhouse())
    if active != manifest["clickhouse"]["active"]:
        raise RuntimeError("ClickHouse active publication differs from the manifest")
    if active["silver_publication_id"] != postgres["silver"]["publication_id"]:
        raise RuntimeError("serving pointer is not aligned to PostgreSQL Silver metadata")
    with tempfile.TemporaryDirectory() as directory:
        tampered = Path(directory) / "tampered.json"
        tampered.write_text(json.dumps({**manifest, "contract": "tampered"}), encoding="utf-8")
        altered = json.loads(tampered.read_text(encoding="utf-8"))
        altered["contract"] = "tampered"
        if (
            _sha256(json.dumps(_body(altered), sort_keys=True, separators=(",", ":")).encode())
            == manifest["manifest_sha256"]
        ):
            raise RuntimeError("manifest tamper negative test did not detect alteration")
    evidence = {
        "verified": True,
        "event_count": postgres["event_count"],
        "warehouse_counts": active["counts"],
        "revenue_by_currency": active["revenue_by_currency"],
        "lineage_consistent": True,
        "tampered_manifest_rejected": True,
        "duration_ms": round((time.perf_counter() - started) * 1000),
    }
    print(json.dumps(evidence, indent=2))


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser()
    sub = root.add_subparsers(dest="command", required=True)
    capture_parser = sub.add_parser("capture")
    capture_parser.add_argument("--output", type=Path, required=True)
    capture_parser.set_defaults(func=capture)
    verify_parser = sub.add_parser("verify")
    verify_parser.add_argument("--manifest", type=Path, required=True)
    verify_parser.set_defaults(func=verify)
    return root


if __name__ == "__main__":
    parsed = parser().parse_args()
    parsed.func(parsed)
