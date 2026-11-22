"""Topic setup for the generator. Matches the ingestion service configuration."""

import time
from typing import Any

from confluent_kafka import admin as kafka_admin

from titan_contracts.topics import (
    ORDER_EVENTS_PARTITIONS,
    ORDER_EVENTS_RETENTION_MS,
    ORDER_EVENTS_TOPIC,
)


def _new_topic(name: str, partitions: int, retention_ms: int) -> Any:
    # The published stubs do not export NewTopic. The class is present at runtime.
    topic_type = kafka_admin.NewTopic  # type: ignore[attr-defined]
    return topic_type(
        name,
        num_partitions=partitions,
        replication_factor=1,
        config={"retention.ms": str(retention_ms)},
    )


def ensure_order_topic(bootstrap_servers: str, *, attempts: int = 5) -> None:
    admin = kafka_admin.AdminClient({"bootstrap.servers": bootstrap_servers})
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            metadata = admin.list_topics(timeout=10)
            topic = metadata.topics.get(ORDER_EVENTS_TOPIC)
            if topic is not None and topic.error is None:
                return
            created = admin.create_topics(
                [_new_topic(ORDER_EVENTS_TOPIC, ORDER_EVENTS_PARTITIONS, ORDER_EVENTS_RETENTION_MS)]
            )
            created[ORDER_EVENTS_TOPIC].result(timeout=30)
            return
        except Exception as exc:
            if "TOPIC_ALREADY_EXISTS" in str(exc) or "already exists" in str(exc).lower():
                return
            last_error = exc
            time.sleep(min(2**attempt, 8))
    raise RuntimeError(f"could not ensure topic {ORDER_EVENTS_TOPIC}") from last_error
