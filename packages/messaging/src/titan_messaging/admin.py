"""Idempotent creation of the commerce topics. Auto-creation stays disabled."""

import logging
import time
from typing import Any

from confluent_kafka import admin as kafka_admin

from titan_contracts.topics import COMMERCE_TOPICS, TopicSpec
from titan_messaging.kafka_config import kafka_client_config

logger = logging.getLogger(__name__)


def _new_topic(spec: TopicSpec) -> Any:
    # The published stubs do not export NewTopic. The class is present at runtime.
    topic_type = kafka_admin.NewTopic  # type: ignore[attr-defined]
    return topic_type(
        spec.name,
        num_partitions=spec.partitions,
        replication_factor=1,
        config={"retention.ms": str(spec.retention_ms)},
    )


def ensure_commerce_topics(bootstrap_servers: str, *, attempts: int = 5) -> None:
    """Create any missing commerce topic. Existing topics are left unchanged."""
    client = kafka_admin.AdminClient(kafka_client_config(bootstrap_servers))
    last_error: Exception | None = None
    pending = list(COMMERCE_TOPICS)
    for attempt in range(1, attempts + 1):
        try:
            metadata = client.list_topics(timeout=10)
            missing = [
                spec
                for spec in pending
                if (topic := metadata.topics.get(spec.name)) is None or topic.error is not None
            ]
            if not missing:
                return
            created = client.create_topics([_new_topic(spec) for spec in missing])
            for spec in missing:
                try:
                    created[spec.name].result(timeout=30)
                    logger.info("created kafka topic", extra={"topic": spec.name})
                except Exception as exc:
                    if "TOPIC_ALREADY_EXISTS" in str(exc) or "already exists" in str(exc).lower():
                        continue
                    raise
            return
        except Exception as exc:
            last_error = exc
            logger.warning(
                "topic setup failed; retrying",
                extra={"attempt": attempt, "error": str(exc)},
            )
            time.sleep(min(2**attempt, 8))
    names = ", ".join(spec.name for spec in COMMERCE_TOPICS)
    raise RuntimeError(f"could not ensure topics {names}") from last_error
