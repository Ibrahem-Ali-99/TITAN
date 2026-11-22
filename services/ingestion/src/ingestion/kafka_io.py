"""confluent-kafka adapter. Automatic offset commits are disabled."""

import logging
import time
from collections.abc import Callable
from typing import Any

from confluent_kafka import Consumer, KafkaError

from ingestion.errors import CommitFailed, PartitionLost
from ingestion.runtime import Polled

logger = logging.getLogger(__name__)


class ConfluentRecord:
    def __init__(self, message: Any) -> None:
        self._message = message

    def topic(self) -> str:
        return str(self._message.topic())

    def partition(self) -> int:
        return int(self._message.partition())

    def offset(self) -> int:
        return int(self._message.offset())

    def value(self) -> bytes | None:
        raw = self._message.value()
        if raw is None:
            return None
        if isinstance(raw, bytes):
            return raw
        return bytes(raw)

    def key(self) -> bytes | None:
        raw = self._message.key()
        if raw is None:
            return None
        if isinstance(raw, bytes):
            return raw
        return bytes(raw)

    def error(self) -> str | None:
        err = self._message.error()
        if err is None:
            return None
        if err.code() == KafkaError._PARTITION_EOF:
            return None
        return str(err)


def retain_prefetched(current: Polled, message: Polled | None, buffer: list[Polled]) -> None:
    """Hold a later record that arrived during backoff.

    The failed record stays in memory and is retried by the caller. Seeking to a
    prefetched offset is unsafe: librdkafka seek targets one position, so seeking
    each queued record leaves the partition at the last offset and drops the
    earlier ones.
    """
    if message is None or message.error() is not None:
        return
    if (
        message.topic() == current.topic()
        and message.partition() == current.partition()
        and message.offset() == current.offset()
    ):
        return
    buffer.append(message)


def drop_revoked(buffer: list[Polled], revoked: set[tuple[str, int]]) -> list[Polled]:
    """Drop buffered records for partitions this consumer no longer owns.

    Those records stay uncommitted. The new owner reads them from the last
    committed offset, which preserves per-partition order.
    """
    return [item for item in buffer if (item.topic(), item.partition()) not in revoked]


class ConfluentConsumer:
    def __init__(self, bootstrap_servers: str, group_id: str, topics: list[str]) -> None:
        self._prefetched: list[Polled] = []
        self._revoked: set[tuple[str, int]] = set()
        self._consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "client.id": "titan-ingestion",
                "enable.auto.commit": False,
                "enable.auto.offset.store": False,
                "auto.offset.reset": "earliest",
                "allow.auto.create.topics": False,
                "session.timeout.ms": 45000,
                "max.poll.interval.ms": 600000,
            }
        )
        self._consumer.subscribe(
            topics,
            on_assign=self._on_assign,  # type: ignore[arg-type]
            on_revoke=self._on_revoke,  # type: ignore[arg-type]
        )

    def _on_assign(self, _consumer: object, partitions: list[object]) -> None:
        for partition in partitions:
            topic = getattr(partition, "topic", None)
            number = getattr(partition, "partition", None)
            if isinstance(topic, str) and isinstance(number, int):
                self._revoked.discard((topic, number))

    def _on_revoke(self, _consumer: object, partitions: list[object]) -> None:
        revoked: set[tuple[str, int]] = set()
        for partition in partitions:
            topic = getattr(partition, "topic", None)
            number = getattr(partition, "partition", None)
            if isinstance(topic, str) and isinstance(number, int):
                revoked.add((topic, number))
        self._revoked.update(revoked)
        self._prefetched = drop_revoked(self._prefetched, revoked)

    def poll(self, timeout: float) -> Polled | None:
        if self._prefetched:
            return self._prefetched.pop(0)
        message = self._consumer.poll(timeout)
        if message is None:
            return None
        err = message.error()
        if err is not None and err.code() == KafkaError._PARTITION_EOF:
            return None
        return ConfluentRecord(message)

    def commit(self, record: Polled) -> None:
        if (record.topic(), record.partition()) in self._revoked:
            raise PartitionLost(
                f"{record.topic()}[{record.partition()}] was revoked before the offset commit"
            )
        if not isinstance(record, ConfluentRecord):
            raise CommitFailed("offset commit received an unexpected record type")
        try:
            self._consumer.commit(message=record._message, asynchronous=False)
        except Exception as exc:
            raise CommitFailed(str(exc)) from exc

    def recover(
        self, record: Polled, delay_seconds: float, should_stop: Callable[[], bool]
    ) -> None:
        """Back off without acknowledging, and keep the group session alive.

        Assigned partitions are paused. Polling continues so ``max.poll.interval.ms``
        stays fresh; librdkafka sends heartbeats on its own thread. Records that
        were already fetched are buffered in order and returned by the next
        ``poll`` calls. They are not seeked.
        """
        assigned = self._consumer.assignment()
        if assigned:
            self._consumer.pause(assigned)
        deadline = time.monotonic() + delay_seconds
        try:
            while time.monotonic() < deadline:
                if should_stop():
                    return
                remaining = deadline - time.monotonic()
                unexpected = self._consumer.poll(min(0.2, max(remaining, 0.0)))
                if unexpected is None:
                    continue
                err = unexpected.error()
                if err is not None:
                    if err.code() != KafkaError._PARTITION_EOF:
                        logger.warning(
                            "kafka poll during backoff returned an error",
                            extra={"error": str(err)},
                        )
                    continue
                topic = unexpected.topic()
                partition = unexpected.partition()
                offset = unexpected.offset()
                if topic is None or partition is None or offset is None:
                    continue
                if (topic, partition) in self._revoked:
                    continue
                retain_prefetched(record, ConfluentRecord(unexpected), self._prefetched)
        finally:
            if assigned:
                self._consumer.resume(assigned)

    def close(self) -> None:
        self._consumer.close()
