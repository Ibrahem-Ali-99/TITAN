from collections.abc import Callable
from datetime import UTC, datetime

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from event_generator.generator import generate_orders
from ingestion.errors import PartitionLost, TransientStorageError
from ingestion.kafka_io import drop_revoked, retain_prefetched
from ingestion.positions import KafkaPosition
from ingestion.runtime import Polled, run_consumer
from ingestion.store import PersistOutcome, SqlEventStore
from titan_contracts.events import CommerceEvent
from titan_store.db import create_db_engine, create_session_factory
from titan_store.models import Base, QuarantineRecord
from titan_store.models import CommerceEvent as StoredEvent


class FakeRecord:
    def __init__(
        self,
        value: bytes | None,
        *,
        offset: int,
        key: bytes | None,
        error: str | None = None,
        topic: str = "titan.commerce.order-events",
        partition: int = 0,
    ) -> None:
        self._value = value
        self._offset = offset
        self._key = key
        self._error = error
        self._topic = topic
        self._partition = partition

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def value(self) -> bytes | None:
        return self._value

    def key(self) -> bytes | None:
        return self._key

    def error(self) -> str | None:
        return self._error


class FakeConsumer:
    def __init__(self, records: list[FakeRecord], trace: list[str]) -> None:
        self._queue = records
        self.trace = trace
        self.closed = False

    def poll(self, timeout: float) -> FakeRecord | None:
        if not self._queue:
            return None
        return self._queue.pop(0)

    def commit(self, record: Polled) -> None:
        self.trace.append(f"commit:{record.offset()}")

    def recover(
        self, record: Polled, delay_seconds: float, should_stop: Callable[[], bool]
    ) -> None:
        del should_stop
        self.trace.append(f"recover:{record.offset()}")

    def close(self) -> None:
        self.closed = True


class MemoryStore:
    def __init__(self, trace: list[str], failures: int = 0) -> None:
        self.trace = trace
        self.failures = failures
        self.events: dict[object, object] = {}
        self.quarantined_types: list[str] = []

    def persist(
        self, event: CommerceEvent, position: KafkaPosition, ingested_at: datetime
    ) -> PersistOutcome:
        del position, ingested_at
        if self.failures:
            self.failures -= 1
            self.trace.append("persist-fail")
            raise TransientStorageError("database is down")
        event_id = event.event_id
        if event_id in self.events:
            self.trace.append("persist-duplicate")
            return PersistOutcome.DUPLICATE
        self.events[event_id] = event
        self.trace.append("persist-ok")
        return PersistOutcome.INSERTED

    def quarantine(
        self,
        position: KafkaPosition,
        raw_value: str | None,
        error_type: str,
        error_detail: str,
        received_at: datetime | None = None,
        *,
        source: str | None = None,
        event_id: object = None,
        event_type: str | None = None,
        schema_version: int | None = None,
        replay_status: str = "not_replayable",
    ) -> PersistOutcome:
        del position, raw_value, error_detail, received_at
        del source, event_id, event_type, schema_version, replay_status
        if self.failures:
            self.failures -= 1
            self.trace.append("quarantine-fail")
            raise TransientStorageError("database is down")
        self.quarantined_types.append(error_type)
        self.trace.append(f"quarantine:{error_type}")
        return PersistOutcome.INSERTED


def _run(records: list[FakeRecord], store: MemoryStore, trace: list[str]) -> FakeConsumer:
    consumer = FakeConsumer(records, trace)
    run_consumer(
        consumer,
        store,
        should_stop=lambda: False,
        heartbeat=lambda: None,
        max_idle_polls=1,
        max_recoveries=5,
    )
    return consumer


def test_offset_is_committed_only_after_persist() -> None:
    event = generate_orders(1, seed=3, deterministic_event_ids=True)[0]
    trace: list[str] = []
    store = MemoryStore(trace)
    record = FakeRecord(
        event.model_dump_json().encode(),
        offset=7,
        key=event.payload.order_id.encode(),
    )
    _run([record], store, trace)
    assert trace == ["persist-ok", "commit:7"]
    assert len(store.events) == 1


def test_duplicate_delivery_commits_without_a_second_row() -> None:
    event = generate_orders(1, seed=3, deterministic_event_ids=True)[0]
    payload = event.model_dump_json().encode()
    key = event.payload.order_id.encode()
    trace: list[str] = []
    store = MemoryStore(trace)
    _run(
        [
            FakeRecord(payload, offset=1, key=key),
            FakeRecord(payload, offset=2, key=key),
        ],
        store,
        trace,
    )
    assert trace == ["persist-ok", "commit:1", "persist-duplicate", "commit:2"]
    assert len(store.events) == 1


def test_database_outage_does_not_acknowledge_until_persist_succeeds() -> None:
    event = generate_orders(1, seed=9, deterministic_event_ids=True)[0]
    trace: list[str] = []
    store = MemoryStore(trace, failures=1)
    _run(
        [
            FakeRecord(
                event.model_dump_json().encode(), offset=4, key=event.payload.order_id.encode()
            )
        ],
        store,
        trace,
    )
    assert trace == ["persist-fail", "recover:4", "persist-ok", "commit:4"]
    assert len(store.events) == 1


def test_invalid_record_is_quarantined_and_does_not_block_the_next_record() -> None:
    event = generate_orders(1, seed=11, deterministic_event_ids=True)[0]
    trace: list[str] = []
    store = MemoryStore(trace)
    _run(
        [
            FakeRecord(b"{not-json", offset=1, key=b"ord_abcdef123456"),
            FakeRecord(
                event.model_dump_json().encode(), offset=2, key=event.payload.order_id.encode()
            ),
        ],
        store,
        trace,
    )
    assert trace[0].startswith("quarantine:")
    assert "commit:1" in trace
    assert trace[-2:] == ["persist-ok", "commit:2"]
    assert store.quarantined_types
    assert len(store.events) == 1


def test_quarantine_failure_does_not_acknowledge() -> None:
    trace: list[str] = []
    store = MemoryStore(trace, failures=1)
    _run([FakeRecord(b"not-json", offset=8, key=b"ord_abcdef123456")], store, trace)
    assert trace == ["quarantine-fail", "recover:8", "quarantine:malformed_json", "commit:8"]


def test_sqlite_persistence_is_idempotent_by_event_id() -> None:
    engine = create_db_engine("sqlite://")
    Base.metadata.create_all(engine)
    store = SqlEventStore(create_session_factory(engine))
    event = generate_orders(1, seed=5, deterministic_event_ids=True)[0]
    now = datetime.now(UTC)
    first = KafkaPosition("titan.commerce.order-events", 0, 10, event.payload.order_id)
    second = KafkaPosition("titan.commerce.order-events", 0, 11, event.payload.order_id)
    assert store.persist(event, first, now) is PersistOutcome.INSERTED
    assert store.persist(event, second, now) is PersistOutcome.DUPLICATE
    with Session(engine) as session:
        assert session.scalar(select(func.count()).select_from(StoredEvent)) == 1
    assert store.quarantine(first, "{", "malformed_json", "bad", now) is PersistOutcome.INSERTED
    assert store.quarantine(first, "{", "malformed_json", "bad", now) is PersistOutcome.DUPLICATE
    with Session(engine) as session:
        assert session.scalar(select(func.count()).select_from(QuarantineRecord)) == 1


def test_partition_loss_does_not_acknowledge_or_retry() -> None:
    event = generate_orders(1, seed=13, deterministic_event_ids=True)[0]
    trace: list[str] = []
    store = MemoryStore(trace)

    class LosingConsumer(FakeConsumer):
        def commit(self, record: Polled) -> None:
            self.trace.append(f"commit-lost:{record.offset()}")
            raise PartitionLost("revoked")

    consumer = LosingConsumer(
        [
            FakeRecord(
                event.model_dump_json().encode(), offset=3, key=event.payload.order_id.encode()
            )
        ],
        trace,
    )
    run_consumer(
        consumer,
        store,
        should_stop=lambda: False,
        heartbeat=lambda: None,
        max_idle_polls=1,
        max_recoveries=5,
    )
    assert trace == ["persist-ok", "commit-lost:3"]
    assert consumer.closed is True


def test_revoked_partitions_drop_buffered_records() -> None:
    current = FakeRecord(b"{}", offset=5, key=b"ord_current0001")
    same_partition = FakeRecord(b"{}", offset=6, key=b"ord_later000001")
    other = FakeRecord(b"{}", offset=1, key=b"ord_other000001", partition=1)
    kept = drop_revoked([current, same_partition, other], {("titan.commerce.order-events", 0)})
    assert [record.offset() for record in kept] == [1]
    assert kept[0].partition() == 1


def test_backoff_keeps_prefetched_records_in_order_without_seeking() -> None:
    current = FakeRecord(b"{}", offset=5, key=b"ord_current0001")
    later = FakeRecord(b"{}", offset=6, key=b"ord_later000001")
    other_partition = FakeRecord(b"{}", offset=1, key=b"ord_other000001", partition=1)
    same = FakeRecord(b"{}", offset=5, key=b"ord_current0001")
    errored = FakeRecord(b"{}", offset=7, key=b"ord_error000001", error="broker down")
    buffer: list[Polled] = []
    retain_prefetched(current, None, buffer)
    retain_prefetched(current, errored, buffer)
    retain_prefetched(current, same, buffer)
    retain_prefetched(current, later, buffer)
    retain_prefetched(current, other_partition, buffer)
    assert [record.offset() for record in buffer] == [6, 1]
    assert buffer[1].partition() == 1
