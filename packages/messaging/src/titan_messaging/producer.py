"""Producer acknowledgements and bounded retries for any commerce event.

Idempotent produce deduplicates broker retries. It does not make the
PostgreSQL write exactly-once, and a broker acknowledgement is not persistence.
"""

import time
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from confluent_kafka import Producer

from titan_contracts.events import CommerceEvent, business_key
from titan_contracts.topics import topic_for_event_type
from titan_messaging.kafka_config import kafka_client_config


class PublishError(RuntimeError):
    """The broker did not acknowledge the event within the retry budget.

    ``uncertain`` is true when the produce may still have been accepted.
    Callers must not report that case as a definitive broker rejection.
    """

    def __init__(self, message: str, *, uncertain: bool = False) -> None:
        super().__init__(message)
        self.uncertain = uncertain


def acknowledgement_is_uncertain(error: object | None, remaining: int) -> bool:
    """A timeout or a still-buffered produce is not proof the broker rejected it."""
    if remaining > 0 and error is None:
        return True
    text = "" if error is None else str(error).lower()
    markers = ("timed out", "timeout", "transport", "broker transport failure")
    return any(marker in text for marker in markers)


@dataclass(frozen=True)
class PublishResult:
    event_id: UUID
    record_key: str
    topic: str
    partition: int
    offset: int


def connect_producer(bootstrap_servers: str, *, client_id: str = "titan-publisher") -> Producer:
    config = kafka_client_config(bootstrap_servers)
    config.update(
        {
            "client.id": client_id,
            "acks": "all",
            "enable.idempotence": True,
            "retries": 5,
            "retry.backoff.ms": 200,
            "delivery.timeout.ms": 30000,
            "linger.ms": 5,
            "allow.auto.create.topics": False,
        }
    )
    return Producer(config)


def publish_record(
    producer: Producer,
    event: CommerceEvent,
    *,
    max_attempts: int = 3,
) -> PublishResult:
    spec = topic_for_event_type(event.event_type)
    payload = event.model_dump_json().encode("utf-8")
    key = business_key(event)
    last_error = "delivery was not acknowledged"
    uncertain = False
    for attempt in range(1, max_attempts + 1):
        box: dict[str, Any] = {}

        def on_delivery(err: object, msg: Any, box: dict[str, Any] = box) -> None:
            box["err"] = err
            box["msg"] = msg

        try:
            producer.produce(
                topic=spec.name,
                key=key.encode("utf-8"),
                value=payload,
                on_delivery=on_delivery,
            )
        except BufferError as exc:
            producer.poll(0.5)
            last_error = str(exc)
            time.sleep(min(2**attempt, 8))
            continue
        remaining = producer.flush(30)
        error = box.get("err")
        message = box.get("msg")
        if remaining == 0 and error is None and message is not None:
            return PublishResult(
                event_id=event.event_id,
                record_key=key,
                topic=spec.name,
                partition=int(message.partition()),
                offset=int(message.offset()),
            )
        last_error = str(error) if error is not None else f"{remaining} message(s) still buffered"
        uncertain = acknowledgement_is_uncertain(error, remaining)
        time.sleep(min(2**attempt, 8))
    raise PublishError(
        f"event {event.event_id} was not acknowledged: {last_error}",
        uncertain=uncertain,
    )
