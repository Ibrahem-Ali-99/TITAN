"""Kafka producer with broker acknowledgements and bounded retries.

The record key is the order id so later order-scoped events stay ordered
per order. Idempotent produce (acks=all) avoids duplicates caused by
producer retries. It does not make the database write exactly-once.
"""

import time
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from confluent_kafka import Producer

from event_generator.admin import ensure_order_topic
from titan_contracts.events import OrderCreatedEvent
from titan_contracts.topics import ORDER_EVENTS_TOPIC


class PublishError(RuntimeError):
    """The broker did not acknowledge the event within the retry budget."""


@dataclass(frozen=True)
class PublishResult:
    event_id: UUID
    order_id: str
    topic: str
    partition: int
    offset: int


def connect_producer(bootstrap_servers: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "titan-event-generator",
            "acks": "all",
            "enable.idempotence": True,
            "retries": 5,
            "retry.backoff.ms": 200,
            "delivery.timeout.ms": 30000,
            "linger.ms": 5,
            "allow.auto.create.topics": False,
        }
    )


def publish_one(
    producer: Producer, event: OrderCreatedEvent, max_attempts: int = 3
) -> PublishResult:
    payload = event.model_dump_json().encode("utf-8")
    key = event.payload.order_id.encode("utf-8")
    last_error = "delivery was not acknowledged"
    for attempt in range(1, max_attempts + 1):
        box: dict[str, Any] = {}

        def on_delivery(err: object, msg: Any, box: dict[str, Any] = box) -> None:
            box["err"] = err
            box["msg"] = msg

        try:
            producer.produce(
                topic=ORDER_EVENTS_TOPIC,
                key=key,
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
                order_id=event.payload.order_id,
                topic=ORDER_EVENTS_TOPIC,
                partition=int(message.partition()),
                offset=int(message.offset()),
            )
        last_error = str(error) if error is not None else f"{remaining} message(s) still buffered"
        time.sleep(min(2**attempt, 8))
    raise PublishError(f"event {event.event_id} was not acknowledged: {last_error}")


def publish_events(
    bootstrap_servers: str,
    events: list[OrderCreatedEvent],
    *,
    max_attempts: int = 3,
) -> list[PublishResult]:
    if not events:
        return []
    ensure_order_topic(bootstrap_servers)
    producer = connect_producer(bootstrap_servers)
    try:
        return [publish_one(producer, event, max_attempts) for event in events]
    finally:
        producer.flush(10)
