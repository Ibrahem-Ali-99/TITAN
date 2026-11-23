"""Seed a deterministic multi-partition Kafka DR fixture."""

from __future__ import annotations

import argparse

from confluent_kafka import Producer, admin


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--partitions", type=int, default=3)
    args = parser.parse_args()
    client = admin.AdminClient({"bootstrap.servers": args.bootstrap})
    result = client.create_topics(
        [admin.NewTopic(args.topic, args.partitions, 1)]  # type: ignore[attr-defined]
    )
    try:
        result[args.topic].result(10)
    except Exception as exc:
        if "TOPIC_ALREADY_EXISTS" not in str(exc):
            raise
    producer = Producer({"bootstrap.servers": args.bootstrap, "acks": "all"})
    for partition in range(args.partitions):
        for index in range(3):
            producer.produce(
                args.topic,
                key=f"order-{partition}-{index}".encode(),
                value=f'{{"event_id":"dr-{partition}-{index}","valid":true}}'.encode(),
                headers=[("schema", b"1"), ("fixture", b"kafka-dr")],
                partition=partition,
            )
    producer.flush(30)
    print("fixture records: 9")


if __name__ == "__main__":
    main()
