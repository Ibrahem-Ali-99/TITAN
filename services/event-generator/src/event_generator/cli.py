"""Publish N synthetic order_created events and require broker acknowledgements."""

import argparse
import os
import sys
import time
from datetime import timedelta

from event_generator.admin import ensure_order_topic
from event_generator.generator import generate_orders
from event_generator.producer import PublishError, connect_producer, publish_one


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Publish synthetic order_created events to Kafka.")
    parser.add_argument("--count", type=int, required=True, help="Number of events to publish")
    parser.add_argument("--seed", type=int, required=True, help="Seed for business payloads")
    parser.add_argument(
        "--rate",
        type=float,
        default=5.0,
        help="Maximum events per second. 0 publishes as fast as acknowledgements allow.",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    parser.add_argument(
        "--deterministic-event-ids",
        action="store_true",
        help=(
            "Derive event ids from the seed. "
            "Omit this for normal runs so each run is a new delivery."
        ),
    )
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument(
        "--occurred-span-hours",
        type=float,
        default=None,
        help="Spread occurred_at from 2026-01-01 across this many hours.",
    )
    parser.add_argument(
        "--late-every",
        type=int,
        default=None,
        help="Move the last event of each group earlier. Requires --late-hours.",
    )
    parser.add_argument("--late-hours", type=float, default=None)
    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    if args.count < 1 or args.count > 100_000:
        print("count must be between 1 and 100000", file=sys.stderr)
        sys.exit(2)
    if args.rate < 0:
        print("rate must be >= 0", file=sys.stderr)
        sys.exit(2)
    span = None if args.occurred_span_hours is None else timedelta(hours=args.occurred_span_hours)
    late = None if args.late_hours is None else timedelta(hours=args.late_hours)
    events = generate_orders(
        args.count,
        args.seed,
        deterministic_event_ids=args.deterministic_event_ids,
        occurred_span=span,
        late_every=args.late_every,
        late_by=late,
    )
    interval = 0.0 if args.rate == 0 else 1.0 / args.rate
    results = []
    try:
        ensure_order_topic(args.bootstrap_servers)
        producer = connect_producer(args.bootstrap_servers)
        try:
            for event in events:
                started = time.monotonic()
                results.append(publish_one(producer, event, max_attempts=args.max_attempts))
                elapsed = time.monotonic() - started
                if interval > elapsed:
                    time.sleep(interval - elapsed)
        finally:
            producer.flush(10)
    except PublishError as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        print(f"publish failed: {exc}", file=sys.stderr)
        sys.exit(1)
    for result in results:
        print(
            f"acknowledged event_id={result.event_id} order_id={result.order_id} "
            f"topic={result.topic} partition={result.partition} offset={result.offset}"
        )


if __name__ == "__main__":
    main()
