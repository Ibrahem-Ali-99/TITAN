"""Insert a small deterministic PostgreSQL DR verification fixture."""

from datetime import UTC, datetime
from uuid import UUID, uuid5

import psycopg
from psycopg.types.json import Jsonb

NAMESPACE = UUID("2b7e1516-5d2e-4f5a-9f3b-8a3f0f5b9f11")


def main() -> None:
    import os

    dsn = os.environ["DATABASE_URL"]
    now = datetime.now(UTC)
    with psycopg.connect(dsn) as connection:
        for index, (currency, amount) in enumerate(
            (("EUR", "12.50"), ("GBP", "20.00"), ("USD", "31.25"))
        ):
            event_id = uuid5(NAMESPACE, f"event-{index}")
            envelope = {
                "event_id": str(event_id),
                "event_type": "order.created",
                "schema_version": 1,
                "payload": {
                    "order_id": f"dr-order-{index}",
                    "currency": currency,
                    "total_amount": amount,
                },
            }
            connection.execute(
                """INSERT INTO commerce_events
                   (event_id,event_type,schema_version,order_id,business_key,disposition,
                    payload_fingerprint,occurred_at,ingested_at,source,event_envelope,
                    kafka_topic,kafka_partition,kafka_offset,kafka_key)
                   VALUES (%s,'order.created',1,%s,%s,'accepted',%s,%s,%s,
                           'dr-fixture',%s,'dr-events',0,%s,%s)""",
                (
                    event_id,
                    f"dr-order-{index}",
                    f"dr-order-{index}",
                    "fixture",
                    now,
                    now,
                    Jsonb(envelope),
                    index,
                    str(event_id),
                ),
            )
        connection.execute(
            """INSERT INTO ingestion_observations
               (observed_at,event_id,outcome,kafka_topic,kafka_partition,kafka_offset)
               VALUES (%s,%s,'duplicate_delivery','dr-events',0,100)""",
            (now, uuid5(NAMESPACE, "event-0")),
        )
        connection.execute(
            """INSERT INTO ingestion_quarantine
               (received_at,kafka_topic,kafka_partition,kafka_offset,error_type,error_detail,replay_status)
               VALUES (%s,'dr-events',0,101,'malformed_json','fixture','not_replayable')""",
            (now,),
        )
        connection.execute(
            """INSERT INTO spark_job_status
               (job_name,state,last_batch_id,last_batch_at,last_offsets,updated_at)
               VALUES ('dr-fixture','completed',1,%s,'dr-events:0:3',%s)""",
            (now, now),
        )
    print("fixture inserted: 3 events, 1 duplicate observation, 1 quarantine row")


if __name__ == "__main__":
    main()
