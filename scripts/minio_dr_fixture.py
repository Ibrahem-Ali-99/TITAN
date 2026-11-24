"""Create deterministic Bronze/Silver Parquet objects in a disposable MinIO bucket."""

from __future__ import annotations

import io
import os

import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio


def parquet_bytes(table: pa.Table) -> bytes:
    stream = io.BytesIO()
    pq.write_table(table, stream, compression="snappy")
    return stream.getvalue()


def main() -> None:
    endpoint = os.environ.get("MINIO_ENDPOINT", "127.0.0.1:9000")
    secure = os.environ.get("MINIO_SECURE", "false").lower() == "true"
    client = Minio(
        endpoint,
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        secure=secure,
        cert_check=True,
    )
    bucket = os.environ.get("LAKE_BUCKET", "titan-lake")
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
    bronze = pa.table(
        {
            "event_id": ["dr-event-1", "dr-event-2"],
            "kafka_offset": [10, 11],
            "parse_status": ["valid", "valid"],
        }
    )
    silver = pa.table(
        {
            "event_id": ["dr-event-1", "dr-event-2"],
            "order_id": ["dr-order-1", "dr-order-2"],
            "currency": ["EUR", "USD"],
            "amount": ["12.50", "31.25"],
        }
    )
    objects = {
        "bronze/commerce_events/batch_id=7/part-00000.parquet": parquet_bytes(bronze),
        "bronze/commerce_events/batch_id=7/_SUCCESS": b"",
        "silver/versions/dr-publication/orders/part-00000.parquet": parquet_bytes(silver),
        "silver/versions/dr-publication/_SUCCESS": b"",
        "silver/current/README": (
            b"convenience pointer; PostgreSQL lake_publications remains authoritative\n"
        ),
    }
    for key, data in objects.items():
        client.put_object(bucket, key, io.BytesIO(data), length=len(data))
    print(f"fixture inserted: {len(objects)} objects, 2 bronze rows, 2 silver rows")


if __name__ == "__main__":
    main()
