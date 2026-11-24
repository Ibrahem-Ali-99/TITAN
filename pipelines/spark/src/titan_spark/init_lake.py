"""Create the lake bucket. Running this again does not recreate it."""

import os
import sys


def ensure_bucket() -> str:
    from minio import Minio

    endpoint = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
    endpoint = endpoint.removeprefix("http://").removeprefix("https://")
    secure = os.environ.get("MINIO_SECURE", "false").lower() == "true"
    bucket = os.environ.get("LAKE_BUCKET", "titan-lake")
    client = Minio(
        endpoint,
        access_key=os.environ.get("MINIO_ACCESS_KEY", os.environ.get("S3_ACCESS_KEY", "titan")),
        secret_key=os.environ.get(
            "MINIO_SECRET_KEY",
            os.environ.get("S3_SECRET_KEY", "titan_dev_password"),
        ),
        secure=secure,
        cert_check=True,
    )
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
    return bucket


def main() -> None:
    bucket = ensure_bucket()
    print(f"bucket {bucket} ready")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"lake init failed: {exc.__class__.__name__}", file=sys.stderr)
        raise
