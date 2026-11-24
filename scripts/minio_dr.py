"""MinIO object-level backup, verification, and isolated restore."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from minio import Minio

from dr_security import secure_tree

DEFAULT_PREFIXES = ("bronze/", "silver/")
MANIFEST_NAME = "manifest.json"
MARKER_NAME = "_titan_restore_complete.json"


def _client(endpoint: str, access_key: str, secret_key: str, secure: bool) -> Minio:
    return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _objects(client: Minio, bucket: str, prefixes: tuple[str, ...]) -> list[Any]:
    result = []
    for prefix in prefixes:
        result.extend(client.list_objects(bucket, prefix=prefix, recursive=True))
    return sorted(
        {obj.object_name: obj for obj in result}.values(), key=lambda obj: obj.object_name
    )


def _get_bytes(client: Minio, bucket: str, key: str) -> tuple[bytes, dict[str, str]]:
    response = client.get_object(bucket, key)
    try:
        return response.read(), {
            str(k): str(v)
            for k, v in response.headers.items()
            if k.lower().startswith("x-amz-meta-")
        }
    finally:
        response.close()
        response.release_conn()


def backup(args: argparse.Namespace) -> None:
    started = time.perf_counter()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    client = _client(args.endpoint, args.access_key, args.secret_key, args.secure)
    if not client.bucket_exists(args.bucket):
        raise SystemExit(f"source bucket does not exist: {args.bucket}")
    prefixes = tuple(args.prefix or DEFAULT_PREFIXES)
    entries: list[dict[str, Any]] = []
    payload_root = output / "objects"
    for obj in _objects(client, args.bucket, prefixes):
        if obj.object_name.endswith("/") or obj.object_name.startswith(
            tuple(p + ".__temporary__" for p in prefixes)
        ):
            continue
        data, metadata = _get_bytes(client, args.bucket, obj.object_name)
        target = payload_root / obj.object_name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        entries.append(
            {
                "key": obj.object_name,
                "size": len(data),
                "sha256": _sha256(data),
                "metadata": metadata,
            }
        )
    manifest = {
        "backup_id": output.name,
        "created_at": datetime.now(UTC).isoformat(),
        "source_bucket": args.bucket,
        "prefixes": list(prefixes),
        "object_count": len(entries),
        "objects": entries,
        "checkpoint_policy": (
            "Spark checkpoints are local Docker volumes, not MinIO objects; "
            "coordinated Kafka/PostgreSQL recovery is required."
        ),
        "tool": "scripts/minio_dr.py",
    }
    (output / MANIFEST_NAME).write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    manifest["backup_payload_bytes"] = sum(int(entry["size"]) for entry in entries)
    manifest["backup_duration_ms"] = round((time.perf_counter() - started) * 1000)
    (output / MANIFEST_NAME).write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    secure_tree(output)
    print(json.dumps(manifest, indent=2))


def verify_backup(directory: Path) -> dict[str, Any]:
    manifest = json.loads((directory / MANIFEST_NAME).read_text(encoding="utf-8"))
    for entry in manifest["objects"]:
        path = directory / "objects" / str(entry["key"])
        if not path.is_file():
            raise SystemExit(f"backup object missing: {entry['key']}")
        data = path.read_bytes()
        if len(data) != entry["size"] or _sha256(data) != entry["sha256"]:
            raise SystemExit(f"backup checksum mismatch: {entry['key']}")
    return {**manifest, "verified": True}


def verify(args: argparse.Namespace) -> None:
    started = time.perf_counter()
    result = verify_backup(args.backup.resolve())
    result["verification_duration_ms"] = round((time.perf_counter() - started) * 1000)
    print(json.dumps(result, indent=2))


def restore(args: argparse.Namespace) -> None:
    started = time.perf_counter()
    if not (
        args.destination_bucket.endswith("-restore") or args.destination_bucket.endswith("_restore")
    ):
        raise SystemExit("destination bucket must end in -restore or _restore")
    evidence = verify_backup(args.backup.resolve())
    destination = _client(
        args.destination_endpoint or args.endpoint,
        args.destination_access_key or args.access_key,
        args.destination_secret_key or args.secret_key,
        args.destination_secure,
    )
    if destination.bucket_exists(args.destination_bucket):
        if (
            any(destination.list_objects(args.destination_bucket, recursive=True))
            and not args.allow_nonempty
        ):
            raise SystemExit("refusing to restore into a nonempty destination bucket")
    else:
        destination.make_bucket(args.destination_bucket)
    created: list[str] = []
    try:
        for entry in evidence["objects"]:
            data = (args.backup.resolve() / "objects" / str(entry["key"])).read_bytes()
            destination.put_object(
                args.destination_bucket,
                str(entry["key"]),
                io.BytesIO(data),
                length=len(data),
                metadata=entry.get("metadata") or {},
            )
            created.append(str(entry["key"]))
        for entry in evidence["objects"]:
            data, _ = _get_bytes(destination, args.destination_bucket, str(entry["key"]))
            if _sha256(data) != entry["sha256"]:
                raise SystemExit(f"restored checksum mismatch: {entry['key']}")
        marker = {
            "backup_id": evidence["backup_id"],
            "object_count": evidence["object_count"],
            "completed_at": datetime.now(UTC).isoformat(),
        }
        destination.put_object(
            args.destination_bucket,
            MARKER_NAME,
            io.BytesIO(json.dumps(marker).encode()),
            length=len(json.dumps(marker).encode()),
            content_type="application/json",
        )
    except Exception:
        for key in created:
            destination.remove_object(args.destination_bucket, key)
        raise
    print(
        json.dumps(
            {
                "backup_id": evidence["backup_id"],
                "restored_object_count": len(created),
                "restore_duration_ms": round((time.perf_counter() - started) * 1000),
                "marker": MARKER_NAME,
            },
            indent=2,
        )
    )


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    root.add_argument("--endpoint", default=os.getenv("MINIO_ENDPOINT", "127.0.0.1:9000"))
    root.add_argument("--access-key", default=os.getenv("MINIO_ACCESS_KEY", ""))
    root.add_argument("--secret-key", default=os.getenv("MINIO_SECRET_KEY", ""))
    root.add_argument("--secure", action="store_true")
    sub = root.add_subparsers(dest="command", required=True)
    b = sub.add_parser("backup")
    b.add_argument("--bucket", required=True)
    b.add_argument("--output", type=Path, required=True)
    b.add_argument("--prefix", action="append")
    b.set_defaults(func=backup)
    v = sub.add_parser("verify")
    v.add_argument("--backup", type=Path, required=True)
    v.set_defaults(func=verify)
    r = sub.add_parser("restore")
    r.add_argument("--backup", type=Path, required=True)
    r.add_argument("--destination-bucket", required=True)
    r.add_argument("--destination-endpoint")
    r.add_argument("--destination-access-key")
    r.add_argument("--destination-secret-key")
    r.add_argument("--destination-secure", action="store_true")
    r.add_argument("--allow-nonempty", action="store_true")
    r.set_defaults(func=restore)
    return root


if __name__ == "__main__":
    parser_args = parser().parse_args()
    parser_args.func(parser_args)
