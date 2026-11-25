"""Provision verified MinIO HTTPS material into a fresh staging volume."""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path

from scripts.staging_tool_container import run as run_tool

MINIO_IMAGE = "alpine/minio@sha256:cf23643a6cf9ce159c57643ceb88279e431262282428c9e0bf3a7ef1a97e84b4"
REQUIRED = ("ca.crt", "minio.crt", "minio.key")


def _run(args: list[str]) -> None:
    run_tool(args)


def verify_source(source: Path) -> None:
    missing = [name for name in REQUIRED if not (source / name).is_file()]
    if missing:
        raise ValueError("MinIO TLS material is incomplete: " + ", ".join(missing))
    _run(
        [
            "openssl",
            "verify",
            "-CAfile",
            str(source / "ca.crt"),
            "-verify_hostname",
            "minio",
            str(source / "minio.crt"),
        ]
    )
    _run(["openssl", "x509", "-in", str(source / "minio.crt"), "-checkend", "0", "-noout"])
    cert_pub = run_tool(
        ["openssl", "x509", "-in", str(source / "minio.crt"), "-pubkey", "-noout"]
    ).stdout
    key_pub = run_tool(["openssl", "pkey", "-in", str(source / "minio.key"), "-pubout"]).stdout
    if cert_pub != key_pub:
        raise ValueError("MinIO certificate and private key do not match")


def seed_volume(source: Path, volume: str) -> None:
    source = source.resolve()
    verify_source(source)
    _run(
        [
            "docker",
            "run",
            "--rm",
            "--user",
            "0",
            "--entrypoint",
            "sh",
            "-v",
            f"{volume}:/out",
            "-v",
            f"{source}:/in:ro",
            MINIO_IMAGE,
            "-c",
            'set -eu; test -z "$(find /out -mindepth 1 -maxdepth 1 -print -quit)"; '
            "mkdir -p /out/CAs; cp /in/ca.crt /out/CAs/ca.crt; cp /in/ca.crt /out/ca.crt; "
            "cp /in/minio.crt /out/public.crt; cp /in/minio.key /out/private.key; "
            "chown 100:101 /out/private.key; chown 100:101 /out/public.crt; "
            "chown 100:101 /out/CAs/ca.crt /out/ca.crt; "
            "chmod 644 /out/CAs/ca.crt /out/ca.crt /out/public.crt; "
            "chmod 600 /out/private.key",
        ]
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--volume", required=True)
    args = parser.parse_args()
    try:
        seed_volume(args.source, args.volume)
    except (OSError, ValueError, subprocess.CalledProcessError) as exc:
        print(f"MinIO TLS provisioning failed: {exc}")
        return 1
    print("MinIO TLS volume seeded and verified; private material is not printed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
