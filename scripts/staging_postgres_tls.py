"""Provision verified PostgreSQL TLS material into a fresh staging volume."""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path

POSTGRES_IMAGE = "postgres@sha256:339a86bf416979bb4d3d2e30a1f9e096e7302564faff4151b804cf53463059d5"
REQUIRED = ("ca.crt", "postgres.crt", "postgres.key")
PG_HBA = "\n".join(
    (
        "local all all trust",
        "hostssl all all 0.0.0.0/0 scram-sha-256",
        "host all all 0.0.0.0/0 reject",
        "",
    )
)


def _run(args: list[str]) -> None:
    subprocess.run(args, check=True, capture_output=True, text=True)


def verify_source(source: Path) -> None:
    missing = [name for name in REQUIRED if not (source / name).is_file()]
    if missing:
        raise ValueError("PostgreSQL TLS material is incomplete: " + ", ".join(missing))
    _run(
        [
            "openssl",
            "verify",
            "-CAfile",
            str(source / "ca.crt"),
            "-verify_hostname",
            "postgres",
            str(source / "postgres.crt"),
        ]
    )
    _run(["openssl", "x509", "-in", str(source / "postgres.crt"), "-checkend", "0", "-noout"])
    cert_pub = subprocess.run(
        ["openssl", "x509", "-in", str(source / "postgres.crt"), "-pubkey", "-noout"],
        check=True,
        capture_output=True,
    ).stdout
    key_pub = subprocess.run(
        ["openssl", "pkey", "-in", str(source / "postgres.key"), "-pubout"],
        check=True,
        capture_output=True,
    ).stdout
    if cert_pub != key_pub:
        raise ValueError("PostgreSQL certificate and private key do not match")


def seed_volume(source: Path, volume: str) -> None:
    source = source.resolve()
    verify_source(source)
    hba = source / "pg_hba.conf"
    hba.write_text(PG_HBA, encoding="ascii")
    try:
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
                POSTGRES_IMAGE,
                "-c",
                'set -eu; test -z "$(find /out -mindepth 1 -maxdepth 1 -print -quit)"; '
                "cp /in/ca.crt /in/postgres.crt /in/postgres.key /in/pg_hba.conf /out/; "
                "chown postgres:postgres /out/postgres.key; chmod 600 /out/postgres.key; "
                "chmod 644 /out/ca.crt /out/postgres.crt /out/pg_hba.conf; "
                "test -s /out/pg_hba.conf",
            ]
        )
    finally:
        hba.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--volume", required=True)
    args = parser.parse_args()
    try:
        seed_volume(args.source, args.volume)
    except (OSError, ValueError, subprocess.CalledProcessError) as exc:
        print(f"PostgreSQL TLS provisioning failed: {exc}")
        return 1
    print("PostgreSQL TLS volume seeded and verified; private material is not printed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
