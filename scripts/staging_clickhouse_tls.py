"""Provision verified ClickHouse TLS material into a fresh staging volume."""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path

from staging_tool_container import run as run_tool

CLICKHOUSE_IMAGE = (
    "clickhouse/clickhouse-server@sha256:"
    "d426d2f612e1e189c90f9a3f8dcd85a03c6851273833817a296bb327e63aa7a7"
)
REQUIRED_FILES = ("ca.crt", "clickhouse.crt", "clickhouse.key", "config.xml")


def _run(args: list[str]) -> None:
    run_tool(args)


def verify_source(source: Path) -> None:
    missing = [name for name in REQUIRED_FILES if not (source / name).is_file()]
    if missing:
        raise ValueError("ClickHouse TLS material is incomplete: " + ", ".join(missing))
    _run(
        [
            "openssl",
            "verify",
            "-CAfile",
            str(source / "ca.crt"),
            "-verify_hostname",
            "clickhouse",
            str(source / "clickhouse.crt"),
        ]
    )
    _run(["openssl", "x509", "-in", str(source / "clickhouse.crt"), "-checkend", "0", "-noout"])
    cert_key = run_tool(
        ["openssl", "x509", "-in", str(source / "clickhouse.crt"), "-pubkey", "-noout"]
    ).stdout
    key_pub = run_tool(["openssl", "pkey", "-in", str(source / "clickhouse.key"), "-pubout"]).stdout
    if cert_key != key_pub:
        raise ValueError("ClickHouse certificate and private key do not match")


def seed_volume(source: Path, volume: str) -> None:
    source = source.resolve()
    verify_source(source)
    command = [
        "docker",
        "run",
        "--rm",
        "--user",
        "0",
        "-v",
        f"{volume}:/out",
        "-v",
        f"{source}:/in:ro",
        CLICKHOUSE_IMAGE,
        "sh",
        "-c",
        'set -eu; test -z "$(find /out -mindepth 1 -maxdepth 1 -print -quit)"; '
        "cp /in/ca.crt /in/clickhouse.crt /in/clickhouse.key /in/config.xml /out/; "
        "chown -R 101:101 /out; chmod 644 /out/ca.crt /out/clickhouse.crt /out/config.xml; "
        "chmod 600 /out/clickhouse.key; "
        "test -f /out/config.xml && test -f /out/clickhouse.key",
    ]
    _run(command)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--volume", required=True)
    args = parser.parse_args()
    try:
        seed_volume(args.source, args.volume)
    except (OSError, ValueError, subprocess.CalledProcessError) as exc:
        print(f"ClickHouse TLS provisioning failed: {exc}")
        return 1
    print("ClickHouse TLS volume seeded and verified; private material is not printed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
