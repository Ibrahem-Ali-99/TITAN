"""Seed a verified HTTPS ingress certificate into a disposable volume."""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path

from scripts.staging_tool_container import run as run_tool

IMAGE = "alpine/minio@sha256:cf23643a6cf9ce159c57643ceb88279e431262282428c9e0bf3a7ef1a97e84b4"


def seed_volume(source: Path, volume: str) -> None:
    source = source.resolve()
    required = (source / "ca.crt", source / "ingress.crt", source / "ingress.key")
    missing = [path.name for path in required if not path.is_file()]
    if missing:
        raise ValueError("ingress TLS material is incomplete: " + ", ".join(missing))
    run_tool(
        [
            "openssl",
            "verify",
            "-CAfile",
            str(source / "ca.crt"),
            "-verify_hostname",
            "staging.local",
            str(source / "ingress.crt"),
        ]
    )
    run_tool(["openssl", "x509", "-in", str(source / "ingress.crt"), "-checkend", "0", "-noout"])
    cert = run_tool(
        ["openssl", "x509", "-in", str(source / "ingress.crt"), "-pubkey", "-noout"]
    ).stdout
    key = run_tool(["openssl", "pkey", "-in", str(source / "ingress.key"), "-pubout"]).stdout
    if cert != key:
        raise ValueError("ingress certificate and private key do not match")
    run_tool(
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
            IMAGE,
            "-c",
            'set -eu; test -z "$(find /out -mindepth 1 -maxdepth 1 -print -quit)"; '
            "cp /in/ca.crt /out/ca.crt; cp /in/ingress.crt /out/ingress.crt; "
            "cp /in/ingress.key /out/ingress.key; "
            "chmod 644 /out/ca.crt /out/ingress.crt; chmod 600 /out/ingress.key",
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
        print(f"ingress TLS provisioning failed: {exc}")
        return 1
    print("ingress TLS volume seeded and verified; private material is not printed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
