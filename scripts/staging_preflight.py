"""Fail-closed validation for the protected staging Compose boundary."""

from __future__ import annotations

import argparse
import json
import os
import stat
import subprocess
import sys
from pathlib import Path
from typing import Any, cast

REQUIRED_SECRETS = (
    "POSTGRES_PASSWORD",
    "CLICKHOUSE_PASSWORD",
    "MINIO_ROOT_PASSWORD",
    "TITAN_READ_TOKEN",
    "TITAN_WRITE_TOKEN",
    "TITAN_ADMIN_TOKEN",
)
PLACEHOLDERS = {"", "titan_dev_password", "local-demo-token", "titan_ci_password"}
DISALLOWED_PORT_SERVICES = {
    "kafka",
    "postgres",
    "clickhouse",
    "minio",
    "api",
    "web",
    "prometheus",
    "grafana",
    "tempo",
}
EXPECTED_VOLUMES = {
    "titan-staging-kafka-data",
    "titan-staging-postgres-data",
    "titan-staging-clickhouse-data",
    "titan-staging-minio-data",
    "titan-staging-spark-checkpoints",
    "titan-staging-web-node-modules",
    "titan-staging-dagster-home",
    "titan-staging-prometheus-data",
    "titan-staging-grafana-data",
    "titan-staging-tempo-data",
}


def _secret_files(root: Path) -> dict[str, Path]:
    return {name: root / name for name in REQUIRED_SECRETS}


def validate_secret_files(root: Path, *, windows_acl_verified: bool = False) -> None:
    if not root.is_dir():
        raise ValueError(f"secret directory does not exist: {root}")
    if os.name == "nt" and not windows_acl_verified:
        raise ValueError("Windows ACL verification is required for staging secrets")
    if os.name != "nt" and stat.S_IMODE(root.stat().st_mode) & 0o077:
        raise ValueError("staging secret directory must be mode 0700 or stricter")
    for name, path in _secret_files(root).items():
        if not path.is_file():
            raise ValueError(f"required staging secret file is missing: {name}")
        if os.name != "nt" and stat.S_IMODE(path.stat().st_mode) & 0o177:
            raise ValueError(f"staging secret file must be mode 0600 or stricter: {name}")
        value = path.read_text(encoding="utf-8").strip()
        if value in PLACEHOLDERS:
            raise ValueError(f"placeholder or empty staging secret rejected: {name}")


def effective_compose(files: list[Path], project: str) -> dict[str, Any]:
    command = [
        "docker",
        "compose",
        "--project-name",
        project,
        "--profile",
        "processing",
        "--profile",
        "orchestration",
        "--profile",
        "observability",
        "--profile",
        "generate",
    ]
    for file in files:
        command.extend(("-f", str(file)))
    command.extend(("config", "--format", "json"))
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    return cast(dict[str, Any], json.loads(result.stdout))


def validate_compose(config: dict[str, Any], project: str) -> None:
    if project != "titan-staging":
        raise ValueError("staging must use the dedicated titan-staging project")
    services = config.get("services", {})
    for service_name in DISALLOWED_PORT_SERVICES:
        if services.get(service_name, {}).get("ports"):
            raise ValueError(f"staging service publishes a host port: {service_name}")
    for service_name, service in services.items():
        for mount in service.get("volumes", []):
            source = str(mount.get("source", "")) if isinstance(mount, dict) else str(mount)
            if "docker.sock" in source or source.startswith(".") or source.startswith("/"):
                raise ValueError(f"staging has an unsafe host bind mount: {service_name}")
    volumes = {str(value.get("name", "")) for value in config.get("volumes", {}).values()}
    if not EXPECTED_VOLUMES.issubset(volumes):
        raise ValueError("staging volume isolation contract is incomplete")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--secrets-dir", type=Path, required=True)
    parser.add_argument("--project-name", default="titan-staging")
    parser.add_argument("--windows-acl-verified", action="store_true")
    parser.add_argument("--compose-file", type=Path, action="append", required=True)
    args = parser.parse_args()
    try:
        validate_secret_files(args.secrets_dir, windows_acl_verified=args.windows_acl_verified)
        validate_compose(effective_compose(args.compose_file, args.project_name), args.project_name)
    except (OSError, ValueError, subprocess.CalledProcessError, json.JSONDecodeError) as exc:
        print(f"staging preflight rejected: {exc}", file=sys.stderr)
        return 1
    print("staging preflight passed: protected profile, private services, isolated resources")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
