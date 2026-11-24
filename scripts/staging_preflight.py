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
    "titan-staging-kafka-tls",
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


def validate_tls_files(root: Path) -> None:
    required = [root / "ca.crt"]
    required.extend(
        root / name
        for service in ("kafka", "postgres", "clickhouse", "minio")
        for name in (f"{service}.crt", f"{service}.key")
    )
    missing = [path.name for path in required if not path.is_file()]
    if missing:
        raise ValueError("required TLS files are missing: " + ", ".join(missing))


def validate_postgres_tls_config(config: dict[str, Any]) -> None:
    postgres = config.get("services", {}).get("postgres", {})
    command = " ".join(str(item) for item in postgres.get("command", []))
    if (
        "ssl=on" not in command
        or "/tls/postgres.crt" not in command
        or "hba_file=/tls/pg_hba.conf" not in command
    ):
        raise ValueError("PostgreSQL TLS overlay does not enable the expected server certificate")
    for service_name in (
        "migrate",
        "ingestion",
        "api",
        "spark-bronze",
        "spark-silver",
        "dagster-daemon",
    ):
        environment = config.get("services", {}).get(service_name, {}).get("environment", {})
        database_url = str(environment.get("DATABASE_URL", ""))
        if (
            "sslmode=verify-full" not in database_url
            or "sslrootcert=/tls/ca.crt" not in database_url
        ):
            raise ValueError(f"PostgreSQL TLS client configuration is incomplete: {service_name}")


def validate_kafka_tls_config(config: dict[str, Any]) -> None:
    kafka = config.get("services", {}).get("kafka", {})
    environment = kafka.get("environment", {})
    if "SASL_SSL://kafka:19092" not in str(environment.get("KAFKA_ADVERTISED_LISTENERS", "")):
        raise ValueError("Kafka TLS overlay must advertise only the SASL_SSL container listener")
    if "SASL_SSL:SASL_SSL" not in str(environment.get("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "")):
        raise ValueError("Kafka TLS overlay does not map its protected listener to SASL_SSL")
    if str(environment.get("KAFKA_SASL_ENABLED_MECHANISMS", "")) != "PLAIN":
        raise ValueError("Kafka TLS staging requires the explicitly selected PLAIN mechanism")
    for service_name in ("api", "ingestion", "spark-bronze"):
        service_env = config.get("services", {}).get(service_name, {}).get("environment", {})
        protocol = str(service_env.get("KAFKA_SECURITY_PROTOCOL", ""))
        if protocol != "SASL_SSL":
            raise ValueError(f"Kafka TLS client configuration is incomplete: {service_name}")
        if not str(service_env.get("KAFKA_SASL_USERNAME", "")).strip():
            raise ValueError(f"Kafka TLS username is missing: {service_name}")
        if service_name == "spark-bronze":
            if not str(service_env.get("KAFKA_SSL_TRUSTSTORE_LOCATION", "")).strip():
                raise ValueError("Spark Kafka truststore is missing")
        elif not str(service_env.get("KAFKA_SSL_CA_LOCATION", "")).strip():
            raise ValueError(f"Kafka CA location is missing: {service_name}")


def validate_kafka_tls_volume(root: Path) -> None:
    required = (
        "ca.crt",
        "kafka.keystore.jks",
        "kafka.truststore.jks",
        "kafka.key.credentials",
        "kafka.keystore.credentials",
        "kafka.truststore.credentials",
        "kafka_server_jaas.conf",
        "client.properties",
    )
    missing = [name for name in required if not (root / name).is_file()]
    if missing:
        raise ValueError("Kafka TLS volume material is incomplete: " + ", ".join(missing))


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
    parser.add_argument("--require-tls", action="store_true")
    parser.add_argument("--tls-dir", type=Path)
    parser.add_argument("--postgres-tls", action="store_true")
    parser.add_argument("--kafka-tls", action="store_true")
    parser.add_argument("--kafka-tls-dir", type=Path)
    parser.add_argument("--compose-file", type=Path, action="append", required=True)
    args = parser.parse_args()
    try:
        validate_secret_files(args.secrets_dir, windows_acl_verified=args.windows_acl_verified)
        if args.require_tls:
            if args.tls_dir is None:
                raise ValueError("--tls-dir is required with --require-tls")
            validate_tls_files(args.tls_dir)
        config = effective_compose(args.compose_file, args.project_name)
        validate_compose(config, args.project_name)
        if args.postgres_tls:
            validate_postgres_tls_config(config)
        if args.kafka_tls:
            validate_kafka_tls_config(config)
            if args.kafka_tls_dir is None:
                raise ValueError("--kafka-tls-dir is required with --kafka-tls")
            validate_kafka_tls_volume(args.kafka_tls_dir)
    except (OSError, ValueError, subprocess.CalledProcessError, json.JSONDecodeError) as exc:
        print(f"staging preflight rejected: {exc}", file=sys.stderr)
        return 1
    print("staging preflight passed: protected profile, private services, isolated resources")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
