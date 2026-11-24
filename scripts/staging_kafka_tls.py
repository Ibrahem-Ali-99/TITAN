"""Build disposable Kafka Java trust/key stores from the staging PEM material.

The generated directory is test secret material and must never be committed.
The broker and Spark use the JKS files; librdkafka clients use ca.crt directly.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
from pathlib import Path

KAFKA_IMAGE = "apache/kafka@sha256:833b867a9585a141da455dc2b57431c65cbccb27f11c91f694444cca9f777e0f"
REQUIRED_VOLUME_FILES = (
    "ca.crt",
    "kafka.keystore.jks",
    "kafka.truststore.jks",
    "kafka.key.credentials",
    "kafka.keystore.credentials",
    "kafka.truststore.credentials",
    "kafka_server_jaas.conf",
    "client.properties",
)


def _run(args: list[str]) -> None:
    subprocess.run(args, check=True, capture_output=True, text=True)


def build_stores(
    source: Path,
    destination: Path,
    password: str,
    username: str,
    sasl_password: str,
) -> None:
    if destination.exists() and any(destination.iterdir()):
        raise ValueError(f"refusing to overwrite nonempty Kafka TLS directory: {destination}")
    destination.mkdir(parents=True, exist_ok=True)
    ca = source / "ca.crt"
    cert = source / "kafka.crt"
    key = source / "kafka.key"
    if not all(path.is_file() for path in (ca, cert, key)):
        raise ValueError("Kafka CA, certificate and private key are required")
    p12 = destination / "kafka.p12"
    keystore = destination / "kafka.keystore.jks"
    truststore = destination / "kafka.truststore.jks"
    _run(
        [
            "openssl",
            "pkcs12",
            "-export",
            "-out",
            str(p12),
            "-inkey",
            str(key),
            "-in",
            str(cert),
            "-certfile",
            str(ca),
            "-passout",
            f"pass:{password}",
        ]
    )
    _run(
        [
            "keytool",
            "-importkeystore",
            "-noprompt",
            "-srckeystore",
            str(p12),
            "-srcstoretype",
            "PKCS12",
            "-srcstorepass",
            password,
            "-destkeystore",
            str(keystore),
            "-deststoretype",
            "JKS",
            "-deststorepass",
            password,
            "-destkeypass",
            password,
            "-alias",
            "1",
        ]
    )
    _run(
        [
            "keytool",
            "-importcert",
            "-noprompt",
            "-trustcacerts",
            "-alias",
            "titan-staging-ca",
            "-file",
            str(ca),
            "-keystore",
            str(truststore),
            "-storetype",
            "JKS",
            "-storepass",
            password,
        ]
    )
    shutil.copy2(ca, destination / "ca.crt")
    (destination / "client.properties").write_text(
        "security.protocol=SASL_SSL\n"
        "sasl.mechanism=PLAIN\n"
        "ssl.truststore.location=/etc/kafka/secrets/kafka.truststore.jks\n"
        f"ssl.truststore.password={password}\n"
        "sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="{username}" password="{sasl_password}";\n'
        "ssl.endpoint.identification.algorithm=https\n",
        encoding="utf-8",
    )
    (destination / "kafka.key.credentials").write_text(password + "\n", encoding="ascii")
    (destination / "kafka.keystore.credentials").write_text(password + "\n", encoding="ascii")
    (destination / "kafka.truststore.credentials").write_text(password + "\n", encoding="ascii")
    (destination / "kafka_server_jaas.conf").write_text(
        "KafkaServer {\n"
        " org.apache.kafka.common.security.plain.PlainLoginModule required\n"
        f' username="{username}" password="{sasl_password}"\n'
        f' user_{username}="{sasl_password}";\n}};\n',
        encoding="ascii",
    )
    if os.name != "nt":
        for path in destination.iterdir():
            path.chmod(0o600)


def seed_volume(source: Path, volume: str) -> None:
    """Copy verified material into an empty Docker volume as Kafka's user."""
    source = source.resolve()
    missing = [name for name in REQUIRED_VOLUME_FILES if not (source / name).is_file()]
    if missing:
        raise ValueError("Kafka TLS material is incomplete: " + ", ".join(missing))
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
        KAFKA_IMAGE,
        "sh",
        "-c",
        'set -eu; test -z "$(find /out -mindepth 1 -maxdepth 1 -print -quit)"; '
        "cp /in/ca.crt /in/kafka.keystore.jks /in/kafka.truststore.jks "
        "/in/kafka.key.credentials /in/kafka.keystore.credentials "
        "/in/kafka.truststore.credentials /in/kafka_server_jaas.conf "
        "/in/client.properties /out/; chown -R 1000:1000 /out; chmod 600 /out/*; "
        "test -f /out/kafka.keystore.jks && test -f /out/kafka.truststore.jks",
    ]
    subprocess.run(command, check=True, capture_output=True, text=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--destination", type=Path)
    parser.add_argument("--volume")
    parser.add_argument("--password", required=True)
    parser.add_argument("--username", required=True)
    parser.add_argument("--sasl-password", required=True)
    args = parser.parse_args()
    if args.volume:
        seed_volume(args.source, args.volume)
        print("Kafka TLS volume seeded and verified")
    else:
        if args.destination is None:
            parser.error("--destination is required when --volume is not supplied")
        build_stores(
            args.source, args.destination, args.password, args.username, args.sasl_password
        )
        print("Kafka TLS stores generated; private material is not printed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
