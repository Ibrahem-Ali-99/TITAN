"""Shared Kafka client security settings.

The default remains the development plaintext listener. Protected staging must
set ``KAFKA_SECURITY_PROTOCOL=SASL_SSL`` and provide all TLS/SASL values; there
is deliberately no implicit downgrade when that mode is selected.
"""

from __future__ import annotations

import os


def kafka_client_config(bootstrap_servers: str) -> dict[str, str | int | float | bool]:
    config: dict[str, str | int | float | bool] = {"bootstrap.servers": bootstrap_servers}
    protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT").strip().upper()
    if protocol == "PLAINTEXT":
        return config
    if protocol != "SASL_SSL":
        raise ValueError(f"unsupported KAFKA_SECURITY_PROTOCOL: {protocol!r}")
    mechanism = os.getenv("KAFKA_SASL_MECHANISM", "").strip().upper()
    username = os.getenv("KAFKA_SASL_USERNAME", "").strip()
    password = os.getenv("KAFKA_SASL_PASSWORD", "")
    ca = os.getenv("KAFKA_SSL_CA_LOCATION", "").strip()
    if mechanism != "PLAIN":
        raise ValueError("protected Kafka requires the configured SASL/PLAIN mechanism")
    if not username or not password or not ca:
        raise ValueError("SASL_SSL Kafka clients require username, password and CA location")
    config.update(
        {
            "security.protocol": protocol,
            "sasl.mechanisms": mechanism,
            "sasl.username": username,
            "sasl.password": password,
            "ssl.ca.location": ca,
            "enable.ssl.certificate.verification": True,
            "ssl.endpoint.identification.algorithm": "https",
        }
    )
    return config
