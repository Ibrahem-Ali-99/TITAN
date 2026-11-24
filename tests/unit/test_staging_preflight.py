import os
from pathlib import Path

import pytest

from staging_preflight import (
    validate_clickhouse_tls_config,
    validate_compose,
    validate_secret_files,
)


def _secrets(root: Path) -> None:
    root.mkdir()
    for name in (
        "POSTGRES_PASSWORD",
        "CLICKHOUSE_PASSWORD",
        "MINIO_ROOT_PASSWORD",
        "TITAN_READ_TOKEN",
        "TITAN_WRITE_TOKEN",
        "TITAN_ADMIN_TOKEN",
    ):
        path = root / name
        path.write_text(f"unique-{name}\n", encoding="utf-8")
        if os.name != "nt":
            path.chmod(0o600)
    if os.name != "nt":
        root.chmod(0o700)


@pytest.mark.skipif(os.name == "nt", reason="POSIX mode regression only")
def test_secret_files_require_private_modes(tmp_path: Path) -> None:
    root = tmp_path / "secrets"
    _secrets(root)
    validate_secret_files(root)
    (root / "TITAN_READ_TOKEN").chmod(0o644)
    with pytest.raises(ValueError, match="0600"):
        validate_secret_files(root)


def test_placeholder_secret_is_rejected(tmp_path: Path) -> None:
    root = tmp_path / "secrets"
    _secrets(root)
    (root / "POSTGRES_PASSWORD").write_text("titan_dev_password\n", encoding="utf-8")
    if os.name != "nt":
        (root / "POSTGRES_PASSWORD").chmod(0o600)
    with pytest.raises(ValueError, match="placeholder"):
        validate_secret_files(root, windows_acl_verified=os.name == "nt")


def test_compose_rejects_ports_socket_and_nonstaging_volume() -> None:
    config = {
        "services": {"postgres": {"ports": [{"published": 5432}]}},
        "volumes": {},
    }
    with pytest.raises(ValueError, match="host port"):
        validate_compose(config, "titan-staging")

    socket_config = {
        "services": {"dagster-daemon": {"volumes": [{"source": "/var/run/docker.sock"}]}},
        "volumes": {},
    }
    with pytest.raises(ValueError, match="unsafe host bind"):
        validate_compose(socket_config, "titan-staging")

    with pytest.raises(ValueError, match="dedicated titan-staging"):
        validate_compose({"services": {}, "volumes": {}}, "titan")


def test_clickhouse_tls_requires_verified_material_and_clients(tmp_path: Path) -> None:
    for name in ("ca.crt", "clickhouse.crt", "clickhouse.key", "config.xml"):
        (tmp_path / name).write_text("test\n", encoding="ascii")
    services = {
        name: {
            "environment": {
                "CLICKHOUSE_SECURE": "true",
                "CLICKHOUSE_CA_CERT": "/tls/ca.crt",
                "CLICKHOUSE_SERVER_HOST_NAME": "clickhouse",
            }
        }
        for name in (
            "clickhouse",
            "api",
            "migrate",
            "ingestion",
            "spark-bronze",
            "spark-silver",
            "dagster-daemon",
        )
    }
    validate_clickhouse_tls_config({"services": services}, tmp_path)
    services["api"]["environment"]["CLICKHOUSE_SECURE"] = "false"
    with pytest.raises(ValueError, match="client configuration"):
        validate_clickhouse_tls_config({"services": services}, tmp_path)
