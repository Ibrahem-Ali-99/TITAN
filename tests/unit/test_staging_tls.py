import shutil
from pathlib import Path

import pytest

from staging_tls import SERVICES, provision, verify

pytestmark = pytest.mark.skipif(
    shutil.which("openssl") is None, reason="OpenSSL executable unavailable"
)


def test_provision_and_verify_certificates(tmp_path: Path) -> None:
    provision(tmp_path, days=2, windows_acl_verified=True)
    verify(tmp_path, windows_acl_verified=True)
    for service in SERVICES:
        assert (tmp_path / f"{service}.crt").is_file()
        assert (tmp_path / f"{service}.key").is_file()


def test_refuses_nonempty_directory(tmp_path: Path) -> None:
    (tmp_path / "existing").write_text("x", encoding="ascii")
    with pytest.raises(ValueError, match="non-empty"):
        provision(tmp_path, days=2, windows_acl_verified=True)


def test_rejects_mismatched_key(tmp_path: Path) -> None:
    provision(tmp_path, days=2, windows_acl_verified=True)
    (tmp_path / "minio.key").write_bytes((tmp_path / "kafka.key").read_bytes())
    with pytest.raises((OSError, ValueError)):
        verify(tmp_path, windows_acl_verified=True)
