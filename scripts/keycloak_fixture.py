"""Disposable Keycloak lifecycle for isolated OIDC tests."""

from __future__ import annotations

import json
import secrets
import shutil
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
IMAGE = "quay.io/keycloak/keycloak:26.3.4"
REALM = "titan-test"
USERS = ("viewer", "analyst", "operator", "admin")


def request_json(
    url: str, *, data: bytes | None = None, headers: dict[str, str] | None = None
) -> dict:
    request = urllib.request.Request(url, data=data, headers=headers or {})
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return json.loads(response.read())
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")[:300]
        raise RuntimeError(f"OIDC endpoint returned HTTP {exc.code}: {detail}") from exc


def wait_for(url: str, timeout: int) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            return request_json(url)
        except Exception:
            time.sleep(1)
    raise RuntimeError(f"Keycloak readiness timed out: {url}")


@dataclass(frozen=True)
class KeycloakFixture:
    container: str
    issuer: str
    client_id: str
    client_secret_file: Path
    password_files: dict[str, Path]
    credential_dir: Path

    @property
    def browser_issuer(self) -> str:
        return self.issuer


@contextmanager
def disposable_keycloak_fixture(timeout: int = 90) -> Iterator[KeycloakFixture]:
    project = f"titan-oidc-fixture-{secrets.token_hex(4)}"
    port = 18080 + secrets.randbelow(500)
    container = f"{project}-keycloak"
    credential_dir = Path(tempfile.mkdtemp(prefix="titan-oidc-fixture-"))
    credential_dir.chmod(0o700)
    passwords = {name: secrets.token_urlsafe(18) for name in USERS}
    client_secret = secrets.token_urlsafe(24)
    template = (ROOT / "tests" / "fixtures" / "keycloak" / "titan-realm.template.json").read_text()
    values = {
        "CLIENT_SECRET": client_secret,
        **{f"{n.upper()}_PASSWORD": v for n, v in passwords.items()},
    }
    realm = template
    for name, value in values.items():
        realm = realm.replace("${" + name + "}", value)
    realm_path = credential_dir / "titan-test-realm.json"
    realm_path.write_text(realm, encoding="utf-8")
    realm_path.chmod(0o600)
    client_file = credential_dir / "client_secret"
    client_file.write_text(client_secret + "\n", encoding="utf-8")
    client_file.chmod(0o600)
    password_files: dict[str, Path] = {}
    for username, password in passwords.items():
        path = credential_dir / f"{username}_password"
        path.write_text(password + "\n", encoding="utf-8")
        path.chmod(0o600)
        password_files[username] = path
    issuer = f"http://127.0.0.1:{port}/realms/{REALM}"
    try:
        subprocess.run(["docker", "image", "inspect", IMAGE], check=True, capture_output=True)
        subprocess.run(
            [
                "docker",
                "run",
                "--detach",
                "--name",
                container,
                "--publish",
                f"127.0.0.1:{port}:8080",
                "--volume",
                f"{realm_path}:/opt/keycloak/data/import/titan-test-realm.json:ro",
                "-e",
                "KEYCLOAK_ADMIN=admin",
                "-e",
                "KEYCLOAK_ADMIN_PASSWORD=disposable-admin",
                IMAGE,
                "start-dev",
                "--http-port=8080",
                "--import-realm",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        wait_for(f"{issuer}/.well-known/openid-configuration", timeout)
        yield KeycloakFixture(
            container, issuer, "titan-browser", client_file, password_files, credential_dir
        )
    finally:
        subprocess.run(["docker", "rm", "--force", container], capture_output=True)
        shutil.rmtree(credential_dir, ignore_errors=True)
