"""Start and validate the disposable TITAN Keycloak OIDC fixture."""

from __future__ import annotations

import argparse
import json
import secrets
import shutil
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

import jwt

ROOT = Path(__file__).resolve().parents[1]
IMAGE = "quay.io/keycloak/keycloak:26.3.4"
REALM = "titan-test"


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


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout", type=int, default=90)
    args = parser.parse_args()
    project = f"titan-oidc-fixture-{secrets.token_hex(4)}"
    port = 18080 + secrets.randbelow(500)
    container = f"{project}-keycloak"
    temp = Path(tempfile.mkdtemp(prefix="titan-oidc-fixture-"))
    temp.chmod(0o700)
    passwords = {
        name: secrets.token_urlsafe(18) for name in ("viewer", "analyst", "operator", "admin")
    }
    client_secret = secrets.token_urlsafe(24)
    template = (ROOT / "tests" / "fixtures" / "keycloak" / "titan-realm.template.json").read_text()
    values = {
        "CLIENT_SECRET": client_secret,
        **{f"{name.upper()}_PASSWORD": value for name, value in passwords.items()},
    }
    realm = template
    for name, value in values.items():
        realm = realm.replace("${" + name + "}", value)
    realm_path = temp / "titan-test-realm.json"
    realm_path.write_text(realm, encoding="utf-8")
    realm_path.chmod(0o600)
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
        discovery = wait_for(f"{issuer}/.well-known/openid-configuration", args.timeout)
        if discovery.get("issuer") != issuer:
            raise RuntimeError("issuer mismatch")
        jwks = request_json(discovery["jwks_uri"])
        expected_roles = {
            "viewer": "viewer",
            "analyst": "analyst",
            "operator": "operator",
            "admin": "admin",
        }
        for username, expected_role in expected_roles.items():
            token_body = urllib.parse.urlencode(
                {
                    "grant_type": "password",
                    "client_id": "titan-browser",
                    "client_secret": client_secret,
                    "username": username,
                    "password": passwords[username],
                    "scope": "openid",
                }
            ).encode()
            token = request_json(
                discovery["token_endpoint"],
                data=token_body,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )["id_token"]
            header = jwt.get_unverified_header(token)
            key = next(item for item in jwks["keys"] if item["kid"] == header["kid"])
            claims = jwt.decode(
                token,
                jwt.algorithms.RSAAlgorithm.from_jwk(json.dumps(key)),
                algorithms=["RS256"],
                issuer=issuer,
                audience="titan",
            )
            if claims.get("titan_roles") != [expected_role]:
                raise RuntimeError(f"{username} role claim mismatch")
        print(
            json.dumps(
                {
                    "issuer": issuer,
                    "discovery": True,
                    "redirect_uri": "https://staging.local:8443/auth/callback",
                    "roles": ["viewer", "analyst", "operator", "admin"],
                    "signed_token": True,
                }
            )
        )
        return 0
    finally:
        subprocess.run(["docker", "rm", "--force", container], capture_output=True)
        shutil.rmtree(temp, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
