"""Start and validate the disposable TITAN Keycloak OIDC fixture."""

from __future__ import annotations

import argparse
import json
import urllib.parse
from typing import cast

import jwt
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey

from scripts.keycloak_fixture import USERS, disposable_keycloak_fixture, request_json


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout", type=int, default=90)
    args = parser.parse_args()
    with disposable_keycloak_fixture(args.timeout) as fixture:
        discovery = request_json(f"{fixture.discovery_url}/.well-known/openid-configuration")
        if discovery.get("issuer") != fixture.issuer:
            raise RuntimeError("issuer mismatch")
        jwks = request_json(cast(str, discovery["jwks_uri"]))
        client_secret = fixture.client_secret_file.read_text().strip()
        for username in USERS:
            password = fixture.password_files[username].read_text().strip()
            body = urllib.parse.urlencode(
                {
                    "grant_type": "password",
                    "client_id": fixture.client_id,
                    "client_secret": client_secret,
                    "username": username,
                    "password": password,
                    "scope": "openid",
                }
            ).encode()
            token = request_json(
                cast(str, discovery["token_endpoint"]),
                data=body,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )["id_token"]
            token = cast(str, token)
            header = jwt.get_unverified_header(token)
            keys = cast(list[dict[str, object]], jwks["keys"])
            key = next(item for item in keys if item["kid"] == header["kid"])
            claims = jwt.decode(
                token,
                cast(RSAPublicKey, jwt.algorithms.RSAAlgorithm.from_jwk(json.dumps(key))),
                algorithms=["RS256"],
                issuer=fixture.issuer,
                audience="titan",
            )
            if claims.get("titan_roles") != [username]:
                raise RuntimeError(f"{username} role claim mismatch")
        print(
            json.dumps(
                {
                    "issuer": fixture.issuer,
                    "discovery": True,
                    "redirect_uri": "https://staging.local:8443/auth/callback",
                    "roles": list(USERS),
                    "signed_token": True,
                }
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
