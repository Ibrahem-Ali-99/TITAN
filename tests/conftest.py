"""Explicit disposable credentials for in-process API unit tests."""

import os

import pytest
from starlette.testclient import TestClient


@pytest.fixture(autouse=True)
def _api_test_credentials(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> None:
    if request.node.get_closest_marker("integration") is not None:
        return
    monkeypatch.setenv("TITAN_READ_TOKEN", "unit-read-token")
    monkeypatch.setenv("TITAN_WRITE_TOKEN", "unit-write-token")
    monkeypatch.setenv("TITAN_ADMIN_TOKEN", "unit-admin-token")


@pytest.fixture(autouse=True)
def _legacy_api_headers(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep pre-auth unit fixtures explicit without weakening application auth."""
    original = TestClient.request

    def request(self, method, url, **kwargs):  # type: ignore[no-untyped-def]
        headers = kwargs.get("headers") or {}
        if "Authorization" not in headers and not os.environ.get("TITAN_RAW_AUTH_TEST"):
            token = (
                "unit-write-token"
                if method.upper() in {"POST", "PUT", "PATCH", "DELETE"}
                else "unit-read-token"
            )
            kwargs["headers"] = {
                "Authorization": f"Bearer {token}",
                **headers,
            }
        return original(self, method, url, **kwargs)

    monkeypatch.setattr(TestClient, "request", request)
