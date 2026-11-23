"""Fail-closed bearer authentication for the API boundary."""

from __future__ import annotations

import hmac
from collections.abc import Awaitable, Callable

from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp

from titan_api.config import ApiSettings


class AuthenticationMiddleware(BaseHTTPMiddleware):
    """Apply read/write/admin scopes without ever logging bearer material."""

    def __init__(self, app: ASGIApp, *, settings: ApiSettings) -> None:
        super().__init__(app)
        self.settings = settings

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        required = required_scope(request.url.path, request.method)
        if required is None:
            return await call_next(request)
        result = authenticate(request.headers.get("authorization"), self.settings, required)
        if result is not None:
            return result
        return await call_next(request)


def required_scope(path: str, method: str) -> str | None:
    if path in {"/health/live", "/health/ready"}:
        return None
    if path in {"/docs", "/redoc", "/openapi.json"}:
        return "read"
    if path == "/metrics" or path.startswith("/api/v1/"):
        if method == "POST" and (path.endswith("/publish") or path.endswith("/replays")):
            return "admin"
        if method in {"POST", "PUT", "PATCH", "DELETE"}:
            return "write"
        return "read"
    return None


def authenticate(
    authorization: str | None, settings: ApiSettings, required: str
) -> JSONResponse | None:
    configured = {
        "read": settings.read_token,
        "write": settings.write_token,
        "admin": settings.admin_token,
    }
    if not any(configured.values()):
        return JSONResponse(
            {"detail": "API authentication is not configured"}, status_code=503
        )
    supplied = ""
    if authorization and authorization.startswith("Bearer "):
        supplied = authorization[7:].strip()
    roles = []
    for role, token in configured.items():
        if token and hmac.compare_digest(supplied, token):
            roles.append(role)
    allowed = required == "read" and bool(roles) or required in roles or "admin" in roles
    if allowed:
        return None
    if not supplied or not roles:
        return JSONResponse(
            {"detail": "authentication required"},
            status_code=401,
            headers={"WWW-Authenticate": "Bearer"},
        )
    return JSONResponse({"detail": "insufficient scope"}, status_code=403)
