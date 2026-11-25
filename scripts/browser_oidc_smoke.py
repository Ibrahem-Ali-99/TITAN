"""Run the real Chromium OIDC login/logout smoke test."""

from __future__ import annotations

import argparse
import atexit
import json
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from playwright.sync_api import sync_playwright


def _safe_url(value: str) -> str:
    parts = urlsplit(value)
    sensitive = {"code", "state", "nonce", "code_challenge", "client_secret", "token"}
    query = [
        (key, "[redacted]" if key.lower() in sensitive else item)
        for key, item in parse_qsl(parts.query)
    ]
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), ""))


def _sanitize(value: object) -> object:
    if isinstance(value, dict):
        return {
            key: "[redacted]"
            if any(
                word in key.lower()
                for word in ("password", "secret", "token", "cookie", "authorization")
            )
            else _sanitize(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_sanitize(item) for item in value]
    if isinstance(value, str) and (value.startswith("http://") or value.startswith("https://")):
        return _safe_url(value)
    return value


def write_evidence(path: Path, evidence: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_sanitize(evidence), indent=2) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--issuer", required=True)
    parser.add_argument("--username", default="viewer")
    parser.add_argument("--password-file", type=Path, required=True)
    parser.add_argument("--screenshot", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    password = args.password_file.read_text(encoding="utf-8").strip()
    evidence: dict[str, object] = {
        "result": "incomplete",
        "console_errors": [],
        "failed_requests": [],
        "redirects": [],
    }
    atexit.register(write_evidence, args.evidence, evidence)
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(
            headless=True,
            args=[
                "--host-resolver-rules=MAP staging.local 127.0.0.1,"
                "MAP host.docker.internal 127.0.0.1"
            ],
        )
        context = browser.new_context(ignore_https_errors=False)
        page = context.new_page()
        evidence["browser_context"] = {"javascript_enabled": True, "ignore_https_errors": False}
        page.on(
            "domcontentloaded",
            lambda: evidence.setdefault("lifecycle", []).append("domcontentloaded"),
        )
        page.on("load", lambda: evidence.setdefault("lifecycle", []).append("load"))
        page.on(
            "console",
            lambda message: (
                evidence.setdefault(
                    "smoke_console"
                    if message.text.startswith("[titan-smoke]")
                    else "console_errors",
                    [],
                ).append(message.text)
                if message.type == "error" or message.text.startswith("[titan-smoke]")
                else None
            ),
        )  # type: ignore[union-attr]
        page.on(
            "pageerror",
            lambda error: evidence.setdefault("page_errors", []).append(str(error)[:240]),
        )
        page.on("requestfailed", lambda request: evidence["failed_requests"].append(request.url))  # type: ignore[union-attr]
        page.on(
            "response",
            lambda response: (
                evidence.setdefault("api_responses", []).append(
                    {"path": response.url.split("staging.local", 1)[-1], "status": response.status}
                )
                if "/api/" in response.url or "/health/ready" in response.url
                else evidence.setdefault("script_responses", []).append(
                    {
                        "path": response.url.split("staging.local", 1)[-1],
                        "status": response.status,
                        "content_type": response.headers.get("content-type", ""),
                    }
                )
                if "/_next/" in response.url
                else None
            ),
        )
        page.on(
            "console",
            lambda message: (
                evidence.setdefault("security_messages", []).append(message.text)
                if message.type == "warning"
                else None
            ),
        )
        page.on(
            "framenavigated",
            lambda frame: (
                evidence["redirects"].append(frame.url) if frame == page.main_frame else None
            ),
        )  # type: ignore[union-attr]
        page.goto(args.base_url, wait_until="domcontentloaded", timeout=60000)
        evidence["initial_url"] = page.url
        page.wait_for_url(args.issuer + "**", timeout=30000)
        if not page.url.startswith(args.issuer):
            raise RuntimeError(f"browser did not redirect to the configured issuer: {page.url}")
        page.locator("#username").fill(args.username)
        page.locator("#password").fill(password)
        page.locator("#kc-login").click()
        page.wait_for_url(args.base_url + "/", timeout=60000)
        evidence["authenticated_url"] = page.url
        page.goto(
            f"{args.base_url}/?__smoke=1&__hydration_probe=1",
            wait_until="domcontentloaded",
            timeout=60000,
        )
        api_checks = {}
        for path in (
            "/api/health/ready",
            "/api/v1/ingestion/stats",
            "/api/v1/events/recent?limit=20",
        ):
            api_checks[path] = page.evaluate(
                """async (path) => {
                  const started = performance.now();
                  const controller = new AbortController();
                  const timer = setTimeout(() => controller.abort(), 10000);
                  try {
                    const response = await fetch(path, {
                      credentials: "same-origin",
                      signal: controller.signal,
                    });
                    const body = await response.text();
                    return {
                      status: response.status,
                      content_type: response.headers.get("content-type"),
                      body_excerpt: body.slice(0, 240),
                      duration_ms: Math.round(performance.now() - started),
                    };
                  } catch (error) {
                    return {
                      error: String(error).split("\\n")[0].slice(0, 240),
                      duration_ms: Math.round(performance.now() - started),
                    };
                  } finally {
                    clearTimeout(timer);
                  }
                }""",
                path,
            )
        evidence["api_checks"] = api_checks
        try:
            evidence["hydration_probe"] = page.locator(
                "[data-testid='hydration-probe']"
            ).inner_text()
        except Exception as error:
            evidence["hydration_probe"] = f"unavailable:{type(error).__name__}"
        try:
            page.wait_for_selector("main h1", timeout=30000)
        except Exception as error:
            evidence["post_callback_url"] = page.url
            evidence["page_title"] = page.title()
            evidence["body_excerpt"] = page.locator("body").inner_text()[:500]
            evidence["document_state"] = page.evaluate(
                """() => ({
                  ready_state: document.readyState,
                  loading_markers: [...document.querySelectorAll("body *")]
                    .filter(node =>
                      (node.textContent || "").includes("Loading persisted ingestion state")
                    )
                    .slice(0, 3).map(node => node.tagName),
                  scripts: [...document.scripts].map(script => script.src).filter(Boolean),
                  next_assets: performance.getEntriesByType("resource")
                    .map(entry => entry.name).filter(name => name.includes("/_next/"))
                    .slice(-8),
                })"""
            )
            args.screenshot.parent.mkdir(parents=True, exist_ok=True)
            page.screenshot(path=str(args.screenshot), full_page=True)
            print(json.dumps(evidence, indent=2))
            print(
                json.dumps(
                    {
                        "diagnostic_summary": {
                            key: evidence.get(key)
                            for key in (
                                "lifecycle",
                                "smoke_console",
                                "page_errors",
                                "api_responses",
                                "hydration_probe",
                                "document_state",
                            )
                        }
                    }
                )
            )
            raise RuntimeError(f"authenticated overview did not render: {error}") from error
        evidence["overview_heading"] = page.locator("main h1").inner_text()
        evidence["authorized_api_status"] = page.evaluate(
            """async () => {
              const response = await fetch("/api/health/ready", { credentials: "same-origin" });
              return {
                status: response.status,
                body_excerpt: (await response.text()).slice(0, 240),
              };
            }"""
        )
        if evidence["authorized_api_status"]["status"] != 200:
            status = evidence["authorized_api_status"]["status"]
            raise RuntimeError(f"authorized API read failed: HTTP {status}")
        args.screenshot.parent.mkdir(parents=True, exist_ok=True)
        page.screenshot(path=str(args.screenshot), full_page=True)
        page.locator('form[action="/auth/logout"] button, button:has-text("Log out")').first.click()
        page.wait_for_url(
            lambda url: url.startswith(args.base_url) or url.startswith(args.issuer), timeout=30000
        )
        evidence["logout_url"] = page.url
        context.close()
        browser.close()
    evidence["logout_verified"] = True
    evidence["result"] = "passed"
    print(json.dumps(evidence, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
