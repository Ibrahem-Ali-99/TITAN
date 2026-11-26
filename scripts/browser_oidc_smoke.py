"""Run the real Chromium OIDC login/logout smoke test."""

from __future__ import annotations

import argparse
import atexit
import json
from pathlib import Path
from typing import Any
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
            and isinstance(item, str)
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
    parser.add_argument("--boundary-probe", action="store_true")
    parser.add_argument("--staging-host", default="127.0.0.1")
    parser.add_argument("--role-check", action="store_true")
    parser.add_argument("--expected-role", choices=("viewer", "analyst", "operator", "admin"))
    args = parser.parse_args()
    password = args.password_file.read_text(encoding="utf-8").strip()
    # Browser event payloads and evaluate() results are heterogeneous JSON values.
    evidence: dict[str, Any] = {
        "result": "incomplete",
        "console_errors": [],
        "failed_requests": [],
        "redirects": [],
        "navigation_diagnostics": [],
    }
    atexit.register(write_evidence, args.evidence, evidence)
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(
            headless=True,
            args=[
                "--no-proxy-server",
                f"--host-resolver-rules=MAP staging.local {args.staging_host}",
            ],
        )
        context = browser.new_context(ignore_https_errors=False)
        page = context.new_page()
        evidence["browser_context"] = {"javascript_enabled": True, "ignore_https_errors": False}
        page.on(  # type: ignore[call-overload]
            "domcontentloaded",
            lambda: evidence.setdefault("lifecycle", []).append("domcontentloaded"),
        )
        page.on("load", lambda: evidence.setdefault("lifecycle", []).append("load"))  # type: ignore[call-overload]
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
        )
        page.on(
            "pageerror",
            lambda error: evidence.setdefault("page_errors", []).append(str(error)[:240]),
        )
        page.on("requestfailed", lambda request: evidence["failed_requests"].append(request.url))
        page.on(
            "request",
            lambda request: (
                evidence["navigation_diagnostics"].append(
                    {
                        "kind": "request",
                        "path": _safe_url(request.url),
                        "method": request.method,
                        "cookie_present": bool(request.headers.get("cookie")),
                    }
                )
                if "?_rsc=" in request.url
                else None
            ),
        )
        page.on(
            "response",
            lambda response: (
                evidence["navigation_diagnostics"].append(
                    {
                        "kind": "response",
                        "path": _safe_url(response.url),
                        "status": response.status,
                        "location": _safe_url(response.headers.get("location", ""))
                        if response.headers.get("location")
                        else None,
                    }
                )
                if "?_rsc=" in response.url
                else None
            ),
        )
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
        )
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
            if args.boundary_probe:
                evidence["proxy_boundary_probe"] = page.evaluate(
                    """async () => {
                      const results = [];
                      for (let i = 0; i < 8; i++) {
                        for (const path of ["/api/v1/monitoring/overview", "/api/health/ready"]) {
                          const started = performance.now();
                          try {
                            const response = await fetch(path, {
                              credentials: "same-origin",
                              signal: AbortSignal.timeout(10000),
                            });
                            await response.arrayBuffer();
                            results.push({
                              path, status: response.status,
                              duration_ms: Math.round(performance.now() - started),
                            });
                          } catch (error) {
                            results.push({
                              path, error: String(error?.name || "Error"),
                              duration_ms: Math.round(performance.now() - started),
                            });
                          }
                        }
                      }
                      return results;
                    }"""
                )
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
            print(json.dumps(_sanitize(evidence), indent=2))
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
        if args.role_check:
            if not args.expected_role:
                raise RuntimeError("--expected-role is required with --role-check")
            role_check = page.evaluate(
                """async () => {
                  const boundedFetch = async (input, init = {}) => {
                    const controller = new AbortController();
                    const timer = setTimeout(() => controller.abort(), 10000);
                    try {
                      const response = await fetch(input, {...init, signal: controller.signal});
                      return {status: response.status, error: null};
                    } catch (error) {
                      return {status: 0, error: error instanceof Error ? error.name : 'Error'};
                    } finally {
                      clearTimeout(timer);
                    }
                  };
                  const headers = [
                    'event_type,occurred_at,order_id,customer_id,currency,product_id,quantity,unit_price,payment_id,amount,payment_method,quantity_on_hand,inventory_delta,warehouse_id',
                    'order_created,2026-01-01T00:00:00Z,role-check-order,role-check-customer,EUR,role-check-product,1,1.00,,,,,,'
                  ].join('\\n');
                  const form = new FormData();
                  form.append('file', new Blob([headers], {type: 'text/csv'}), 'role-check.csv');
                  const write = await boundedFetch('/api/v1/ingestion/imports', {
                    method: 'POST', body: form,
                  });
                  const admin = await boundedFetch(
                    '/api/v1/ingestion/imports/00000000-0000-0000-0000-000000000000/publish',
                    {method: 'POST'},
                  );
                  return {
                    write_status: write.status,
                    admin_status: admin.status,
                    write_error: write.error,
                    admin_error: admin.error,
                  };
                }"""
            )
            evidence["role_check"] = role_check
            expected = {
                "viewer": (403, 403),
                "analyst": (403, 403),
                "operator": (202, 403),
                "admin": (202, 404),
            }[args.expected_role]
            actual = (role_check["write_status"], role_check["admin_status"])
            if actual != expected:
                raise RuntimeError(
                    f"role authorization mismatch: expected {expected}, got {actual}"
                )
            evidence["result"] = "passed"
            write_evidence(args.evidence, evidence)
            context.close()
            browser.close()
            print(json.dumps(_sanitize(evidence), indent=2))
            return 0
        args.screenshot.parent.mkdir(parents=True, exist_ok=True)
        page.screenshot(path=str(args.screenshot), full_page=True)
        screen_routes = {
            "/": ("Overview", "Operations overview"),
            "/streaming": ("Streaming", "Event streaming"),
            "/data-sources": ("Data Sources", "Data sources"),
            "/pipelines": ("Pipelines", "Pipeline orchestration"),
            "/warehouse": ("Warehouse", "Warehouse analytics"),
            "/data-quality": ("Data Quality", "Data quality"),
            "/monitoring": ("Monitoring", "System monitoring"),
            "/ml": ("ML", "Demand intelligence"),
        }
        screen_results: dict[str, object] = {}
        for path, (label, expected_heading) in screen_routes.items():
            page.set_viewport_size({"width": 1440, "height": 900})
            evidence.setdefault("session_cookie_state", []).append(
                {
                    "path": path,
                    "cookie_present": any(
                        cookie["name"] == "titan_session"
                        for cookie in page.context.cookies(args.base_url)
                    ),
                }
            )
            if path == "/":
                page.goto(args.base_url + path, wait_until="domcontentloaded", timeout=60000)
            else:
                page.locator(f'a[href="{path}"]').click()
                page.wait_for_url(f"{args.base_url}{path}", timeout=30000)
            heading_locator = page.locator("main h1").filter(has_text=expected_heading).first
            heading_locator.wait_for(timeout=30000)
            heading = heading_locator.inner_text()
            page.set_viewport_size({"width": 390, "height": 844})
            page.reload(wait_until="domcontentloaded", timeout=60000)
            mobile_heading_locator = page.locator("main h1").filter(has_text=expected_heading).first
            mobile_heading_locator.wait_for(timeout=30000)
            mobile_heading = mobile_heading_locator.inner_text()
            screen_results[path] = {
                "label": label,
                "desktop_heading": heading,
                "mobile_heading": mobile_heading,
            }
        evidence["screen_results"] = screen_results
        page.set_viewport_size({"width": 1440, "height": 900})
        logout = page.locator(
            'form[action="/auth/logout"] button, button:has-text("Log out")'
        ).first
        if logout.count():
            logout.click()
        else:
            logout_result = page.evaluate(
                """async () => {
                  const response = await fetch('/auth/logout', {
                    method: 'POST',
                    credentials: 'same-origin',
                    headers: { origin: window.location.origin },
                  });
                  return {status: response.status, body: await response.text()};
                }"""
            )
            evidence["logout_response"] = {
                "status": logout_result["status"],
                "body_excerpt": logout_result["body"][:240],
            }
            if logout_result["status"] != 200:
                raise RuntimeError(f"logout failed: HTTP {logout_result['status']}")
        page.goto(args.base_url + "/", wait_until="domcontentloaded")
        evidence["logout_url"] = page.url
        evidence["fresh_navigation_url"] = page.url
        if not any(url.startswith(args.issuer) for url in evidence["redirects"]):
            raise RuntimeError(
                "fresh protected navigation did not redirect through the OIDC issuer"
            )
        evidence["fresh_navigation_required_oidc"] = True
        context.close()
        browser.close()
    evidence["logout_verified"] = True
    evidence["result"] = "passed"
    write_evidence(args.evidence, evidence)
    print(json.dumps(_sanitize(evidence), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
