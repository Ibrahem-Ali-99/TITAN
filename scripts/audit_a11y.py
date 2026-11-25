"""Run axe-core against the eight console pages at four viewport widths.

Requires Playwright and a running console. The base URL defaults to the
development port and can be overridden with TITAN_WEB_BASE_URL.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.request

from playwright.sync_api import sync_playwright

PAGES = (
    "/",
    "/data-sources",
    "/streaming",
    "/pipelines",
    "/warehouse",
    "/data-quality",
    "/ml",
    "/monitoring",
)
VIEWPORTS = ((1440, 900), (1024, 768), (768, 1024), (390, 844))
AXE_URL = "https://cdn.jsdelivr.net/npm/axe-core@4.10.3/axe.min.js"


def main() -> None:
    base = os.environ.get("TITAN_WEB_BASE_URL", "http://localhost:3000").rstrip("/")
    axe = urllib.request.urlopen(AXE_URL, timeout=30).read().decode("utf-8")
    findings: list[dict[str, object]] = []
    notes: list[dict[str, object]] = []
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        for width, height in VIEWPORTS:
            page = browser.new_page(viewport={"width": width, "height": height})
            for path in PAGES:
                console_errors: list[str] = []
                page.on(  # type: ignore[call-overload]
                    "console",
                    lambda message, errors=console_errors: _remember_console(errors, message),
                )
                page.on("pageerror", lambda error, errors=console_errors: errors.append(str(error)))  # type: ignore[call-overload]
                page.goto(f"{base}{path}", wait_until="networkidle", timeout=60000)
                page.evaluate(axe)
                result = page.evaluate(
                    """async () => await axe.run(document, {
                        resultTypes: ['violations'],
                        runOnly: {type: 'tag', values: ['wcag2a', 'wcag2aa']}
                    })"""
                )
                for item in result["violations"]:
                    findings.append(
                        {
                            "viewport": f"{width}x{height}",
                            "path": path,
                            "id": item["id"],
                            "impact": item["impact"],
                            "nodes": len(item["nodes"]),
                            "help": item["help"],
                        }
                    )
                layout = page.evaluate(
                    """() => {
                        const root = document.documentElement;
                        const main = document.querySelector("main");
                        return {
                            overflow: root.scrollWidth - root.clientWidth,
                            h1: document.querySelector("h1")?.textContent?.trim() || null,
                            nav: document.querySelectorAll('nav[aria-label="Primary"] a').length,
                            tables: [...document.querySelectorAll("table")].map((table) => ({
                                headers: table.querySelectorAll("th").length,
                                label: table.closest("[aria-label]")
                                    ? table.closest("[aria-label]").getAttribute("aria-label")
                                    : null
                            })),
                            charts: document.querySelectorAll(".recharts-wrapper svg").length,
                            loading: (main?.innerText || "").startsWith("Loading")
                        };
                    }"""
                )
                viewport = f"{width}x{height}"
                if layout["overflow"] > 1:
                    findings.append(
                        _layout(viewport, path, "horizontal-overflow", layout["overflow"])
                    )
                if layout["nav"] != 8 or not layout["h1"]:
                    findings.append(_layout(viewport, path, "navigation", layout))
                if layout["loading"]:
                    findings.append(_layout(viewport, path, "still-loading", True))
                for table in layout["tables"]:
                    if table["headers"] < 1:
                        findings.append(_layout(viewport, path, "table-headers", table))
                if console_errors:
                    findings.append(_layout(viewport, path, "console-error", console_errors[:5]))
                if path == "/":
                    _keyboard(page, findings, notes, viewport)
                if path == "/data-sources":
                    _csv_validation(page, findings, notes, viewport)
                if path == "/warehouse":
                    notes.append(
                        {
                            "viewport": viewport,
                            "charts": layout["charts"],
                            "warehouse_text": page.locator("main").inner_text()[:400],
                        }
                    )
                    if layout["charts"] < 1:
                        findings.append(_layout(viewport, path, "missing-chart", layout["charts"]))
            page.close()
        browser.close()
    print(json.dumps({"violations": findings, "notes": notes}, indent=2))
    if findings:
        sys.exit(1)


def _remember_console(errors: list[str], message: object) -> None:
    if getattr(message, "type", None) == "error":
        errors.append(str(getattr(message, "text", message)))


def _layout(viewport: str, path: str, check: str, detail: object) -> dict[str, object]:
    return {
        "viewport": viewport,
        "path": path,
        "id": check,
        "impact": "serious",
        "nodes": 1,
        "help": json.dumps(detail),
    }


def _keyboard(
    page: object,
    findings: list[dict[str, object]],
    notes: list[dict[str, object]],
    viewport: str,
) -> None:
    page.keyboard.press("Tab")  # type: ignore[attr-defined]
    focus = page.evaluate(  # type: ignore[attr-defined]
        """() => {
            const el = document.activeElement;
            const style = el ? getComputedStyle(el) : null;
            return {
                tag: el ? el.tagName : null,
                name: el
                    ? (el.getAttribute("aria-label") || el.textContent || "").trim().slice(0, 80)
                    : null,
                outline: style ? style.outlineStyle : null,
                outlineWidth: style ? style.outlineWidth : null
            };
        }"""
    )
    visible = focus["outline"] not in (None, "none") and focus["tag"] not in (None, "BODY")
    notes.append({"viewport": viewport, "focus": focus, "visible": visible})
    if not visible:
        findings.append(_layout(viewport, "/", "keyboard-focus", focus))


def _csv_validation(
    page: object,
    findings: list[dict[str, object]],
    notes: list[dict[str, object]],
    viewport: str,
) -> None:
    validity = page.evaluate(  # type: ignore[attr-defined]
        """() => {
            const input = document.querySelector('input[type="file"]');
            const form = input ? input.closest("form") : null;
            const blocked = form ? !form.reportValidity() : null;
            return {
                label: input ? input.getAttribute("aria-label") : null,
                required: input ? input.required : null,
                blocked
            };
        }"""
    )
    notes.append({"viewport": viewport, "csv_validation": validity})
    if validity["label"] != "CSV file" or validity["blocked"] is not True:
        findings.append(_layout(viewport, "/data-sources", "csv-validation", validity))


if __name__ == "__main__":
    main()
