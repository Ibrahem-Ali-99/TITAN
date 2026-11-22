"""Run axe-core against the eight console pages at two viewports.

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
VIEWPORTS = ((1440, 900), (390, 844))
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
                page.goto(f"{base}{path}", wait_until="networkidle", timeout=30000)
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
                if path == "/":
                    page.keyboard.press("Tab")
                    focus = page.evaluate(
                        """() => {
                            const el = document.activeElement;
                            const style = el ? getComputedStyle(el) : null;
                            return {
                                tag: el ? el.tagName : null,
                                outline: style ? style.outlineStyle : null,
                                outlineWidth: style ? style.outlineWidth : null
                            };
                        }"""
                    )
                    visible = focus["outline"] not in (None, "none") and focus["tag"] not in (
                        None,
                        "BODY",
                    )
                    notes.append(
                        {"viewport": f"{width}x{height}", "focus": focus, "visible": visible}
                    )
                    if not visible:
                        findings.append(
                            {
                                "viewport": f"{width}x{height}",
                                "path": path,
                                "id": "keyboard-focus",
                                "impact": "serious",
                                "nodes": 1,
                                "help": json.dumps(focus),
                            }
                        )
                if path == "/warehouse":
                    notes.append(
                        {
                            "viewport": f"{width}x{height}",
                            "warehouse_text": page.locator("main").inner_text()[:500],
                        }
                    )
            page.close()
        browser.close()
    print(json.dumps({"violations": findings, "notes": notes}, indent=2))
    if findings:
        sys.exit(1)


if __name__ == "__main__":
    main()
