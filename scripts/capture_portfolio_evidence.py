"""Capture verified portfolio screenshots for all TITAN dashboard routes.

The light captures use the real theme button and are checked after the CSS
transition, so the archive records the actual rendered palette rather than a
manually forced DOM attribute.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, cast

from playwright.sync_api import Page, sync_playwright

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "docs" / "assets" / "v020-visual-polish"
PAGES = (
    ("overview", "/"),
    ("data-sources", "/data-sources"),
    ("streaming", "/streaming"),
    ("pipelines", "/pipelines"),
    ("warehouse", "/warehouse"),
    ("data-quality", "/data-quality"),
    ("ml", "/ml"),
    ("monitoring", "/monitoring"),
)
CAPTURES = (("dark-1440x900", 1440, 900), ("dark-390x844", 390, 844), ("light-1440x900", 1440, 900))


def main() -> None:
    base = os.environ.get("TITAN_WEB_BASE_URL", "http://localhost:3000").rstrip("/")
    OUTPUT.mkdir(parents=True, exist_ok=True)
    for old in OUTPUT.glob("*.png"):
        old.unlink()
    metadata: dict[str, Any] = {
        "base_url": base,
        "routes": {},
        "assertions": [],
    }
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(base, wait_until="domcontentloaded", timeout=60000)
        for slug, route in PAGES:
            route_meta: dict[str, Any] = {}
            page.set_viewport_size({"width": 1440, "height": 900})
            _set_theme(page, "dark")
            page.goto(f"{base}{route}", wait_until="networkidle", timeout=60000)
            _assert_route(page, route)
            route_meta["dark"] = _snapshot(page)
            page.screenshot(path=str(OUTPUT / f"{slug}-dark-1440x900-full.png"), full_page=True)

            page.set_viewport_size({"width": 390, "height": 844})
            page.goto(f"{base}{route}", wait_until="networkidle", timeout=60000)
            _assert_route(page, route)
            page.screenshot(path=str(OUTPUT / f"{slug}-dark-390x844-full.png"), full_page=True)

            page.set_viewport_size({"width": 1440, "height": 900})
            page.goto(f"{base}{route}", wait_until="networkidle", timeout=60000)
            if page.get_by_role("button", name="Use light theme").count() != 1:
                raise AssertionError(f"light-theme control missing on {route}")
            page.get_by_role("button", name="Use light theme").click()
            page.wait_for_timeout(450)
            light = _snapshot(page)
            if light["theme"] != "light" or light["background"] == route_meta["dark"]["background"]:
                raise AssertionError(f"light theme did not render distinctly on {route}: {light}")
            page.screenshot(path=str(OUTPUT / f"{slug}-light-1440x900-full.png"), full_page=True)
            route_meta["light"] = light
            _set_theme(page, "dark")
            metadata["routes"][route] = route_meta
        browser.close()
    (OUTPUT / "capture-metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    print(json.dumps(metadata, indent=2))


def _set_theme(page: Page, theme: str) -> None:
    page.evaluate("(theme) => window.localStorage.setItem('titan-theme', theme)", theme)


def _snapshot(page: Page) -> dict[str, str]:
    return cast(
        dict[str, str],
        page.evaluate(
            """() => {
            const app = document.querySelector('.app');
            const card = document.querySelector('.card');
            const heading = document.querySelector('h1');
            const style = (element) => element ? getComputedStyle(element) : null;
            return {
                theme: document.documentElement.dataset.theme || '',
                background: style(app)?.backgroundColor || '',
                surface: style(card)?.backgroundColor || '',
                text: style(heading)?.color || '',
                border: style(card)?.borderTopColor || '',
            };
        }"""
        ),
    )


def _assert_route(page: Page, route: str) -> None:
    if not page.locator("main h1").count():
        raise AssertionError(f"missing page heading on {route}")
    if page.locator(".loading-state").count():
        raise AssertionError(f"route still loading on {route}")
    if route == "/streaming":
        bars = page.locator(".mini-bar-row > i > b")
        if bars.count() < 1:
            raise AssertionError("streaming distribution bars are missing")
        for index in range(bars.count()):
            bar = bars.nth(index)
            if float(bar.bounding_box()["width"]) <= 0:  # type: ignore[index]
                raise AssertionError("streaming distribution bar has no visible width")
    if route == "/":
        if not page.get_by_text("Event throughput").count():
            raise AssertionError("overview throughput panel is missing")
        if not page.locator(".recharts-wrapper svg").count():
            raise AssertionError("overview throughput chart is missing")


if __name__ == "__main__":
    main()
