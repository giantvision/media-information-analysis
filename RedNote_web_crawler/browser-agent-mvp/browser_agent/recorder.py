"""Optional Playwright network recorder.

The runtime core works offline with saved snapshots. This module is a thin
adapter for real browser capture when Playwright is installed.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


async def record_url(url: str, output_path: str | Path, wait_ms: int = 3000) -> list[dict[str, Any]]:
    try:
        from playwright.async_api import async_playwright
    except ImportError as exc:
        raise RuntimeError(
            "Playwright is not installed. Install it with `pip install playwright` "
            "and run `playwright install chromium` to enable live recording."
        ) from exc

    snapshots: list[dict[str, Any]] = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()

        async def on_response(response):
            content_type = response.headers.get("content-type", "")
            body: Any = None
            if "json" in content_type.lower():
                try:
                    body = await response.json()
                except Exception:
                    try:
                        body = await response.text()
                    except Exception:
                        body = None
            snapshots.append(
                {
                    "response_id": f"resp_{len(snapshots)}",
                    "url": response.url,
                    "method": response.request.method,
                    "status": response.status,
                    "content_type": content_type,
                    "request": {
                        "url": response.request.url,
                        "method": response.request.method,
                        "resource_type": response.request.resource_type,
                        "post_data": response.request.post_data,
                    },
                    "body": body,
                }
            )

        page.on("response", on_response)
        await page.goto(url, wait_until="networkidle")
        await page.wait_for_timeout(wait_ms)
        await browser.close()

    path = Path(output_path)
    path.write_text(json.dumps(snapshots, ensure_ascii=False, indent=2), encoding="utf-8")
    return snapshots
