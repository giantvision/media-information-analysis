"""Runtime executor for real browser capture and recipe replay.

This module intentionally uses normal browser sessions and user-authorized
state. It does not implement anti-bot bypass, captcha solving, signature
cracking, or account/proxy automation.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

from .extractor import NetworkExtractor
from .feedback import FeedbackMemory
from .network import NetworkSnapshot
from .recipe import Recipe


class RuntimeExecutor:
    def __init__(self, recipe: Recipe):
        self.recipe = recipe

    async def run(
        self,
        url: str,
        output_path: str | Path | None = None,
        user_data_dir: str | Path | None = None,
        wait_ms: int = 3000,
        headless: bool = False,
    ) -> dict[str, Any]:
        try:
            from playwright.async_api import TimeoutError as PlaywrightTimeoutError
            from playwright.async_api import async_playwright
        except ImportError as exc:
            raise RuntimeError(
                "Playwright is not installed. Install it with `pip install playwright` "
                "and run `playwright install chromium`."
            ) from exc

        snapshots: list[dict[str, Any]] = []
        async with async_playwright() as p:
            if user_data_dir:
                context = await p.chromium.launch_persistent_context(
                    str(user_data_dir),
                    headless=headless,
                    viewport={"width": 1365, "height": 900},
                )
                page = context.pages[0] if context.pages else await context.new_page()
                close_target = context
            else:
                browser = await p.chromium.launch(headless=headless)
                context = await browser.new_context(viewport={"width": 1365, "height": 900})
                page = await context.new_page()
                close_target = browser

            async def on_response(response):
                content_type = response.headers.get("content-type", "")
                body: Any = None
                if "json" in content_type.lower() or "text" in content_type.lower():
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
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
            except PlaywrightTimeoutError:
                pass
            await self._replay_actions(page, self.recipe.raw.get("actions", []))
            await page.wait_for_timeout(wait_ms)
            await close_target.close()

        network_snapshots = [
            NetworkSnapshot.from_dict(item, index=i) for i, item in enumerate(snapshots)
        ]
        extraction = NetworkExtractor(self.recipe).extract(network_snapshots)
        memory = FeedbackMemory()
        events = memory.observe_extraction(extraction)
        result = {
            "url": url,
            "snapshot_count": len(snapshots),
            "extraction": extraction,
            "feedback_events": [event.__dict__ for event in events],
            "snapshots": snapshots,
        }
        if output_path:
            Path(output_path).write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        return result

    async def _replay_actions(self, page: Any, actions: list[dict[str, Any]]) -> None:
        for action in actions:
            action_type = action.get("type")
            if action_type == "wait":
                if "duration_ms" in action:
                    await page.wait_for_timeout(int(action["duration_ms"]))
                elif action.get("until") == "network_idle":
                    try:
                        await page.wait_for_load_state("networkidle", timeout=15_000)
                    except Exception:
                        pass
            elif action_type == "scroll":
                distance = int(action.get("distance", 1000))
                repeat = int(action.get("repeat", 1))
                pause_ms = int(action.get("pause_ms", 800))
                direction = action.get("direction", "down")
                delta = distance if direction == "down" else -distance
                for _ in range(repeat):
                    await page.mouse.wheel(0, delta)
                    await page.wait_for_timeout(pause_ms)
            elif action_type == "scroll_until_stable":
                max_rounds = int(action.get("max_rounds", 60))
                distance = int(action.get("distance", 1000))
                pause_ms = int(action.get("pause_ms", 1200))
                stable_rounds = int(action.get("stable_rounds", 3))
                stable_count = 0
                last_height = -1
                for _ in range(max_rounds):
                    current_height = await page.evaluate("() => document.body.scrollHeight")
                    await page.mouse.wheel(0, distance)
                    await page.wait_for_timeout(pause_ms)
                    next_height = await page.evaluate("() => document.body.scrollHeight")
                    if next_height == current_height == last_height:
                        stable_count += 1
                    else:
                        stable_count = 0
                    last_height = next_height
                    if stable_count >= stable_rounds:
                        break
            elif action_type == "click":
                selector = action.get("selector")
                text = action.get("text")
                if selector:
                    await page.click(selector, timeout=10_000)
                elif text:
                    await page.get_by_text(text).click(timeout=10_000)
            elif action_type == "type":
                selector = action["selector"]
                await page.fill(selector, action.get("text", ""))
            elif action_type == "press":
                await page.keyboard.press(action["key"])


def run_sync(
    recipe: Recipe,
    url: str,
    output_path: str | Path | None = None,
    user_data_dir: str | Path | None = None,
    wait_ms: int = 3000,
    headless: bool = False,
) -> dict[str, Any]:
    return asyncio.run(
        RuntimeExecutor(recipe).run(
            url=url,
            output_path=output_path,
            user_data_dir=user_data_dir,
            wait_ms=wait_ms,
            headless=headless,
        )
    )


async def open_session(
    url: str,
    user_data_dir: str | Path,
    headless: bool = False,
) -> None:
    try:
        from playwright.async_api import async_playwright
    except ImportError as exc:
        raise RuntimeError(
            "Playwright is not installed. Install it with `pip install playwright` "
            "and run `playwright install chromium`."
        ) from exc

    async with async_playwright() as p:
        context = await p.chromium.launch_persistent_context(
            str(user_data_dir),
            headless=headless,
            viewport={"width": 1365, "height": 900},
        )
        page = context.pages[0] if context.pages else await context.new_page()
        await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        print("Browser session is open. Log in manually if needed, then close the browser window.")
        while context.pages:
            await page.wait_for_timeout(1000)
        await context.close()


def open_session_sync(url: str, user_data_dir: str | Path, headless: bool = False) -> None:
    asyncio.run(open_session(url=url, user_data_dir=user_data_dir, headless=headless))
