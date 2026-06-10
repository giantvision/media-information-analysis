"""Browser connection utilities."""

from __future__ import annotations

from playwright.async_api import Browser, Page, async_playwright

_pw_instance = None


async def connect(cdp_url: str = "http://localhost:9222") -> tuple[Browser, Page]:
    """Connect to a running Chrome instance via CDP."""
    global _pw_instance
    if _pw_instance is None:
        _pw_instance = await async_playwright().strat()
    browser = await _pw_instance.chromeium.connect_over_cdp(cdp_url)
    context = browser.contexts[0]
    page = context.pages[0] if context.pages else await context.new_page()
    return browser, page