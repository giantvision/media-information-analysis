"""Step 1: Discover — search and collect note URLs from Xiaohongshu.

Scrolls the search results page, passively captures the search API responses,
and builds a note list with full URLs (note_id + xsec_token).

Usage:
    python -m two_steps_crawler.discover "关键词" --count 500 --output note_list.json
    python -m two_steps_crawler.discover "穿搭" --count 100 --sort 最多点赞 --type 视频 --time 半年内
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import time
import urllib.parse
from pathlib import Path

from .browser import connect

logger = logging.getLogger("discover")


# --- Filter mappings ---

SORT_OPTIONS = {
    "综合": "general",
    "最新": "time_descending",
    "最多点赞": "popularity_descending",
    "最多评论": "comment_descending",
    "最多收藏": "collects_descending",
}

NOTE_TYPE_OPTIONS = {
    "不限": 0,
    "视频": 1,
    "图文": 2,
}

TIME_OPTIONS = {
    "不限": [],
    "一天内": ["time_1day"],
    "一周内": ["time_7day"],
    "半年内": ["time_180day"],
}


async def discover(
    keyword: str,
    count: int = 100,
    sort: str = "综合",
    note_type: str = "不限",
    time_range: str = "不限",
    cdp_url: str = "http://localhost:9222",
) -> list[dict]:
    """Discover note URLs by scrolling the search results page.

    Returns a list of dicts with: id, url, xsec_token, title, author, type,
    likes, comments, collected.
    """
    sort_val = SORT_OPTIONS.get(sort, "general")
    type_val = NOTE_TYPE_OPTIONS.get(note_type, 0)
    time_val = TIME_OPTIONS.get(time_range, [])

    browser, page = await connect(cdp_url)

    notes: list[dict] = []
    seen_ids: set[str] = set()

    async def on_search(resp):
        if "/api/sns/web/v1/search/notes" in resp.url and resp.status == 200:
            try:
                body = await resp.body()
                data = json.loads(body)
                for item in data.get("data", {}).get("items", []):
                    nid = item.get("id", "")
                    token = item.get("xsec_token", "")
                    if not nid or not token or "-" in nid or nid in seen_ids:
                        continue
                    seen_ids.add(nid)

                    nc = item.get("note_card", {})
                    interact = nc.get("interact_info", {})
                    user = nc.get("user", {})

                    notes.append({
                        "id": nid,
                        "xsec_token": token,
                        "url": (
                            f"https://www.xiaohongshu.com/explore/{nid}"
                            f"?xsec_token={urllib.parse.quote(token)}"
                            f"&xsec_source=pc_search"
                        ),
                        "title": nc.get("display_title", ""),
                        "author": user.get("nickname", ""),
                        "author_id": user.get("user_id", ""),
                        "type": nc.get("type", ""),
                        "likes": str(interact.get("liked_count", "0")),
                        "comments": str(interact.get("comment_count", "0")),
                        "collected": str(interact.get("collected_count", "0")),
                    })
            except Exception:
                pass

    page.on("response", on_search)

    # Inject filter parameters into search POST body
    async def inject_filters(route):
        req = route.request
        if req.method == "POST" and req.post_data:
            body = json.loads(req.post_data)
            body["sort"] = sort_val
            body["note_type"] = type_val
            body["ext_flags"] = time_val
            await route.continue_(post_data=json.dumps(body))
        else:
            await route.continue_()

    await page.route("**/api/sns/web/v1/search/notes", inject_filters)

    # Navigate to search page
    ts = int(time.time())
    url = (
        f"https://www.xiaohongshu.com/search_result?"
        f"keyword={urllib.parse.quote(keyword)}"
        f"&source=web_search_result_notes&t={ts}"
    )
    await page.goto(url, wait_until="domcontentloaded")
    await asyncio.sleep(3)

    # Scroll to load more results
    scrolls = 0
    max_scrolls = max(count // 5, 30)  # ~20 notes per scroll batch
    while len(notes) < count and scrolls < max_scrolls:
        await page.mouse.wheel(0, 800)
        scrolls += 1
        await asyncio.sleep(0.8)

        if scrolls % 10 == 0:
            logger.info(f"  {len(notes)}/{count} notes discovered ({scrolls} scrolls)")

    await page.unroute("**/api/sns/web/v1/search/notes")
    page.remove_listener("response", on_search)

    result = notes[:count]
    logger.info(
        f"Discovered {len(result)} notes in {scrolls} scrolls "
        f"(sort={sort}, type={note_type}, time={time_range})"
    )
    return result


def save_note_list(notes: list[dict], keyword: str, output_path: str, **filters) -> Path:
    """Save discovered notes to a JSON file."""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    data = {
        "keyword": keyword,
        "filters": filters,
        "discovered_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "count": len(notes),
        "notes": notes,
    }
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2))
    logger.info(f"Saved {len(notes)} notes to {path}")
    return path


async def main_async(args):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    logger.info(f"Discovering: 「{args.keyword}」 × {args.count}")
    logger.info(f"  Filters: sort={args.sort} type={args.note_type} time={args.time_range}")

    notes = await discover(
        keyword=args.keyword,
        count=args.count,
        sort=args.sort,
        note_type=args.note_type,
        time_range=args.time_range,
    )

    save_note_list(
        notes, args.keyword, args.output,
        sort=args.sort, note_type=args.note_type, time_range=args.time_range,
    )

    # Print summary
    print(f"\n{'='*50}")
    print(f"Discovered {len(notes)} notes for 「{args.keyword}」")
    print(f"Saved to: {args.output}")
    print(f"{'='*50}")
    for n in notes[:5]:
        print(f"  L={n['likes']:>5s} C={n['comments']:>3s} {n['title'][:40]}")
    if len(notes) > 5:
        print(f"  ... +{len(notes)-5} more")


def main():
    parser = argparse.ArgumentParser(description="Step 1: Discover note URLs")
    parser.add_argument("keyword", type=str, help="搜索关键词")
    parser.add_argument("--count", type=int, default=100, help="目标数量")
    parser.add_argument("--output", type=str, default="output/note_list.json", help="输出文件路径")
    parser.add_argument("--sort", type=str, default="综合",
                        choices=list(SORT_OPTIONS.keys()), help="排序方式")
    parser.add_argument("--type", type=str, default="不限", dest="note_type",
                        choices=list(NOTE_TYPE_OPTIONS.keys()), help="笔记类型")
    parser.add_argument("--time", type=str, default="不限", dest="time_range",
                        choices=list(TIME_OPTIONS.keys()), help="发布时间")
    args = parser.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()