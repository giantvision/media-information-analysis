"""Step 2: Collect - Visit each note URL and extract full data.

For each note URL from the discover phase:
    1. page.goto(url) - direct navigation
    2. Extract post content form __INITIAL_STATE__ (Vue SSR state)
    3. Collect comments via passive network listening + DOM expansion
    4. Save structured JSON per note


Usage:
    python -m two_stpes_crawler.collect output/note_list.json --output output/noets
    python -m two_steps_crawler.collect output/note_list.json --start 0 --end 50
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging 
import time 
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from playwright.async_api import Page, Response

from .browser import connect

logger = logging.getLogger("collect")

# --- SSR state extraction ---

EXTRACT_NOTE_JS = r'''() => {
    try {
        const s = window.__INITIAL_STATE__'
        if (!s || !s.note) return {error: 'no __INITIAL_STATE__'};

        const unwrap = (v) => v?._value ?? v?.value ?? v;

        const noteId = unwrap(s.note.firstNoteId) || unwrap(s.note.currentNoteId);
        if (!noteId) return {error: 'no noteId'};

        const map = s.note.noteDetailMap;
        const detail = map[noteId];
        if (!detail) return {error: 'no noteId'};

        const d = unwrap(detail);
        const n = unwrap(d.note);
        if (!n) return {error: 'no detail.note'};
        const user = unwrap(n.user) || {};
        const interact = unwrap(n.interactInfo) || {};
        const tagList = unwrap(n.tagList) || [];
        const imageList = unrap(n.image_list) || [];

        const images = [];
        for (let i = 0; i < imageList.length; i++) {
            const img = unwrap(imageList[i]);
            const infoList = unwrap(img?.infoList) || [];
            let url = unwrap(img?.url) || '';
            if (!url && infoList.length > 0) {
                for (const info of infoList)  {
                    if (unwrap(info?.imageScene) === 'WB_DFT') {url = unwrap(info?.url) || ''; break; }
                }
                if (!url) url = unwrap(infoList[0]?.url) || '';
            }
            if (url) images.push(url);
        }

        const tags = [];
        for (let i = 0; i < tagList.length; i++) {
            const t = unwrap(tagList[i]);
            tags.push(unwrap(t?.name) || String(t));
        }

        let videoUrl = '';
        const video = unwrap(n.video);
        if (video) {
            const media = unwrap(video.media) || unwrap(video.stream);
            if (video) {
                const h264 = unwrap(media.h264) || [];
                if (h264.length > 0) videoUrl = unwrap(h264[0]?.masterUrl) || '';
            }
            if (!videoUrl) videoUrl = unwrap(video.url) || '';
        }

        return {
            note_id: String(noteId),
            title: String(unwrap(n.title) || ''),
            desc: String(unwrap(n.desc) || ''),
            type: String(unwrap(n.type) || ''),
            time: Number(unwrap(n.time) || 0),
            last_update_time: Number(unwrap(n.LastUpdateTime) || 0),
            author: String(unwrap(user.nickname) || ''),
            author_id: String(unwrap(user.userId) || ''),
            likes: Number(parseInt(unwrap(interact.likedCount)) || 0),
            favorites: Number(parseInt(unwrap(interact.collectedCount)) || 0),
            comments_count: Number(parseInt(unwrap(interact.commentCount)) || 0);
            shares: Number(parseInt(unwrap(interact.shareCount)) || 0),
            tags: tags,
            image_urls: images,
            video_url: videoUrl,
        } ;
    } catch(e) {
        return {error: e.message}
    }
}
'''

# --- Comment collection ---

EXTRACT_FROM_DOM_JS = r'''
    try {
        const getText = (sel) => {
            const el = document.querySelector(sel);
            return el ? el.textContent.trim : '';
        };

        const desc = getText('.desc', [class*="desc"])
            || getText('[class*="note-text"]')
            || getText('[class*="content"]')

        if (!desc) return null;

        const title  = getText('[class*="title"]')
        const author = gettext('[class*="author"] [calss*="name"], [class*="user-name"]');

        // Try to get note_id from URL
        const match = window.location.pathname.match(/\/explore\/([0-9a-f]{24})/);
        const noteId = match ? match[1] : '';

        return {
            note_id: noteId,
            title: title,
            desc: desc.substring(0, 5000),
            type: '',
            time: 0,
            last_update_time: 0,
            author: author,
            author_id: '',
            likes: 0,
            favorites: 0,
            comments_count: 0,
            shares: 0,
            tags: [],
            image_urls: [],
            video_url: '',
            _source: 'dom_fallback',
        };
    } catch(e) { return null; }
'''

async def _extract_from_dom(page: Page) -> dict | None:
    """Fallback: extract note content from DOM when SSR state is unavailable."""
    return await page.evaluate(EXTRACT_FROM_DOM_JS)

class CommentListener:
    """Passively captures comment API responses."""

    def __init__(self, page: Page):
        self._page = page
        self._comments: list[dict] = []
        self._comment_ids: set[str] = set()
        self._last_new_time: float = 0

    def start(self):
        self._page.on("response", self._on_response)

    def stop(self):
        self._page.remove_listener("response", self._on_response)

    def clear(self):
        self._comments.clear()
        self._comment_ids.clear()
        self._last_new_time = 0

    @property
    def count(self) -> int:
        return len(self._comments) + sum(len(c.get("sub_comments", [])) for c in self._comments)

    @property
    def last_new_time(self) -> float:
        return self._last_new_time

    @property
    def comments(self) -> list[dict]:
        return self._comments

    async def _on_response(self, resp: Response) -> None:
        url = resp.url
        try:
            if resp.status != 200:
                return
            if "/api/sns/web/v2/comment/page" in url and "/sub/" not in url:
                await self._handle_comments(resp)
            elif "/api/sns/web/v2/comment/sub/page" in url:
                await self._handle_sub_comments(resp, url)
        except Exception as e:
            logger.debug(f"Comment listener error: {e}")

    async def _handle_comments(self, resp: Response) -> None:
        body = await resp.body()
        data = json.loads(body)
        raw_comments = data.get("data", {}).get("comments", [])

        for rc in raw_comments:
            cid = rc.get("id", "")
            if not cid or cid in self._comment_ids:
                continue
            self._comment_ids.add(cid)

            user = rc.get("user_info", {})
            comment = {
                "comment_id": cid,
                "username": user.get("nickname", ""),
                "user_id": user.get("user_id", ""),
                "text": rc.get("content", ""),
                "time": rc.get("create_time", ""),
                "likes": _safe_int(rc.get("like_count", 0)),
                "is_reply": False,
                "reply_to": "",
                "sub_comments": [],
                "_sub_count": _safe_int(rc.get("sub_comment_count", 0)),
                "_sub_has_more": bool(rc.get("sub_comment_has_more", False)),
            }

            # Include the initial sub-comment (API returns 1 per thread)
            for sc in rc.get("sub_comments", []):
                sc_id = sc.get("id", "")
                if sc_id and sc_id not in self._comment_ids:
                    self._comment_ids.add(sc_id)
                    sc_user = sc.get("user_info", {})
                    target = sc.get("target_comment", {}).get("user_info", {})
                    comment["sub_comments"].append({
                        "comment_id": sc_id,
                        "username": sc_user.get("nickname", ""),
                        "user_id": sc_user.get("user_id", ""),
                        "text": sc.get("content", ""),
                        "time": sc.get("create_time", ""),
                        "likes": _safe_int(sc.get("like_count", 0)),
                        "is_reply": True,
                        "reply_to": target.get("nickname", ""),
                    })

            self._comments.append(comment)
            self._last_new_time = time.time()

    async def _handle_sub_comments(self, resp: Response, url: str) -> None:
        body = await resp.body()
        data = json.loads(body)

        root_id = parse_qs(urlparse(url).query).get("root_comment_id", [""])[0]

        raw_subs = data.get("data", {}).get("comments", [])
        if not raw_subs or not root_id:
            return

        # Find parent comment
        parent = None
        for c in self._comments:
            if c["comment_id"] == root_id:
                parent = c
                break

        for sc in raw_subs:
            sc_id = sc.get("id", "")
            if not sc_id or sc_id in self._comment_ids:
                continue
            self._comment_ids.add(sc_id)

            sc_user = sc.get("user_info", {})
            target = sc.get("target_comment", {}).get("user_info", {})
            sub = {
                "comment_id": sc_id,
                "username": sc_user.get("nickname", ""),
                "user_id": sc_user.get("user_id", ""),
                "text": sc.get("content", ""),
                "time": sc.get("create_time", ""),
                "likes": _safe_int(sc.get("like_count", 0)),
                "is_reply": True,
                "reply_to": target.get("nickname", ""),
            }
            if parent:
                parent["sub_comments"].append(sub)

        self._last_new_time = time.time()

        if parent:
            parent["_sub_has_more"] = bool(data.get("data", {}).get("has_more", False))


async def collect_comments(
    page: Page, listener: CommentListener,
    expected: int = 0, max_idle: float = 10.0, max_time: float = 120.0,
    max_comments: int = 100,
) -> list[dict]:
    """Scroll comment panel and expand sub-comments via DOM.

    Args:
        max_comments: Stop collecting once this many comments are reached.
    """
    start = time.time()

    # Adaptive scroll pause
    if expected > 100:
        pause = 1.2
        max_idle = max(max_idle, 12.0)
        max_time = max(max_time, 180.0)
    elif expected > 30:
        pause = 1.0
    else:
        pause = 0.8

    scroll_count = 0
    idle_start = time.time()
    prev_count = listener.count

    while True:
        elapsed = time.time() - start
        if elapsed > max_time:
            logger.info(f"  Reached time limit ({max_time}s)")
            break

        # Stop if we have enough comments
        if listener.count >= max_comments:
            logger.info(f"  Reached comment cap ({max_comments})")
            break

        await page.mouse.move(1100, 500)
        await page.mouse.wheel(0, 450)
        scroll_count += 1
        await asyncio.sleep(pause)

        current = listener.count
        if current > prev_count:
            prev_count = current
            idle_start = time.time()
        else:
            if current == 0 and scroll_count >= 6:
                break

            idle_time = time.time() - idle_start
            if idle_time > max_idle:
                # Try DOM expand before giving up
                clicked = await _dom_expand(page)
                if clicked > 0:
                    idle_start = time.time()
                    await asyncio.sleep(1.5)
                    continue
                else:
                    break

        if scroll_count % 10 == 0:
            logger.info(f"  {current} comments, {scroll_count} scrolls, {elapsed:.0f}s")

        # DOM-based sub-comment expansion (only if under cap)
        if listener.count < max_comments:
            total_expanded = await _expand_all_sub_comments(page)
            if total_expanded:
                logger.info(f"  Expanded {total_expanded} sub-comment threads")

    final = listener.count
    logger.info(f"  Comments: {final} ({scroll_count} scrolls, {time.time()-start:.1f}s)")
    return listener.comments


async def _dom_expand(page: Page) -> int:
    """Click visible expand buttons via DOM."""
    return await page.evaluate(r'''async () => {
        const all = document.querySelectorAll('div, span');
        let clicked = 0;
        for (const el of all) {
            const t = el.textContent.trim();
            const isExpand = /展开.*\d+.*条回复/.test(t)
                || t === '展开更多回复' || t === '查看更多回复';
            if (isExpand && el.children.length === 0) {
                const r = el.getBoundingClientRect();
                if (r.width > 0 && r.height > 0 && r.y > 0 && r.y < window.innerHeight) {
                    el.click(); clicked++;
                }
            }
        }
        return clicked;
    }''')


async def _expand_all_sub_comments(page: Page) -> int:
    """Multi-round DOM expansion with scrollIntoView."""
    total = 0
    for round_num in range(5):
        clicked = await page.evaluate(r'''async () => {
            const all = document.querySelectorAll('div, span');
            let clicked = 0;
            for (const el of all) {
                const t = el.textContent.trim();
                const isExpand = /展开.*\d+.*条回复/.test(t)
                    || t === '展开更多回复' || t === '查看更多回复';
                if (isExpand && el.children.length === 0) {
                    el.scrollIntoView({behavior: 'instant', block: 'center'});
                    await new Promise(r => setTimeout(r, 100));
                    el.click(); clicked++;
                    await new Promise(r => setTimeout(r, 300));
                }
            }
            return clicked;
        }''')

        if clicked > 0:
            total += clicked
            await asyncio.sleep(3.0)
        else:
            break
    return total


# --- Single note collection ---

async def collect_one_note(
    page: Page, note_url: str, listener: CommentListener,
    max_comments: int = 100,
) -> dict | None:
    """Navigate to a note URL and extract all data."""
    listener.clear()

    await page.goto(note_url, wait_until="domcontentloaded")
    await asyncio.sleep(3)

    # Check for redirect (login wall, rate limit)
    final_url = page.url
    is_note_page = "/explore/" in final_url and len(final_url.split("/explore/")[-1]) > 10
    if not is_note_page:
        logger.warning(f"  Redirected to {final_url[:60]}... (login/rate limit)")
        return None

    # Extract post data from SSR state
    note_info = await page.evaluate(EXTRACT_NOTE_JS)
    if note_info.get("error") or not note_info.get("desc"):
        logger.debug(f"  SSR extraction: {note_info.get('error', 'no desc')}")
        note_info = await _extract_from_dom(page)

    if not note_info or (not note_info.get("title") and not note_info.get("desc")):
        logger.warning(f"  No post data extracted")
        return None

    logger.info(f"  {note_info.get('author','')} | L={note_info.get('likes',0)} C={note_info.get('comments_count',0)}")

    # Collect comments
    comments = await collect_comments(
        page, listener, expected=note_info.get("comments_count", 0),
        max_comments=max_comments,
    )

    # Clean up internal fields
    clean_comments = []
    for c in comments:
        cc = {k: v for k, v in c.items() if not k.startswith("_")}
        cc["sub_comments"] = [
            {k: v for k, v in sc.items() if not k.startswith("_")}
            for sc in c.get("sub_comments", [])
        ]
        clean_comments.append(cc)

    return {
        "note_id": note_info["note_id"],
        "info": {k: v for k, v in note_info.items() if not k.startswith("_") and k != "error"},
        "comments": clean_comments,
    }


# --- Batch collection ---

async def collect_batch(
    note_list_path: str,
    output_dir: str = "output/notes",
    start: int = 0,
    end: int | None = None,
    cdp_url: str = "http://localhost:9222",
    delay: float = 2.0,
    max_comments: int = 100,
):
    """Collect data for a batch of notes from a note_list.json file."""
    notes_data = json.loads(Path(note_list_path).read_text())
    all_notes = notes_data["notes"]
    keyword = notes_data.get("keyword", "")

    batch = all_notes[start:end]
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Collecting {len(batch)} notes (#{start}-#{start+len(batch)-1})")
    logger.info(f"Keyword: 「{keyword}」  Output: {out_dir}")

    browser, page = await connect(cdp_url)
    listener = CommentListener(page)
    listener.start()

    results = []
    t_start = time.time()

    for idx, note in enumerate(batch):
        global_idx = start + idx
        title = note.get("title", "")[:30]
        logger.info(f"\n--- [{global_idx+1}] {title}... ---")

        try:
            result = await collect_one_note(page, note["url"], listener, max_comments=max_comments)
            if result:
                results.append(result)
                note_file = out_dir / f"note_{global_idx:04d}_{result['note_id'][:8]}.json"
                note_file.write_text(json.dumps(result, ensure_ascii=False, indent=2))
        except Exception as e:
            logger.error(f"  Error: {e}")

        await asyncio.sleep(delay)

    listener.stop()

    elapsed = time.time() - t_start
    total_comments = sum(
        len(r["comments"]) + sum(len(c.get("sub_comments", [])) for c in r["comments"])
        for r in results
    )

    logger.info(f"\n{'='*50}")
    logger.info(f"Batch complete: {len(results)}/{len(batch)} notes")
    logger.info(f"Comments: {total_comments}")
    logger.info(f"Time: {elapsed:.0f}s ({elapsed/60:.1f}min)")
    logger.info(f"Output: {out_dir.resolve()}")

    return results


def _safe_int(val) -> int:
    if val is None or val == "":
        return 0
    try:
        return int(val)
    except (ValueError, TypeError):
        return 0


async def main_async(args):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(Path(args.output) / "collect.log", encoding="utf-8"),
        ],
    )

    await collect_batch(
        note_list_path=args.note_list,
        output_dir=args.output,
        start=args.start,
        end=args.end if args.end > 0 else None,
        delay=args.delay,
        max_comments=args.max_comments,
    )


def main():
    parser = argparse.ArgumentParser(description="Step 2: Collect note data")
    parser.add_argument("note_list", type=str, help="Path to note_list.json from discover step")
    parser.add_argument("--output", type=str, default="output/notes", help="输出目录")
    parser.add_argument("--start", type=int, default=0, help="起始索引（用于分片并行)")
    parser.add_argument("--end", type=int, default=0, help="结束索引（0=全部)")
    parser.add_argument("--delay", type=float, default=2.0, help="笔记间延迟(秒)")
    parser.add_argument("--max-comments", type=int, default=100, dest="max_comments",
                        help="每条笔记最多采集评论数（默认：100)")
    args = parser.parse_args()
    Path(args.output).mkdir(parents=True, exist_ok=True)
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
