# Two-Step Crawler — Architecture

## Overview

A lightweight two-phase Xiaohongshu crawler that separates **discovery** (collecting note URLs) from **collection** (extracting full data per note). No LLM, no CV, no heavy dependencies — only Playwright + passive network interception + minimal DOM operations.

```
Phase 1: DISCOVER (fast, lightweight)        Phase 2: COLLECT (parallelizable)

┌─────────────────────────────┐             ┌──────────────────────────────┐
│ Search page scroll          │             │ For each URL:                │
│   ↓                         │             │    page.goto(url)            │
│ Passive: search API capture │  ────────▶  │    SSR state → post content  │
│   ↓                         │  note_list  │    Network → comments        │
│ note_list.json              │  .json      │    DOM → expand sub-comments │
│ (id + xsec_token + URL)     │             │    → note_XXXX.json          │
└─────────────────────────────┘             └──────────────────────────────┘

│ ~12 notes/sec                               Can split across N browsers
```

## Module Structure

```
src/two_steps_crawler/
├── __init__.py         Package marker
├── __main__.py         CLI: python -m two_steps_crawler <discover|collect>
├── browser.py          Playwright CDP connection (singleton)
├── discover.py         Step 1: Search → note URL list
└── collect.py          Step 2: Visit URLs → extract data + comments
```

| Module | Purpose |
|--------|---------|
| `browser.py` | `connect()`: singleton Playwright instance, CDP attach to running Chrome |
| `discover.py` | `discover()`: scroll search page, capture search API, inject filters via route |
| `collect.py` | `collect_one_note()`: SSR state extraction + comment scroll/expand + save JSON |

## Dependencies

Only **Playwright**. No OpenCV, no numpy, no LLM APIs.

```
playwright >= 1.40
```

## Step 1: Discover

### Flow

```
1. Connect to Chrome via CDP
2. route.continue_() injects sort/type/time into search POST body
3. page.goto(search_url)
4. page.on("response") captures /api/sns/web/v1/search/notes responses
5. Scroll to trigger pagination (~20 notes per API response)
6. Build list: {id, xsec_token, url, title, author, likes, comments, type}
7. Save to note_list.json
```

### Filter Injection

The search API uses POST with filters in the body. We intercept and modify via `route.continue_()`:

| CLI Flag | POST Field | Values |
|----------|------------|--------|
| `--sort` | `sort` | `general` / `time_descending` / `popularity_descending` / `comment_descending` / `collects_descending` |
| `--type` | `note_type` | `0` (all) / `1` (video) / `2` (image-text) |
| `--time` | `ext_flags` | `[]` / `["time_1day"]` / `["time_7day"]` / `["time_180day"]` |

### Output: note_list.json

```json
{
  "keyword": "穿搭",
  "filters": {"sort": "最多点赞", "note_type": "视频", "time_range": "半年内"},
  "discovered_at": "2026-06-05 20:46:28",
  "count": 500,
  "notes": [
    {
      "id": "68f9e843000000000301c42e",
      "xsec_token": "ABQIb3D3...",
      "url": "https://www.xiaohongshu.com/explore/68f9e843...?xsec_token=...",
      "title": "...",
      "author": "...",
      "type": "normal",
      "likes": "3073",
      "comments": "41",
      "collected": "1947"
    }
  ]
}
```

## Step 2: Collect

### Flow per Note

```
1. page.goto(note_url)  — direct navigation with xsec_token
2. Check for redirect (login wall → skip note)
3. Extract post data:
     Primary:  window.__INITIAL_STATE__.note.noteDetailMap[noteId].note (Vue SSR)
     Fallback: DOM selectors (.desc, [class*="note-text"])
4. Scroll comment panel → network listener captures comment/page API
5. DOM expand: page.evaluate() clicks "展开X条回复" via scrollIntoView
6. Stop at max_comments cap (default: 100)
7. Save note_XXXX.json
```

### SSR State Extraction

Xiaohongshu uses Vue 3 with SSR hydration. The page's `window.__INITIAL_STATE__` contains the full note data wrapped in Vue reactive refs.

Key path: `__INITIAL_STATE__` → `note` → `noteDetailMap[noteId]` → `.note` → actual fields

```javascript
const unwrap = (v) => v?._value ?? v?.value ?? v;

const noteId = unwrap(s.note.firstNoteId);
const detail = s.note.noteDetailMap[noteId];
const n = unwrap(unwrap(detail).note);  // two levels: detail → detail.note

// n now has: title, desc, user, interactInfo, tagList, imageList, video, etc.
```

Available fields from SSR: `note_id`, `title`, `desc`, `type`, `time`, `author`, `author_id`, `likes`, `favorites`, `comments_count`, `shares`, `tags`, `image_urls`, `video_url`.

### Redirect Detection

Direct URL navigation may trigger login walls or rate limits:

```python
is_note_page = "/explore/" in page.url and len(page.url.split("/explore/")[-1]) > 10
if not is_note_page:
    return None  # redirected away from note page
```

### Comment Collection

```
CommentListener (passive network capture)
├── /api/sns/web/v2/comment/page      → top-level comments (paginated by scroll)
└── /api/sns/web/v2/comment/sub/page → sub-comment expansion
    └── root_comment_id extracted from URL query param (not response body)

Scroll loop:
├── Adaptive pause: 1.2s (>100 expected) / 1.0s (>30) / 0.8s (small)
├── Stop: max_comments cap / time limit / idle timeout / zero-comment early exit
└── DOM expand: scrollIntoView + click "展开X条回复", up to 5 rounds
    └── Skipped entirely if already at max_comments cap
```

### Output: note_XXXX.json

```json
{
  "note_id": "68a1db82000000001b03da66",
  "info": {
    "note_id": "...",
    "title": "...",
    "desc": "full post content...",
    "author": "...",
    "author_id": "...",
    "likes": 463,
    "favorites": 200,
    "comments_count": 1194,
    "tags": ["tag1", "tag2"],
    "image_urls": ["https://..."],
    "time": 1755437954000
  },
  "comments": [
    {
      "comment_id": "...",
      "username": "用户A",
      "user_id": "...",
      "text": "评论内容",
      "time": "1755500000",
      "likes": 5,
      "is_reply": false,
      "sub_comments": [
        {"username": "用户B", "text": "回复", "reply_to": "用户A"}
      ]
    }
  ]
}
```

## CLI Reference

```bash
# Step 1: Discover
python -m two_steps_crawler discover "关键词" \
  --count 500 \
  --sort 最多评论 \
  --type 图文 \
  --time 半年内 \
  --output output/note_list.json

# Step 2: Collect (full)
python -m two_steps_crawler collect output/note_list.json \
  --output output/notes \
  --delay 2.0 \
  --max-comments 100

# Step 2: Collect (parallel slices)
python -m two_steps_crawler collect output/note_list.json --start 0 --end 100
python -m two_steps_crawler collect output/note_list.json --start 100 --end 200
```

| Flag | Default | Phase | Description |
|------|---------|-------|-------------|
| `--count` | 100 | discover | Number of notes to find |
| `--sort` | 综合 | discover | 综合/最新/最多点赞/最多评论/最多收藏 |
| `--type` | 不限 | discover | 不限/视频/图文 |
| `--time` | 不限 | discover | 不限/一天内/一周内/半年内 |
| `--output` | varies | both | Output file/directory |
| `--start` | 0 | collect | Slice start index |
| `--end` | 0 | collect | Slice end index (0=all) |
| `--delay` | 2.0 | collect | Seconds between notes |
| `--max-comments` | 100 | collect | Comment cap per note |

## Performance

| Metric | Value |
|--------|-------|
| Discover speed | ~12 notes/sec (~500 notes in 40s) |
| Collect speed (≤100 comments) | ~30-50s per note |
| Collect speed (zero comments) | ~8s per note |
| Comment cap effect | 1500-comment posts: 2min → 30s |

## Prerequisites

```bash
# 1. Start Chrome with remote debugging
chrome --remote-debugging-port=9222

# 2. Log in to xiaohongshu.com manually

# 3. Install
python3 -m venv .venv
.venv/bin/pip install -e .
playwright install chromium
```

## Code Review Fixes

| Issue | Fix |
|-------|-----|
| Redirect detection: chain comparison bug | Replaced with simple `/explore/` URL check |
| Silent exception in CommentListener | Added `logger.debug` |
| `_source` field leaking to output JSON | Filter keys starting with `_` |
| `max_comments` not passed through CLI→batch→note | Full parameter chain + `--max-comments` flag |
| `from urllib.parse` imported inside loop | Moved to top-level import |