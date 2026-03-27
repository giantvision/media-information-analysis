#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
百度贴吧批量帖子爬虫（多线程 · JSONL 输出版）
==============================================
功能：
  1. 扫描 category_classify_output_split_20260326 目录下指定的 JSONL 文件
  2. 提取所有 thread_id 进行批量爬取
  3. 多线程并行爬取，降低总耗时
  4. 同一输入 JSONL 的所有帖子合并写入同名输出 JSONL（每帖一行）
  5. 支持断点续传（已写入的 thread_id 自动跳过）

只处理以下4种JSONL文件:
  - problem_solving_posts.jsonl
  - review_or_suggestion_posts.jsonl
  - player_conflict_posts.jsonl
  - update_prediction_posts.jsonl

输出结构（镜像输入目录，同名 JSONL，每行一个帖子的完整爬取结果）:
  category_classify_output_split_post_20260326/
    jmzy_split_data/
      negative/
        problem_solving_posts.jsonl
        ...

用法:
  python tieba_batch_crawler.py [--workers N] [--max-pages N] [--input-dir DIR] [--output-dir DIR]

示例:
  python tieba_batch_crawler.py --workers 5 --max-pages 50
"""

import re
import json
import time
import sys
import logging
import hashlib
import argparse
import threading
from datetime import datetime
from pathlib import Path
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

# ============================================================
# 日志配置（线程安全）
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ============================================================
# 目录 & 文件常量
# ============================================================
INPUT_BASE_DIR  = "category_classify_output_split_20260326"
OUTPUT_BASE_DIR = "category_classify_output_split_post_20260326"

GAME_DIRS = [
    "jmzy_split_data",
    "mdtx_split_data",
    "sgz_split_data",
    "stzb_split_data",
    "wyxs_split_data",
]

SENTIMENT_DIRS = ["negative", "neutral", "positive"]

TARGET_FILES = {
    "problem_solving_posts.jsonl",
    "review_or_suggestion_posts.jsonl",
    "player_conflict_posts.jsonl",
    "update_prediction_posts.jsonl",
}

# ============================================================
# API 常量
# ============================================================
CLIENT_API_URL       = "https://tiebac.baidu.com/c/f/pb/page"
CLIENT_FLOOR_API_URL = "https://tiebac.baidu.com/c/f/pb/floor"
MOBILE_API_URL       = "https://tieba.baidu.com/mg/p/getPbData"
SIGN_KEY             = "tiebaclient!!!"

CLIENT_COMMON_PARAMS = {
    "_client_type":    "2",
    "_client_version": "12.57.1.0",
    "_os_version":     "33",
    "_phone_imei":     "000000000000000",
    "from":            "tieba",
    "cuid":            "baidutiebaapp",
}

CLIENT_HEADERS = {
    "User-Agent":      "bdtb for Android 12.57.1.0",
    "Content-Type":    "application/x-www-form-urlencoded",
    "Accept":          "*/*",
    "Accept-Encoding": "gzip",
    "Connection":      "keep-alive",
    "Host":            "tiebac.baidu.com",
}

MOBILE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/17.0 Mobile/15E148 Safari/604.1"
    ),
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Referer":         "https://tieba.baidu.com/",
}

REQUEST_DELAY = 0.8


# ============================================================
# 工具函数
# ============================================================
def clean_text(text: str) -> str:
    if not text:
        return ""
    return re.sub(r'\s+', ' ', text).strip()


def safe_int(val, default=0):
    if val is None:
        return default
    try:
        s = str(val).strip()
        if '万' in s:
            return int(float(s.replace('万', '')) * 10000)
        if '亿' in s:
            return int(float(s.replace('亿', '')) * 100000000)
        return int(s)
    except (ValueError, TypeError):
        return default


def format_timestamp(ts) -> str:
    if not ts:
        return ""
    try:
        ts_val = int(ts)
        if ts_val > 1e12:
            ts_val //= 1000
        return datetime.fromtimestamp(ts_val).strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError, OSError):
        return str(ts)


def calc_sign(params: dict) -> str:
    sign_str = ''.join(f"{k}={v}" for k, v in sorted(params.items()))
    sign_str += SIGN_KEY
    return hashlib.md5(sign_str.encode('utf-8')).hexdigest()


def _extract_user_from_author_dict(author_dict: dict) -> dict:
    if not isinstance(author_dict, dict):
        return {}
    uid = str(author_dict.get('id', ''))
    if not uid:
        return {}
    return {
        "user_id":    uid,
        "user_name":  author_dict.get('name', ''),
        "nick_name":  (author_dict.get('name_show', '')
                       or author_dict.get('name', '')
                       or author_dict.get('nick_name', '')),
        "level_id":   safe_int(author_dict.get('level_id', 0)),
        "portrait":   author_dict.get('portrait', ''),
        "ip_address": author_dict.get('ip_address', ''),
    }


def _resolve_sub_comment_user(sp: dict, user_map: dict) -> tuple:
    sp_uid    = str(sp.get('author_id', ''))
    sp_author = sp.get('author', {})
    if not sp_uid and isinstance(sp_author, dict):
        sp_uid = str(sp_author.get('id', ''))

    if sp_uid and sp_uid in user_map:
        u    = user_map[sp_uid]
        name = u.get('nick_name', '') or u.get('name_show', '') or u.get('user_name', '')
        if name:
            return sp_uid, name

    if isinstance(sp_author, dict) and sp_author:
        ex = _extract_user_from_author_dict(sp_author)
        if ex:
            eid = ex['user_id']
            if eid not in user_map:
                user_map[eid] = ex
            elif not user_map[eid].get('nick_name'):
                user_map[eid].update({k: v for k, v in ex.items() if v})
            name = ex.get('nick_name', '') or ex.get('user_name', '')
            if name:
                return sp_uid or eid, name
    return sp_uid, ''


def _resolve_reply_to(sp: dict, user_map: dict) -> dict:
    if not isinstance(sp, dict):
        return {}

    reply_uid, reply_name = '', ''

    for block in sp.get('content', []):
        if not isinstance(block, dict):
            continue
        btype = str(block.get('type', ''))
        if btype == '4':
            uid = str(block.get('uid', ''))
            if uid and uid != '0':
                reply_uid  = uid
                reply_name = block.get('text', '')
                if uid in user_map:
                    u = user_map[uid]
                    reply_name = (u.get('nick_name', '') or u.get('name_show', '')
                                  or u.get('user_name', '') or reply_name)
                break
        elif btype == '11':
            uid = str(block.get('uid', ''))
            if uid and uid != '0':
                reply_uid  = uid
                reply_name = reply_name or (block.get('name_show', '')
                                            or block.get('name', '')
                                            or block.get('text', ''))
                break

    if not reply_name:
        title = sp.get('title', '')
        if isinstance(title, str) and title.strip():
            reply_name = title.strip()

    if not reply_uid and not reply_name:
        for key in ('reply_to_id', 'reply_uid', 'reply_to_user_id'):
            v = sp.get(key)
            if v and str(v) != '0':
                reply_uid = str(v)
                break
        for key in ('reply_to_user', 'replyUser', 'reply_user'):
            obj = sp.get(key, {})
            if isinstance(obj, dict) and obj:
                reply_uid  = reply_uid  or str(obj.get('id', ''))
                reply_name = reply_name or (obj.get('name_show', '') or obj.get('name', ''))
                break

    if reply_name and not reply_uid:
        for uid, u in user_map.items():
            if (u.get('nick_name') == reply_name or u.get('name_show') == reply_name
                    or u.get('user_name') == reply_name):
                reply_uid = uid
                break

    if reply_uid and not reply_name and reply_uid in user_map:
        u = user_map[reply_uid]
        reply_name = u.get('nick_name', '') or u.get('name_show', '') or u.get('user_name', '')

    return {"uid": reply_uid, "name": reply_name} if (reply_uid or reply_name) else {}


def _fix_content_reply_to(content: str, reply_to: dict) -> str:
    if not reply_to or not reply_to.get('name'):
        return content
    return re.sub(
        r'回复\s*(?:@用户\d+)?\s*\n?\s*:',
        f'回复 @{reply_to["name"]} :',
        content, count=1,
    )


def parse_content_blocks(blocks, user_map: dict = None) -> tuple:
    if not blocks:
        return "", []
    if isinstance(blocks, str):
        return blocks.strip(), []
    if not isinstance(blocks, list):
        return str(blocks).strip(), []
    if user_map is None:
        user_map = {}

    parts, images = [], []

    for block in blocks:
        if not isinstance(block, dict):
            parts.append(str(block))
            continue
        btype = str(block.get('type', ''))

        if btype == '0':
            t = block.get('text', '')
            if t: parts.append(t)
        elif btype == '1':
            lnk = block.get('link', '') or block.get('text', '')
            if lnk: parts.append(lnk)
        elif btype == '2':
            c = block.get('c', '') or block.get('text', '')
            if c: parts.append(c)
        elif btype == '3':
            url = (block.get('origin_src', '') or block.get('big_cdn_src', '')
                   or block.get('cdn_src', '') or block.get('src', ''))
            if url:
                if url.startswith('//'): url = 'https:' + url
                images.append(url)
        elif btype == '4':
            uid = str(block.get('uid', ''))
            if uid and uid != '0':
                name = block.get('text', '')
                if uid in user_map:
                    u = user_map[uid]
                    name = (u.get('nick_name', '') or u.get('name_show', '')
                            or u.get('user_name', '') or name)
                parts.append(name if name else f'@用户{uid}')
        elif btype == '5':
            v = block.get('link', '') or block.get('text', '')
            if v: parts.append(f'[视频: {v}]')
        elif btype == '9':
            t = block.get('text', '')
            if t: parts.append(t)
        elif btype == '10':
            parts.append('[语音]')
        elif btype == '11':
            t = block.get('text', '')
            if t:
                parts.append(t)
            else:
                uid  = str(block.get('uid', ''))
                name = ''
                if uid and uid in user_map:
                    u    = user_map[uid]
                    name = (u.get('name_show', '') or u.get('nick_name', '')
                            or u.get('name', '') or u.get('user_name', ''))
                if not name:
                    name = (block.get('name_show', '') or block.get('name', '')
                            or block.get('nick_name', ''))
                    if name and uid and uid not in user_map:
                        user_map[uid] = {"user_id": uid, "user_name": name,
                                         "nick_name": name, "name_show": name}
                parts.append(f'@{name}' if name else (f'@用户{uid}' if uid else ''))
        elif btype == '18':
            t = block.get('text', '')
            if t: parts.append(t)
        elif btype == '20':
            parts.append('[短视频]')
        else:
            t = block.get('text', '')
            if t: parts.append(t)

    content = '\n'.join(p for p in parts if p).strip()
    content = re.sub(r'(回复\s*)\n(.+?)\n(\s*:)', r'\1(\2)\3', content)
    return content, images


# ============================================================
# 核心爬虫类（每线程独立实例，独立 session）
# ============================================================
class TiebaPostScraper:

    def __init__(self, tid: str, max_pages: int = 50, fetch_sub_comments: bool = True):
        self.tid                = tid
        self.max_pages          = max_pages
        self.fetch_sub_comments = fetch_sub_comments
        self.session            = requests.Session()
        self._user_map: Dict    = {}

        self.result = {
            "post_id":        tid,
            "post_url":       f"https://tieba.baidu.com/p/{tid}",
            "scrape_time":    time.strftime("%Y-%m-%d %H:%M:%S"),
            "title":          "",
            "forum_name":     "",
            "author":         {},
            "content":        "",
            "content_images": [],
            "create_time":    "",
            "total_replies":  0,
            "total_pages":    0,
            "share_count":    0,
            "comments":       [],
        }

    # ---------- HTTP helpers ----------
    def _post_req(self, url, data=None, headers=None, timeout=15):
        for attempt in range(3):
            try:
                r = self.session.post(url, data=data, headers=headers, timeout=timeout)
                r.raise_for_status()
                return r
            except requests.RequestException:
                if attempt < 2:
                    time.sleep(REQUEST_DELAY * (attempt + 1))
        return None

    def _get_req(self, url, params=None, headers=None, timeout=15):
        for attempt in range(3):
            try:
                r = self.session.get(url, params=params, headers=headers, timeout=timeout)
                r.raise_for_status()
                return r
            except requests.RequestException:
                if attempt < 2:
                    time.sleep(REQUEST_DELAY * (attempt + 1))
        return None

    def _build_params(self, extra: dict) -> dict:
        p = dict(CLIENT_COMMON_PARAMS)
        p.update(extra)
        p['sign'] = calc_sign(p)
        return p

    # ---------- Client API ----------
    def _scrape_via_client_api(self) -> bool:
        page_num = 1
        while page_num <= self.max_pages:
            params = self._build_params({
                "kz": self.tid, "pn": str(page_num),
                "rn": "30", "lz": "0",
                "r":  str(int(time.time())),
            })
            resp = self._post_req(CLIENT_API_URL, data=params, headers=CLIENT_HEADERS)
            if not resp:
                return False if page_num == 1 else True

            try:
                data = resp.json()
            except json.JSONDecodeError:
                return False if page_num == 1 else True

            ec = data.get('error_code', '') or data.get('error', {}).get('errno', '')
            if str(ec) not in ('0', ''):
                return False if page_num == 1 else True

            if page_num == 1:
                self._parse_meta(data)

            post_list = (data.get('post_list') or
                         data.get('data', {}).get('post_list', []))
            if not post_list:
                return False if page_num == 1 else True

            self.result['comments'].extend(self._parse_posts(post_list, data))

            page_info  = data.get('page', {}) or data.get('data', {}).get('page', {})
            total_page = safe_int(page_info.get('total_page', 0))
            if total_page > 0:
                self.result['total_pages'] = total_page

            if (page_num >= self.result['total_pages'] > 0
                    or str(page_info.get('has_more', '1')) == '0'):
                break

            page_num += 1
            time.sleep(REQUEST_DELAY)

        return bool(self.result['comments']) or bool(self.result.get('content'))

    def _parse_meta(self, data: dict):
        thread    = data.get('thread', {})    or data.get('data', {}).get('thread', {})
        forum     = data.get('forum', {})     or data.get('data', {}).get('forum', {})
        page_info = data.get('page', {})      or data.get('data', {}).get('page', {})

        self.result['title']         = thread.get('title', '')
        self.result['forum_name']    = forum.get('name', '')
        self.result['total_replies'] = safe_int(thread.get('reply_num', 0))
        self.result['share_count']   = safe_int(thread.get('share_num', 0))
        self.result['total_pages']   = safe_int(page_info.get('total_page', 0))

        author = thread.get('author', {})
        if isinstance(author, dict) and author.get('id'):
            self.result['author'] = {
                "user_id":   str(author.get('id', '')),
                "user_name": author.get('name', ''),
                "nick_name": author.get('name_show', '') or author.get('name', ''),
                "level_id":  safe_int(author.get('level_id', 0)),
                "is_author": True,
            }

    def _parse_posts(self, post_list: list, full_data: dict) -> list:
        thread           = (full_data.get('thread', {})
                            or full_data.get('data', {}).get('thread', {}))
        thread_author_id = str(thread.get('author', {}).get('id', ''))
        user_map         = self._user_map

        for u in (full_data.get('user_list')
                  or full_data.get('data', {}).get('user_list', [])):
            if isinstance(u, dict):
                uid = str(u.get('id', ''))
                if uid:
                    user_map[uid] = {
                        "user_id":    uid,
                        "user_name":  u.get('name', ''),
                        "nick_name":  u.get('name_show', '') or u.get('name', ''),
                        "level_id":   safe_int(u.get('level_id', 0)),
                        "portrait":   u.get('portrait', ''),
                        "ip_address": u.get('ip_address', ''),
                    }

        t_author = thread.get('author', {})
        if isinstance(t_author, dict) and t_author.get('id'):
            tid2 = str(t_author['id'])
            if tid2 not in user_map:
                user_map[tid2] = {
                    "user_id":   tid2,
                    "user_name": t_author.get('name', ''),
                    "nick_name": t_author.get('name_show', '') or t_author.get('name', ''),
                    "level_id":  safe_int(t_author.get('level_id', 0)),
                }

        comments = []
        for post in post_list:
            floor_num = safe_int(post.get('floor', 0))
            post_id   = str(post.get('id', ''))
            uid       = str(post.get('author_id', ''))

            if uid and uid in user_map:
                ui        = user_map[uid]
                user_name = ui.get('user_name', '')
                nick_name = ui.get('nick_name', '') or user_name
                level_id  = ui.get('level_id', 0)
            else:
                ex = _extract_user_from_author_dict(post.get('author', {}))
                if ex and ex.get('user_id'):
                    uid       = uid or ex['user_id']
                    user_name = ex.get('user_name', '')
                    nick_name = ex.get('nick_name', '') or user_name
                    level_id  = ex.get('level_id', 0)
                    user_map[uid] = ex
                else:
                    user_name = nick_name = ''
                    level_id  = 0

            user = {
                "user_id":   uid,
                "user_name": user_name,
                "nick_name": nick_name,
                "level_id":  level_id,
                "is_author": uid == thread_author_id and uid != '',
            }

            text, images = parse_content_blocks(post.get('content', []), user_map)
            post_time    = format_timestamp(post.get('time', 0))
            agree        = post.get('agree', {})
            like_count   = (safe_int(agree.get('agree_num', 0))
                            if isinstance(agree, dict) else safe_int(agree))
            sub_count    = safe_int(post.get('sub_post_number', 0))

            if floor_num == 1:
                self.result.update({
                    'content':        text,
                    'content_images': images,
                    'create_time':    post_time,
                    'like_count':     like_count,
                    'author':         user,
                })
                continue

            comment = {
                "floor":             floor_num,
                "post_id":           post_id,
                "user":              user,
                "content":           text,
                "images":            images,
                "post_time":         post_time,
                "like_count":        like_count,
                "sub_comment_count": sub_count,
                "sub_comments":      [],
            }

            spl = post.get('sub_post_list', {})
            if isinstance(spl, dict):
                sub_posts = spl.get('sub_post_list', [])
                if sub_posts:
                    for sp in sub_posts:
                        _resolve_sub_comment_user(sp, user_map)
                        _resolve_reply_to(sp, user_map)
                    for sp in sub_posts:
                        sp_uid, sp_name = _resolve_sub_comment_user(sp, user_map)
                        reply_to        = _resolve_reply_to(sp, user_map)
                        sp_text, sp_img = parse_content_blocks(sp.get('content', []), user_map)
                        if reply_to:
                            sp_text = _fix_content_reply_to(sp_text, reply_to)
                        sc = {
                            "user_name": sp_name,
                            "user_id":   sp_uid,
                            "content":   sp_text,
                            "time":      format_timestamp(sp.get('time', 0)),
                        }
                        if reply_to and reply_to.get('name'):
                            sc['reply_to'] = reply_to
                        if sp_img:
                            sc['images'] = sp_img
                        comment['sub_comments'].append(sc)

            comments.append(comment)
        return comments

    # ---------- Floor sub-comments ----------
    def _fetch_sub_comments(self, pid: str, total: int) -> list:
        sub_comments = []
        user_map     = self._user_map
        max_pages    = (total // 10) + 2

        for page in range(1, max_pages + 1):
            params = self._build_params({
                "kz": self.tid, "pid": pid,
                "pn": str(page), "rn": "20",
            })
            resp = self._post_req(CLIENT_FLOOR_API_URL, data=params, headers=CLIENT_HEADERS)
            if not resp:
                break
            try:
                data = resp.json()
            except json.JSONDecodeError:
                break

            ec = data.get('error_code', '')
            if str(ec) not in ('0', ''):
                break

            for u in (data.get('user_list') or data.get('data', {}).get('user_list', [])):
                if isinstance(u, dict):
                    uid = str(u.get('id', ''))
                    if uid:
                        existing = user_map.get(uid)
                        entry    = {
                            "user_id":   uid,
                            "user_name": u.get('name', ''),
                            "name_show": u.get('name_show', '') or u.get('name', ''),
                            "nick_name": u.get('name_show', '') or u.get('name', ''),
                            "level_id":  safe_int(u.get('level_id', 0)),
                        }
                        if not existing:
                            user_map[uid] = entry
                        elif not existing.get('nick_name'):
                            existing.update({k: v for k, v in entry.items() if v})

            subpost_list = (data.get('subpost_list')
                            or data.get('data', {}).get('subpost_list', []))
            if not subpost_list:
                break

            for sp in subpost_list:
                _resolve_sub_comment_user(sp, user_map)
                _resolve_reply_to(sp, user_map)
            for sp in subpost_list:
                sp_uid, sp_name = _resolve_sub_comment_user(sp, user_map)
                reply_to        = _resolve_reply_to(sp, user_map)
                sp_text, sp_img = parse_content_blocks(sp.get('content', []), user_map)
                if reply_to:
                    sp_text = _fix_content_reply_to(sp_text, reply_to)
                sc = {
                    "user_name": sp_name,
                    "user_id":   sp_uid,
                    "content":   sp_text,
                    "time":      format_timestamp(sp.get('time', 0)),
                }
                if reply_to and reply_to.get('name'):
                    sc['reply_to'] = reply_to
                if sp_img:
                    sc['images'] = sp_img
                sub_comments.append(sc)

            time.sleep(REQUEST_DELAY * 0.5)

        return sub_comments

    # ---------- Mobile API (fallback) ----------
    def _scrape_via_mobile_api(self) -> bool:
        page_num = 1
        while page_num <= self.max_pages:
            resp = self._get_req(MOBILE_API_URL, params={
                "kz": self.tid, "pn": str(page_num),
                "rn": "30", "format": "json",
            }, headers=MOBILE_HEADERS)

            if not resp:
                return False if page_num == 1 else True
            try:
                data = resp.json()
            except json.JSONDecodeError:
                return False if page_num == 1 else True

            if data.get('errno', -1) != 0:
                return False if page_num == 1 else True

            api = data.get('data', {})
            if page_num == 1:
                self.result['title']         = api.get('thread', {}).get('title', '')
                self.result['forum_name']    = api.get('forum', {}).get('name', '')
                self.result['total_replies'] = safe_int(api.get('thread', {}).get('reply_num', 0))
                self.result['share_count']   = safe_int(api.get('thread', {}).get('share_num', 0))

            posts = api.get('post_list', [])
            if not posts:
                return False if page_num == 1 else True

            t_author_id = str(api.get('thread', {}).get('author', {}).get('id', ''))

            for post in posts:
                author    = post.get('author', {})
                floor_num = safe_int(post.get('floor', 0))
                raw_c     = post.get('content', '')
                if isinstance(raw_c, list):
                    text, images = parse_content_blocks(raw_c)
                elif isinstance(raw_c, str):
                    text, images = clean_text(raw_c), []
                else:
                    text, images = str(raw_c) if raw_c else '', []

                comment = {
                    "floor":             floor_num,
                    "post_id":           str(post.get('id', '')),
                    "user": {
                        "user_id":   str(author.get('id', '')),
                        "user_name": author.get('name', ''),
                        "nick_name": author.get('name_show', '') or author.get('name', ''),
                        "level_id":  safe_int(author.get('level_id', 0)),
                        "is_author": str(author.get('id', '')) == t_author_id,
                    },
                    "content":           text,
                    "images":            images,
                    "post_time":         format_timestamp(post.get('time', '')),
                    "like_count":        safe_int(
                        post.get('agree', {}).get('agree_num', 0)
                        if isinstance(post.get('agree'), dict)
                        else post.get('agree', 0)
                    ),
                    "sub_comment_count": safe_int(post.get('sub_post_number', 0)),
                    "sub_comments":      [],
                }

                if floor_num == 1:
                    self.result.update({
                        'content':        text,
                        'content_images': images,
                        'create_time':    comment['post_time'],
                        'author':         comment['user'],
                    })
                    continue
                self.result['comments'].append(comment)

            total_page = safe_int(api.get('page', {}).get('total_page', 1))
            self.result['total_pages'] = total_page
            if page_num >= total_page:
                break
            page_num += 1
            time.sleep(REQUEST_DELAY)

        return bool(self.result['comments'])

    # ---------- Public entry ----------
    def scrape(self) -> dict:
        ok = self._scrape_via_client_api()
        if not ok:
            ok = self._scrape_via_mobile_api()

        if ok and self.fetch_sub_comments:
            for c in self.result['comments']:
                pid   = c.get('post_id', '')
                count = c.get('sub_comment_count', 0)
                if pid and count > 0 and len(c.get('sub_comments', [])) < count:
                    sub = self._fetch_sub_comments(pid, count)
                    if sub:
                        c['sub_comments'] = sub
                    time.sleep(REQUEST_DELAY * 0.5)

        return self.result


# ============================================================
# JSONL 输出管理器
# 核心职责：
#   1. 每个输出文件一把独立锁，不同文件并发写互不阻塞
#   2. 启动时预加载已有文件中的 post_id，支持断点续传
#   3. 写入后立即 flush，防止进程异常丢数据
# ============================================================
class JsonlOutputManager:

    def __init__(self, output_base: Path):
        self.output_base  = output_base
        self._meta_lock   = threading.Lock()          # 保护字典本身
        self._file_locks: Dict[Path, threading.Lock] = {}
        self._done_ids:   Dict[Path, set]             = {}

    def _ensure(self, out_path: Path) -> threading.Lock:
        """懒初始化：首次访问时创建锁并加载已有 post_id。"""
        with self._meta_lock:
            if out_path not in self._file_locks:
                self._file_locks[out_path] = threading.Lock()
                self._done_ids[out_path]   = self._load_existing_ids(out_path)
            return self._file_locks[out_path]

    @staticmethod
    def _load_existing_ids(path: Path) -> set:
        ids = set()
        if not path.exists():
            return ids
        with open(path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    pid = str(json.loads(line).get('post_id', ''))
                    if pid:
                        ids.add(pid)
                except json.JSONDecodeError:
                    pass
        return ids

    def is_done(self, out_path: Path, thread_id: str) -> bool:
        lock = self._ensure(out_path)
        with lock:
            return thread_id in self._done_ids[out_path]

    def append(self, out_path: Path, result: dict) -> bool:
        """
        线程安全地将爬取结果追加写入目标 JSONL。
        返回 True 表示实际写入，False 表示并发重复跳过。
        """
        tid  = str(result.get('post_id', ''))
        lock = self._ensure(out_path)
        with lock:
            if tid in self._done_ids[out_path]:
                return False
            out_path.parent.mkdir(parents=True, exist_ok=True)
            with open(out_path, 'a', encoding='utf-8') as f:
                f.write(json.dumps(result, ensure_ascii=False) + '\n')
                f.flush()
            self._done_ids[out_path].add(tid)
            return True


# ============================================================
# 任务扫描
# ============================================================
def scan_tasks(input_base: Path, output_base: Path,
               manager: JsonlOutputManager) -> List[Dict]:
    """
    扫描输入目录，构建任务列表。
    - 每个任务: {"thread_id": str, "out_path": Path}
    - 跨文件全局去重（同一 thread_id 只爬一次）
    - 已完成（断点续传）的任务直接跳过，不加入列表
    """
    tasks    = []
    seen_ids = set()
    skip_cnt = 0

    for game_dir in GAME_DIRS:
        for sentiment in SENTIMENT_DIRS:
            dir_path = input_base / game_dir / sentiment
            if not dir_path.exists():
                continue

            for filename in TARGET_FILES:
                jsonl_path = dir_path / filename
                if not jsonl_path.exists():
                    continue

                out_path = output_base / game_dir / sentiment / filename

                with open(jsonl_path, 'r', encoding='utf-8') as f:
                    for lineno, line in enumerate(f, 1):
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            record = json.loads(line)
                        except json.JSONDecodeError:
                            logger.warning(f"JSON解析失败: {jsonl_path}:{lineno}")
                            continue

                        tid = str(record.get('thread_id', '')).strip()
                        if not tid or not tid.isdigit():
                            logger.warning(f"无效 thread_id '{tid}': {jsonl_path}:{lineno}")
                            continue

                        if tid in seen_ids:
                            logger.debug(f"跳过重复 thread_id: {tid}")
                            continue
                        seen_ids.add(tid)

                        if manager.is_done(out_path, tid):
                            skip_cnt += 1
                            continue

                        tasks.append({"thread_id": tid, "out_path": out_path})

    logger.info(f"扫描完成 | 待爬取: {len(tasks)} | 已跳过(断点续传): {skip_cnt}")
    return tasks


# ============================================================
# 单帖工作函数（在线程池中执行）
# ============================================================
def scrape_one(task: Dict, manager: JsonlOutputManager, max_pages: int) -> Dict:
    tid      = task["thread_id"]
    out_path = task["out_path"]

    scraper = TiebaPostScraper(tid=tid, max_pages=max_pages, fetch_sub_comments=True)
    try:
        result  = scraper.scrape()
        written = manager.append(out_path, result)
        return {
            "tid":      tid,
            "status":   "success" if written else "duplicate",
            "title":    result.get("title", ""),
            "comments": len(result.get("comments", [])),
            "out_path": out_path,
        }
    except Exception as e:
        return {
            "tid":      tid,
            "status":   "error",
            "error":    str(e),
            "out_path": out_path,
        }


# ============================================================
# 批量主逻辑
# ============================================================
def run_batch(
    input_base:  Path,
    output_base: Path,
    max_workers: int = 5,
    max_pages:   int = 50,
):
    manager = JsonlOutputManager(output_base)
    tasks   = scan_tasks(input_base, output_base, manager)

    if not tasks:
        logger.warning("没有待爬取的任务（可能已全部完成或输入目录为空）")
        return

    total        = len(tasks)
    done_cnt     = 0
    success_cnt  = 0
    fail_cnt     = 0
    failed_list  = []
    counter_lock = threading.Lock()
    start_time   = time.time()

    logger.info(
        f"启动批量爬取 | 待爬: {total} 帖 | "
        f"并发线程: {max_workers} | 最大页数/帖: {max_pages}"
    )
    logger.info(f"输入: {input_base.resolve()}")
    logger.info(f"输出: {output_base.resolve()}")
    logger.info("=" * 65)

    with ThreadPoolExecutor(max_workers=max_workers,
                            thread_name_prefix="TiebaWorker") as executor:

        future_map = {
            executor.submit(scrape_one, task, manager, max_pages): task
            for task in tasks
        }

        for future in as_completed(future_map):
            task = future_map[future]
            tid  = task["thread_id"]

            try:
                res = future.result()
            except Exception as e:
                res = {"tid": tid, "status": "error", "error": str(e),
                       "out_path": task["out_path"]}

            with counter_lock:
                done_cnt += 1

                if res["status"] == "success":
                    success_cnt += 1
                    logger.info(
                        f"[{done_cnt}/{total}] ✅ {tid} | "
                        f"{res.get('title', '')[:28]} | "
                        f"{res.get('comments', 0)} 楼 "
                        f"→ .../{res['out_path'].parent.name}/{res['out_path'].name}"
                    )
                elif res["status"] == "duplicate":
                    logger.debug(f"[{done_cnt}/{total}] ⏭  {tid} 并发重复，跳过")
                else:
                    fail_cnt += 1
                    logger.warning(
                        f"[{done_cnt}/{total}] ❌ {tid} 失败: {res.get('error', 'unknown')}"
                    )
                    failed_list.append({
                        "thread_id": tid,
                        "out_path":  str(res.get("out_path", "")),
                        "error":     res.get("error", ""),
                    })

                if done_cnt % 50 == 0:
                    elapsed = time.time() - start_time
                    speed   = done_cnt / elapsed if elapsed > 0 else 0
                    eta     = (total - done_cnt) / speed if speed > 0 else 0
                    logger.info(
                        f"\n--- 进度 [{done_cnt}/{total}] "
                        f"成功:{success_cnt} 失败:{fail_cnt} "
                        f"速度:{speed:.2f} 帖/s "
                        f"剩余约:{eta/60:.1f} 分钟 ---\n"
                    )

    elapsed = time.time() - start_time
    logger.info("\n" + "=" * 65)
    logger.info("批量爬取完成!")
    logger.info(f"  总计: {total} | 成功: {success_cnt} | 失败: {fail_cnt}")
    logger.info(f"  总耗时: {elapsed/60:.1f} 分钟")
    logger.info("=" * 65)

    if failed_list:
        fail_log = output_base / "failed_tasks.jsonl"
        with open(fail_log, 'w', encoding='utf-8') as f:
            for item in failed_list:
                f.write(json.dumps(item, ensure_ascii=False) + '\n')
        logger.info(f"失败任务已记录至: {fail_log}")


# ============================================================
# 命令行入口
# ============================================================
def parse_args():
    parser = argparse.ArgumentParser(
        description="百度贴吧批量帖子爬虫（多线程 · JSONL 合并输出版）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
输出格式说明:
  每个输出 JSONL 与输入同名，同一文件下的所有帖子各占一行（完整 JSON 对象）。
  结构示例:
    {"post_id":"xxx","title":"...","comments":[...],...}   ← 帖子A
    {"post_id":"yyy","title":"...","comments":[...],...}   ← 帖子B

线程数建议（Mac M4）:
  测试: --workers 3   正常: --workers 5（默认）   激进: --workers 8

示例:
  python tieba_batch_crawler.py
  python tieba_batch_crawler.py --workers 6 --max-pages 30
  python tieba_batch_crawler.py \\
    --input-dir  /data/category_classify_output_split_20260326 \\
    --output-dir /data/category_classify_output_split_post_20260326
        """
    )
    parser.add_argument("--workers",    type=int, default=5,
                        help="并发线程数（默认: 5，建议 3-8）")
    parser.add_argument("--max-pages",  type=int, default=50,
                        help="每帖最大爬取页数（默认: 50）")
    parser.add_argument("--input-dir",  type=str, default=INPUT_BASE_DIR,
                        help=f"输入根目录（默认: {INPUT_BASE_DIR}）")
    parser.add_argument("--output-dir", type=str, default=OUTPUT_BASE_DIR,
                        help=f"输出根目录（默认: {OUTPUT_BASE_DIR}）")
    return parser.parse_args()


def main():
    args        = parse_args()
    input_base  = Path(args.input_dir)
    output_base = Path(args.output_dir)

    if not input_base.exists():
        logger.error(f"输入目录不存在: {input_base.resolve()}")
        sys.exit(1)

    output_base.mkdir(parents=True, exist_ok=True)
    run_batch(
        input_base  = input_base,
        output_base = output_base,
        max_workers = args.workers,
        max_pages   = args.max_pages,
    )


if __name__ == "__main__":
    main()