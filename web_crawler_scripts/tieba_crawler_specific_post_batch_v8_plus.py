#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
百度贴吧批量帖子爬虫 (Batch Baidu Tieba Post Scraper)
======================================================
功能：
  1. 扫描 category_classify_output_split_20260326 目录下指定的 JSONL 文件
  2. 提取所有 thread_id 进行批量爬取
  3. 多线程并行爬取，降低总耗时
  4. 将结果按原文件夹结构存储到 category_classify_output_split_post_20260326

只处理以下4种JSONL文件:
  - problem_solving_posts.jsonl
  - review_or_suggestion_posts.jsonl
  - player_conflict_posts.jsonl
  - update_prediction_posts.jsonl

用法:
  python tieba_batch_crawler.py [--workers N] [--max-pages N] [--input-dir DIR] [--output-dir DIR]

示例:
  python tieba_batch_crawler.py --workers 6 --max-pages 50
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
from typing import Optional, List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue

import requests

# ============================================================
# 日志配置 (线程安全)
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# 进度锁
_progress_lock = threading.Lock()
_progress_counter = {"done": 0, "total": 0, "success": 0, "fail": 0}

# ============================================================
# 目录与文件常量
# ============================================================
INPUT_BASE_DIR = "category_classify_output_split_20260326"
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
CLIENT_API_URL = "https://tiebac.baidu.com/c/f/pb/page"
CLIENT_FLOOR_API_URL = "https://tiebac.baidu.com/c/f/pb/floor"
MOBILE_API_URL = "https://tieba.baidu.com/mg/p/getPbData"

SIGN_KEY = "tiebaclient!!!"

CLIENT_COMMON_PARAMS = {
    "_client_type": "2",
    "_client_version": "12.57.1.0",
    "_os_version": "33",
    "_phone_imei": "000000000000000",
    "from": "tieba",
    "cuid": "baidutiebaapp",
}

CLIENT_HEADERS = {
    "User-Agent": "bdtb for Android 12.57.1.0",
    "Content-Type": "application/x-www-form-urlencoded",
    "Accept": "*/*",
    "Accept-Encoding": "gzip",
    "Connection": "keep-alive",
    "Host": "tiebac.baidu.com",
}

MOBILE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/17.0 Mobile/15E148 Safari/604.1"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Referer": "https://tieba.baidu.com/",
}

REQUEST_DELAY = 0.8  # 多线程下适当降低间隔


# ============================================================
# 工具函数（与原脚本一致）
# ============================================================
def extract_tid(url_or_id: str) -> str:
    url_or_id = url_or_id.strip()
    if url_or_id.isdigit():
        return url_or_id
    match = re.search(r'/p/(\d+)', url_or_id)
    if match:
        return match.group(1)
    raise ValueError(f"无法从输入中提取帖子ID: {url_or_id}")


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def safe_int(val, default=0):
    if val is None:
        return default
    try:
        val_str = str(val).strip()
        if '万' in val_str:
            return int(float(val_str.replace('万', '')) * 10000)
        if '亿' in val_str:
            return int(float(val_str.replace('亿', '')) * 100000000)
        return int(val_str)
    except (ValueError, TypeError):
        return default


def format_timestamp(ts) -> str:
    if not ts:
        return ""
    try:
        ts_val = int(ts)
        if ts_val > 1e12:
            ts_val = ts_val // 1000
        return datetime.fromtimestamp(ts_val).strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError, OSError):
        return str(ts)


def calc_sign(params: dict) -> str:
    sorted_params = sorted(params.items())
    sign_str = ''.join(f"{k}={v}" for k, v in sorted_params)
    sign_str += SIGN_KEY
    return hashlib.md5(sign_str.encode('utf-8')).hexdigest()


def _extract_user_from_author_dict(author_dict: dict) -> dict:
    if not isinstance(author_dict, dict):
        return {}
    uid = str(author_dict.get('id', ''))
    if not uid:
        return {}
    return {
        "user_id": uid,
        "user_name": author_dict.get('name', ''),
        "nick_name": author_dict.get('name_show', '') or author_dict.get('name', '') or author_dict.get('nick_name', ''),
        "level_id": safe_int(author_dict.get('level_id', 0)),
        "portrait": author_dict.get('portrait', ''),
        "ip_address": author_dict.get('ip_address', ''),
    }


def _resolve_sub_comment_user(sp: dict, user_map: dict) -> tuple:
    sp_uid = str(sp.get('author_id', ''))
    sp_author = sp.get('author', {})
    if not sp_uid and isinstance(sp_author, dict):
        sp_uid = str(sp_author.get('id', ''))

    if sp_uid and sp_uid in user_map:
        sp_name = (user_map[sp_uid].get('nick_name', '')
                   or user_map[sp_uid].get('name_show', '')
                   or user_map[sp_uid].get('user_name', ''))
        if sp_name:
            return sp_uid, sp_name

    if isinstance(sp_author, dict) and sp_author:
        extracted = _extract_user_from_author_dict(sp_author)
        if extracted:
            if extracted['user_id'] not in user_map:
                user_map[extracted['user_id']] = extracted
            elif not user_map[extracted['user_id']].get('nick_name'):
                user_map[extracted['user_id']].update(
                    {k: v for k, v in extracted.items() if v}
                )
            sp_name = extracted.get('nick_name', '') or extracted.get('user_name', '')
            if sp_name:
                return sp_uid or extracted['user_id'], sp_name

    return sp_uid, ''


def _resolve_reply_to(sp: dict, user_map: dict) -> dict:
    if not isinstance(sp, dict):
        return {}

    reply_uid = ''
    reply_name = ''

    content_blocks = sp.get('content', [])
    if isinstance(content_blocks, list):
        for block in content_blocks:
            if not isinstance(block, dict):
                continue
            btype = str(block.get('type', ''))
            if btype == '4':
                block_uid = str(block.get('uid', ''))
                if block_uid and block_uid != '0':
                    reply_uid = block_uid
                    reply_name = block.get('text', '')
                    if reply_uid in user_map:
                        u = user_map[reply_uid]
                        reply_name = (u.get('nick_name', '')
                                      or u.get('name_show', '')
                                      or u.get('user_name', '')
                                      or reply_name)
                    break
            elif btype == '11':
                block_uid = str(block.get('uid', ''))
                if block_uid and block_uid != '0':
                    reply_uid = block_uid
                    if not reply_name:
                        reply_name = (block.get('name_show', '')
                                      or block.get('name', '')
                                      or block.get('text', ''))
                    break

    if not reply_name:
        title = sp.get('title', '')
        if isinstance(title, str) and title.strip():
            reply_name = title.strip()

    if not reply_uid and not reply_name:
        for key in ('reply_to_id', 'reply_uid', 'reply_to_user_id'):
            val = sp.get(key)
            if val and str(val) != '0':
                reply_uid = str(val)
                break
        for key in ('reply_to_user', 'replyUser', 'reply_user'):
            obj = sp.get(key, {})
            if isinstance(obj, dict) and obj:
                if not reply_uid:
                    reply_uid = str(obj.get('id', ''))
                if not reply_name:
                    reply_name = (obj.get('name_show', '')
                                  or obj.get('name', ''))
                break

    if reply_name and not reply_uid:
        for uid, u in user_map.items():
            if (u.get('nick_name') == reply_name
                    or u.get('name_show') == reply_name
                    or u.get('user_name') == reply_name):
                reply_uid = uid
                break

    if reply_uid and not reply_name and reply_uid in user_map:
        u = user_map[reply_uid]
        reply_name = (u.get('nick_name', '')
                      or u.get('name_show', '')
                      or u.get('user_name', ''))

    if reply_uid or reply_name:
        return {"uid": reply_uid, "name": reply_name}
    return {}


def _fix_content_reply_to(content: str, reply_to: dict) -> str:
    if not reply_to or not reply_to.get('name'):
        return content
    name = reply_to['name']
    pattern = r'回复\s*(?:@用户\d+)?\s*\n?\s*:'
    replacement = f'回复 @{name} :'
    fixed = re.sub(pattern, replacement, content, count=1)
    return fixed


def parse_content_blocks(content_blocks, user_map: dict = None) -> tuple:
    text_parts = []
    images = []

    if not content_blocks:
        return "", []
    if isinstance(content_blocks, str):
        return content_blocks.strip(), []
    if not isinstance(content_blocks, list):
        return str(content_blocks).strip(), []
    if user_map is None:
        user_map = {}

    for block in content_blocks:
        if not isinstance(block, dict):
            text_parts.append(str(block))
            continue

        block_type = str(block.get('type', ''))

        if block_type == '0':
            text = block.get('text', '')
            if text:
                text_parts.append(text)
        elif block_type == '1':
            link = block.get('link', '') or block.get('text', '')
            if link:
                text_parts.append(link)
        elif block_type == '2':
            c = block.get('c', '') or block.get('text', '')
            if c:
                text_parts.append(c)
        elif block_type == '3':
            img_url = (block.get('origin_src', '')
                       or block.get('big_cdn_src', '')
                       or block.get('cdn_src', '')
                       or block.get('src', ''))
            if img_url:
                if img_url.startswith('//'):
                    img_url = 'https:' + img_url
                images.append(img_url)
        elif block_type == '4':
            uid = str(block.get('uid', ''))
            if uid and uid != '0':
                name = block.get('text', '')
                if uid in user_map:
                    u = user_map[uid]
                    name = (u.get('nick_name', '')
                            or u.get('name_show', '')
                            or u.get('user_name', '')
                            or name)
                if name:
                    text_parts.append(name)
                elif uid:
                    text_parts.append(f'@用户{uid}')
        elif block_type == '5':
            video_url = block.get('link', '') or block.get('text', '')
            if video_url:
                text_parts.append(f'[视频: {video_url}]')
        elif block_type == '9':
            text = block.get('text', '')
            if text:
                text_parts.append(text)
        elif block_type == '10':
            text_parts.append('[语音]')
        elif block_type == '11':
            text = block.get('text', '')
            if text:
                text_parts.append(text)
            else:
                uid = str(block.get('uid', ''))
                resolved_name = ''
                if uid and uid in user_map:
                    u = user_map[uid]
                    resolved_name = (u.get('name_show', '')
                                     or u.get('nick_name', '')
                                     or u.get('name', '')
                                     or u.get('user_name', ''))
                if not resolved_name:
                    resolved_name = (block.get('name_show', '')
                                     or block.get('name', '')
                                     or block.get('nick_name', ''))
                    if resolved_name and uid and uid not in user_map:
                        user_map[uid] = {
                            "user_id": uid,
                            "user_name": resolved_name,
                            "nick_name": resolved_name,
                            "name_show": resolved_name,
                        }
                if resolved_name:
                    text_parts.append(f'@{resolved_name}')
                elif uid:
                    text_parts.append(f'@用户{uid}')
        elif block_type == '18':
            text = block.get('text', '')
            if text:
                text_parts.append(text)
        elif block_type == '20':
            text_parts.append('[短视频]')
        else:
            text = block.get('text', '')
            if text:
                text_parts.append(text)

    content = '\n'.join(text_parts).strip()
    content = re.sub(r'(回复\s*)\n(.+?)\n(\s*:)', r'\1(\2)\3', content)
    return content, images


# ============================================================
# 核心爬虫类（与原脚本一致，独立 session）
# ============================================================
class TiebaPostScraper:

    def __init__(self, tid: str, max_pages: int = 50, fetch_sub_comments: bool = True):
        self.tid = tid
        self.max_pages = max_pages
        self.fetch_sub_comments = fetch_sub_comments
        self.session = requests.Session()
        self._user_map = {}

        self.result = {
            "post_id": tid,
            "post_url": f"https://tieba.baidu.com/p/{tid}",
            "scrape_time": time.strftime("%Y-%m-%d %H:%M:%S"),
            "title": "",
            "forum_name": "",
            "author": {},
            "content": "",
            "content_images": [],
            "create_time": "",
            "total_replies": 0,
            "total_pages": 0,
            "share_count": 0,
            "comments": [],
        }

    def _get(self, url, params=None, headers=None, timeout=15):
        for attempt in range(3):
            try:
                resp = self.session.get(url, params=params, headers=headers, timeout=timeout)
                resp.raise_for_status()
                return resp
            except requests.RequestException as e:
                if attempt < 2:
                    time.sleep(REQUEST_DELAY * (attempt + 1))
        return None

    def _post(self, url, data=None, headers=None, timeout=15):
        for attempt in range(3):
            try:
                resp = self.session.post(url, data=data, headers=headers, timeout=timeout)
                resp.raise_for_status()
                return resp
            except requests.RequestException as e:
                if attempt < 2:
                    time.sleep(REQUEST_DELAY * (attempt + 1))
        return None

    def _build_client_params(self, extra_params: dict) -> dict:
        params = dict(CLIENT_COMMON_PARAMS)
        params.update(extra_params)
        params['sign'] = calc_sign(params)
        return params

    def _scrape_via_client_api(self) -> bool:
        page_num = 1
        while page_num <= self.max_pages:
            params = self._build_client_params({
                "kz": self.tid,
                "pn": str(page_num),
                "rn": "30",
                "lz": "0",
                "r": str(int(time.time())),
            })

            resp = self._post(CLIENT_API_URL, data=params, headers=CLIENT_HEADERS)
            if not resp:
                if page_num == 1:
                    return False
                break

            try:
                data = resp.json()
            except json.JSONDecodeError:
                if page_num == 1:
                    return False
                break

            error_code = data.get('error_code', '') or data.get('error', {}).get('errno', '')
            if str(error_code) != '0' and str(error_code) != '':
                if page_num == 1:
                    return False
                break

            if page_num == 1:
                self._parse_client_meta(data)

            post_list = data.get('post_list', [])
            if not post_list:
                post_list = data.get('data', {}).get('post_list', [])

            if not post_list:
                if page_num == 1:
                    return False
                break

            page_comments = self._parse_client_posts(post_list, data)
            self.result['comments'].extend(page_comments)

            page_info = data.get('page', {}) or data.get('data', {}).get('page', {})
            total_pages = safe_int(page_info.get('total_page', 0))
            if total_pages > 0:
                self.result['total_pages'] = total_pages
            if page_num >= self.result.get('total_pages', 1) and self.result['total_pages'] > 0:
                break

            has_more = page_info.get('has_more', '1')
            if str(has_more) == '0':
                break

            page_num += 1
            time.sleep(REQUEST_DELAY)

        return len(self.result['comments']) > 0 or bool(self.result.get('content'))

    def _parse_client_meta(self, data: dict):
        thread = data.get('thread', {}) or data.get('data', {}).get('thread', {})
        forum = data.get('forum', {}) or data.get('data', {}).get('forum', {})
        page_info = data.get('page', {}) or data.get('data', {}).get('page', {})

        self.result['title'] = thread.get('title', '') or ''
        self.result['forum_name'] = forum.get('name', '') or ''
        self.result['total_replies'] = safe_int(thread.get('reply_num', 0))
        self.result['share_count'] = safe_int(thread.get('share_num', 0))
        self.result['total_pages'] = safe_int(page_info.get('total_page', 0))

        author = thread.get('author', {})
        if isinstance(author, dict) and author.get('id'):
            self.result['author'] = {
                "user_id": str(author.get('id', '')),
                "user_name": author.get('name', ''),
                "nick_name": author.get('name_show', '') or author.get('name', ''),
                "level_id": safe_int(author.get('level_id', 0)),
                "is_author": True,
            }

    def _parse_client_posts(self, post_list: list, full_data: dict) -> list:
        comments = []
        thread = full_data.get('thread', {}) or full_data.get('data', {}).get('thread', {})
        thread_author_id = str(thread.get('author', {}).get('id', ''))

        user_list = full_data.get('user_list', []) or full_data.get('data', {}).get('user_list', [])
        user_map = self._user_map
        for u in user_list:
            if isinstance(u, dict):
                uid = str(u.get('id', ''))
                if uid:
                    user_map[uid] = {
                        "user_id": uid,
                        "user_name": u.get('name', ''),
                        "nick_name": u.get('name_show', '') or u.get('name', ''),
                        "level_id": safe_int(u.get('level_id', 0)),
                        "portrait": u.get('portrait', ''),
                        "ip_address": u.get('ip_address', ''),
                    }

        t_author = thread.get('author', {})
        if isinstance(t_author, dict) and t_author.get('id'):
            t_uid = str(t_author['id'])
            if t_uid not in user_map:
                user_map[t_uid] = {
                    "user_id": t_uid,
                    "user_name": t_author.get('name', ''),
                    "nick_name": t_author.get('name_show', '') or t_author.get('name', ''),
                    "level_id": safe_int(t_author.get('level_id', 0)),
                    "portrait": t_author.get('portrait', ''),
                    "ip_address": t_author.get('ip_address', ''),
                }

        self._user_map = user_map

        for post in post_list:
            floor_num = safe_int(post.get('floor', 0))
            post_id = str(post.get('id', ''))
            uid = str(post.get('author_id', ''))

            if uid and uid in user_map:
                u_info = user_map[uid]
                user_name = u_info.get('user_name', '')
                nick_name = u_info.get('nick_name', '') or user_name
                level_id = u_info.get('level_id', 0)
            else:
                post_author = post.get('author', {})
                if isinstance(post_author, dict) and post_author:
                    extracted = _extract_user_from_author_dict(post_author)
                    if extracted and extracted.get('user_id'):
                        uid = uid or extracted['user_id']
                        user_name = extracted.get('user_name', '')
                        nick_name = extracted.get('nick_name', '') or user_name
                        level_id = extracted.get('level_id', 0)
                        user_map[uid] = extracted
                    else:
                        user_name = nick_name = ''
                        level_id = 0
                else:
                    user_name = nick_name = ''
                    level_id = 0

            user = {
                "user_id": uid,
                "user_name": user_name,
                "nick_name": nick_name,
                "level_id": level_id,
                "is_author": uid == thread_author_id and uid != '',
            }

            content_blocks = post.get('content', [])
            text_content, images = parse_content_blocks(content_blocks, user_map)

            raw_time = post.get('time', 0)
            post_time = format_timestamp(raw_time)

            agree_info = post.get('agree', {})
            like_count = 0
            if isinstance(agree_info, dict):
                like_count = safe_int(agree_info.get('agree_num', 0))
            elif isinstance(agree_info, (int, str)):
                like_count = safe_int(agree_info)

            sub_comment_count = safe_int(post.get('sub_post_number', 0))

            if floor_num == 1:
                self.result['content'] = text_content
                self.result['content_images'] = images
                self.result['create_time'] = post_time
                self.result['like_count'] = like_count
                self.result['author'] = user
                continue

            comment = {
                "floor": floor_num,
                "post_id": post_id,
                "user": user,
                "content": text_content,
                "images": images,
                "post_time": post_time,
                "like_count": like_count,
                "sub_comment_count": sub_comment_count,
                "sub_comments": [],
            }

            sub_post_list = post.get('sub_post_list', {})
            if isinstance(sub_post_list, dict):
                sub_posts = sub_post_list.get('sub_post_list', [])
                if sub_posts:
                    for sp in sub_posts:
                        _resolve_sub_comment_user(sp, user_map)
                        _resolve_reply_to(sp, user_map)
                    for sp in sub_posts:
                        sp_uid, sp_name = _resolve_sub_comment_user(sp, user_map)
                        reply_to = _resolve_reply_to(sp, user_map)

                        sp_content_blocks = sp.get('content', [])
                        sp_text, sp_images = parse_content_blocks(sp_content_blocks, user_map)

                        if reply_to:
                            sp_text = _fix_content_reply_to(sp_text, reply_to)

                        sub_comment = {
                            "user_name": sp_name,
                            "user_id": sp_uid,
                            "content": sp_text,
                            "time": format_timestamp(sp.get('time', 0)),
                        }
                        if reply_to and reply_to.get('name'):
                            sub_comment['reply_to'] = reply_to
                        if sp_images:
                            sub_comment['images'] = sp_images
                        comment['sub_comments'].append(sub_comment)

            comments.append(comment)

        return comments

    def _fetch_sub_comments_client(self, tid: str, pid: str, total: int) -> list:
        sub_comments = []
        page = 1
        max_sub_pages = (total // 10) + 2
        user_map = self._user_map

        while page <= max_sub_pages:
            params = self._build_client_params({
                "kz": tid,
                "pid": pid,
                "pn": str(page),
                "rn": "20",
            })

            resp = self._post(CLIENT_FLOOR_API_URL, data=params, headers=CLIENT_HEADERS)
            if not resp:
                break

            try:
                data = resp.json()
            except json.JSONDecodeError:
                break

            error_code = data.get('error_code', '')
            if str(error_code) != '0' and str(error_code) != '':
                break

            floor_user_list = data.get('user_list', []) or data.get('data', {}).get('user_list', [])
            for u in floor_user_list:
                if isinstance(u, dict):
                    uid = str(u.get('id', ''))
                    if uid:
                        if uid not in user_map:
                            user_map[uid] = {
                                "user_id": uid,
                                "user_name": u.get('name', ''),
                                "name_show": u.get('name_show', '') or u.get('name', ''),
                                "nick_name": u.get('name_show', '') or u.get('name', ''),
                                "level_id": safe_int(u.get('level_id', 0)),
                            }
                        else:
                            existing = user_map[uid]
                            if not existing.get('nick_name') and not existing.get('name_show'):
                                new_name = u.get('name_show', '') or u.get('name', '')
                                if new_name:
                                    existing['nick_name'] = new_name
                                    existing['name_show'] = new_name

            subpost_list = data.get('subpost_list', [])
            if not subpost_list:
                subpost_list = data.get('data', {}).get('subpost_list', [])

            if not subpost_list:
                break

            for sp in subpost_list:
                _resolve_sub_comment_user(sp, user_map)
                _resolve_reply_to(sp, user_map)

            for sp in subpost_list:
                sp_uid, sp_name = _resolve_sub_comment_user(sp, user_map)
                reply_to = _resolve_reply_to(sp, user_map)

                sp_content_blocks = sp.get('content', [])
                sp_text, sp_images = parse_content_blocks(sp_content_blocks, user_map)

                if reply_to:
                    sp_text = _fix_content_reply_to(sp_text, reply_to)

                sub_comment = {
                    "user_name": sp_name,
                    "user_id": sp_uid,
                    "content": sp_text,
                    "time": format_timestamp(sp.get('time', 0)),
                }
                if reply_to and reply_to.get('name'):
                    sub_comment['reply_to'] = reply_to
                if sp_images:
                    sub_comment['images'] = sp_images
                sub_comments.append(sub_comment)

            page += 1
            time.sleep(REQUEST_DELAY * 0.5)

        return sub_comments

    def _scrape_via_mobile_api(self) -> bool:
        page_num = 1
        while page_num <= self.max_pages:
            params = {
                "kz": self.tid,
                "pn": str(page_num),
                "rn": "30",
                "obj_param2": "chrome",
                "format": "json",
            }

            resp = self._get(MOBILE_API_URL, params=params, headers=MOBILE_HEADERS)
            if not resp:
                if page_num == 1:
                    return False
                break

            try:
                data = resp.json()
            except json.JSONDecodeError:
                if page_num == 1:
                    return False
                break

            if data.get('errno', -1) != 0:
                if page_num == 1:
                    return False
                break

            if page_num == 1:
                api_data = data.get('data', {})
                thread_data = api_data.get('thread', {})
                forum_data = api_data.get('forum', {})
                self.result['title'] = thread_data.get('title', '')
                self.result['forum_name'] = forum_data.get('name', '')
                self.result['total_replies'] = safe_int(thread_data.get('reply_num', 0))
                self.result['share_count'] = safe_int(thread_data.get('share_num', 0))

            post_list = data.get('data', {}).get('post_list', [])
            if not post_list:
                if page_num == 1:
                    return False
                break

            thread_data = data.get('data', {}).get('thread', {})
            thread_author_id = thread_data.get('author', {}).get('id', '')

            for post in post_list:
                author = post.get('author', {})
                floor_num = safe_int(post.get('floor', 0))

                content_raw = post.get('content', '')
                if isinstance(content_raw, list):
                    text_content, images = parse_content_blocks(content_raw)
                elif isinstance(content_raw, str):
                    text_content = clean_text(content_raw)
                    images = []
                else:
                    text_content = str(content_raw) if content_raw else ''
                    images = []

                comment = {
                    "floor": floor_num,
                    "post_id": str(post.get('id', '')),
                    "user": {
                        "user_id": str(author.get('id', '')),
                        "user_name": author.get('name', ''),
                        "nick_name": author.get('name_show', '') or author.get('name', ''),
                        "level_id": safe_int(author.get('level_id', 0)),
                        "is_author": str(author.get('id', '')) == str(thread_author_id),
                    },
                    "content": text_content,
                    "images": images,
                    "post_time": format_timestamp(post.get('time', '')),
                    "like_count": safe_int(
                        post.get('agree', {}).get('agree_num', 0)
                        if isinstance(post.get('agree'), dict)
                        else post.get('agree', 0)
                    ),
                    "sub_comment_count": safe_int(post.get('sub_post_number', 0)),
                    "sub_comments": [],
                }

                if floor_num == 1:
                    self.result['content'] = text_content
                    self.result['content_images'] = images
                    self.result['create_time'] = comment['post_time']
                    self.result['author'] = comment['user']
                    continue

                self.result['comments'].append(comment)

            pager = data.get('data', {}).get('page', {})
            total_page = safe_int(pager.get('total_page', 1))
            self.result['total_pages'] = total_page
            if page_num >= total_page:
                break

            page_num += 1
            time.sleep(REQUEST_DELAY)

        return len(self.result['comments']) > 0

    def _scrape_all_sub_comments(self):
        floors_needing_sub = [
            c for c in self.result['comments']
            if c.get('sub_comment_count', 0) > 0
            and len(c.get('sub_comments', [])) < c.get('sub_comment_count', 0)
        ]

        if not floors_needing_sub:
            return

        for comment in floors_needing_sub:
            pid = comment.get('post_id', '')
            count = comment.get('sub_comment_count', 0)
            if pid and count > 0:
                sub = self._fetch_sub_comments_client(self.tid, pid, count)
                if sub:
                    comment['sub_comments'] = sub
                time.sleep(REQUEST_DELAY * 0.5)

    def scrape(self) -> dict:
        success = self._scrape_via_client_api()

        if not success:
            success = self._scrape_via_mobile_api()

        if success and self.fetch_sub_comments:
            self._scrape_all_sub_comments()

        return self.result


# ============================================================
# 批量任务扫描 & 执行
# ============================================================

def scan_tasks(input_base: Path) -> List[Dict]:
    """
    扫描输入目录，返回任务列表。
    每个任务为:
      {
        "thread_id": str,
        "relative_path": Path,   # 相对于 input_base 的路径（含文件名）
        "output_file": Path,     # 输出 JSON 完整路径
        "source_line": dict,     # 原始 JSONL 行数据
      }
    """
    tasks = []
    seen_ids = set()  # 全局去重

    for game_dir in GAME_DIRS:
        for sentiment in SENTIMENT_DIRS:
            dir_path = input_base / game_dir / sentiment
            if not dir_path.exists():
                continue

            for filename in TARGET_FILES:
                jsonl_path = dir_path / filename
                if not jsonl_path.exists():
                    continue

                with open(jsonl_path, "r", encoding="utf-8") as f:
                    for lineno, line in enumerate(f, 1):
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            record = json.loads(line)
                        except json.JSONDecodeError:
                            logger.warning(f"JSON解析失败: {jsonl_path}:{lineno}")
                            continue

                        thread_id = str(record.get("thread_id", "")).strip()
                        if not thread_id or not thread_id.isdigit():
                            logger.warning(f"无效 thread_id: {jsonl_path}:{lineno}")
                            continue

                        if thread_id in seen_ids:
                            logger.debug(f"跳过重复 thread_id: {thread_id}")
                            continue
                        seen_ids.add(thread_id)

                        # 相对路径（不含基础目录）
                        rel_path = jsonl_path.relative_to(input_base)

                        tasks.append({
                            "thread_id": thread_id,
                            "relative_path": rel_path,
                            "source_line": record,
                        })

    logger.info(f"共扫描到 {len(tasks)} 个唯一帖子待爬取")
    return tasks


def already_scraped(output_dir: Path, thread_id: str) -> bool:
    """检查该帖子是否已经爬取过（断点续传）"""
    output_file = output_dir / f"{thread_id}.json"
    if output_file.exists() and output_file.stat().st_size > 100:
        return True
    return False


def scrape_one(task: dict, output_base: Path, max_pages: int) -> Dict:
    """
    爬取单个帖子，返回结果摘要。
    每个线程独立创建 TiebaPostScraper 实例，避免共享 session。
    """
    tid = task["thread_id"]
    rel_path = task["relative_path"]

    # 输出目录 = output_base / game_dir / sentiment /
    output_dir = output_base / rel_path.parent
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / f"{tid}.json"

    # 断点续传：已爬过则跳过
    if already_scraped(output_dir, tid):
        return {
            "tid": tid,
            "status": "skipped",
            "output_file": str(output_file),
            "title": "",
        }

    scraper = TiebaPostScraper(
        tid=tid,
        max_pages=max_pages,
        fetch_sub_comments=True,
    )

    try:
        result = scraper.scrape()
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        return {
            "tid": tid,
            "status": "success",
            "output_file": str(output_file),
            "title": result.get("title", ""),
            "comments": len(result.get("comments", [])),
        }
    except Exception as e:
        return {
            "tid": tid,
            "status": "error",
            "error": str(e),
            "output_file": str(output_file),
        }


def run_batch(
    input_base: Path,
    output_base: Path,
    max_workers: int = 5,
    max_pages: int = 50,
):
    """
    多线程批量爬取主逻辑。

    线程数建议（Mac M4）：
      - 网络 I/O 密集型，4-8 线程较为合适
      - 过多线程可能触发服务端限流
      - 默认 5 线程
    """
    tasks = scan_tasks(input_base)
    if not tasks:
        logger.warning("未找到任何有效任务，请检查输入目录和文件结构")
        return

    total = len(tasks)
    _progress_counter["total"] = total
    _progress_counter["done"] = 0
    _progress_counter["success"] = 0
    _progress_counter["fail"] = 0
    _progress_counter["skip"] = 0

    start_time = time.time()

    logger.info(f"开始批量爬取，共 {total} 个帖子，使用 {max_workers} 个并发线程")
    logger.info(f"输入目录: {input_base.resolve()}")
    logger.info(f"输出目录: {output_base.resolve()}")
    logger.info("=" * 60)

    failed_tasks = []

    with ThreadPoolExecutor(
        max_workers=max_workers,
        thread_name_prefix="TiebaWorker"
    ) as executor:

        future_to_task = {
            executor.submit(scrape_one, task, output_base, max_pages): task
            for task in tasks
        }

        for future in as_completed(future_to_task):
            task = future_to_task[future]
            tid = task["thread_id"]

            try:
                res = future.result()
            except Exception as e:
                res = {"tid": tid, "status": "error", "error": str(e)}

            with _progress_lock:
                _progress_counter["done"] += 1
                done = _progress_counter["done"]

                if res["status"] == "success":
                    _progress_counter["success"] += 1
                    logger.info(
                        f"[{done}/{total}] ✅ {tid} | "
                        f"{res.get('title', '')[:30]} | "
                        f"{res.get('comments', 0)} 楼"
                    )
                elif res["status"] == "skipped":
                    _progress_counter["skip"] += 1
                    logger.info(f"[{done}/{total}] ⏭  {tid} 已存在，跳过")
                else:
                    _progress_counter["fail"] += 1
                    logger.warning(
                        f"[{done}/{total}] ❌ {tid} 失败: {res.get('error', 'unknown')}"
                    )
                    failed_tasks.append(task)

                # 每 50 个打印一次进度摘要
                if done % 50 == 0:
                    elapsed = time.time() - start_time
                    speed = done / elapsed if elapsed > 0 else 0
                    eta = (total - done) / speed if speed > 0 else 0
                    logger.info(
                        f"\n--- 进度摘要 [{done}/{total}] ---\n"
                        f"  成功: {_progress_counter['success']}  "
                        f"跳过: {_progress_counter['skip']}  "
                        f"失败: {_progress_counter['fail']}\n"
                        f"  速度: {speed:.2f} 帖/秒  "
                        f"预计剩余: {eta/60:.1f} 分钟\n"
                    )

    elapsed = time.time() - start_time
    logger.info("\n" + "=" * 60)
    logger.info("批量爬取完成!")
    logger.info(f"  总计:   {total}")
    logger.info(f"  成功:   {_progress_counter['success']}")
    logger.info(f"  跳过:   {_progress_counter['skip']}")
    logger.info(f"  失败:   {_progress_counter['fail']}")
    logger.info(f"  总耗时: {elapsed/60:.1f} 分钟")
    logger.info("=" * 60)

    # 保存失败任务清单，方便重试
    if failed_tasks:
        fail_log = output_base / "failed_tasks.jsonl"
        with open(fail_log, "w", encoding="utf-8") as f:
            for t in failed_tasks:
                f.write(json.dumps({
                    "thread_id": t["thread_id"],
                    "relative_path": str(t["relative_path"]),
                }, ensure_ascii=False) + "\n")
        logger.info(f"失败任务已记录至: {fail_log}")


# ============================================================
# 命令行入口
# ============================================================
def parse_args():
    parser = argparse.ArgumentParser(
        description="百度贴吧批量帖子爬虫（多线程版）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 使用默认设置（5线程，最多50页/帖）
  python tieba_batch_crawler.py

  # 自定义线程数和爬取深度
  python tieba_batch_crawler.py --workers 8 --max-pages 30

  # 自定义输入输出目录
  python tieba_batch_crawler.py \\
    --input-dir /data/category_classify_output_split_20260326 \\
    --output-dir /data/category_classify_output_split_post_20260326

线程数建议（Mac M4）:
  - 轻量爬取（测试）: --workers 3
  - 正常爬取:         --workers 5  (默认)
  - 激进爬取:         --workers 8  (可能触发限流)
        """
    )
    parser.add_argument(
        "--workers", type=int, default=5,
        help="并发线程数，建议 3-8（默认: 5）"
    )
    parser.add_argument(
        "--max-pages", type=int, default=50,
        help="每个帖子最多爬取页数（默认: 50）"
    )
    parser.add_argument(
        "--input-dir", type=str,
        default=INPUT_BASE_DIR,
        help=f"输入根目录（默认: {INPUT_BASE_DIR}）"
    )
    parser.add_argument(
        "--output-dir", type=str,
        default=OUTPUT_BASE_DIR,
        help=f"输出根目录（默认: {OUTPUT_BASE_DIR}）"
    )
    return parser.parse_args()


def main():
    args = parse_args()

    input_base = Path(args.input_dir)
    output_base = Path(args.output_dir)

    if not input_base.exists():
        logger.error(f"输入目录不存在: {input_base.resolve()}")
        sys.exit(1)

    output_base.mkdir(parents=True, exist_ok=True)

    run_batch(
        input_base=input_base,
        output_base=output_base,
        max_workers=args.workers,
        max_pages=args.max_pages,
    )


if __name__ == "__main__":
    main()