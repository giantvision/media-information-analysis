#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
百度贴吧帖子爬虫 v2 (Baidu Tieba Post Scraper v2)
===================================================
改进版：使用贴吧客户端 API（tiebac.baidu.com）+ 移动端 Web API 双通道
解决 PC 端 403 Forbidden 问题

功能：爬取指定贴吧帖子的完整数据，包括：
  - 帖子标题、正文内容
  - 评论数量、点赞数
  - 所有楼层的用户评论（含楼中楼回复）
  - 图片链接（不下载图片，仅保存URL）
  - 用户信息（用户名、ID、等级等）

输出：结构化 JSON 文件

使用方式：
  python tieba_crawler_specific_post_v5.py <帖子URL或帖子ID>
  python tieba_crawler_specific_post_v5.py https://tieba.baidu.com/p/10591110434
  python tieba_crawler_specific_post_v5.py 10591110434
"""

import re
import json
import time
import sys
import logging
import hashlib
from datetime import datetime
from typing import Optional, List, Dict, Any

import requests

# ============================================================
# 日志配置
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ============================================================
# 常量
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

REQUEST_DELAY = 1.0


# ============================================================
# 工具函数
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
    """
    从子评论元数据中解析「被回复者」信息。

    修复说明：
      楼中楼 API 中，被回复者信息编码在 content 块里：
        content[0]: type=0, text="回复 "
        content[1]: type=4, uid=被回复者uid, text=被回复者昵称   ← 关键修复
        content[2]: type=0, text=" :正文..."

      type=4 + 有 uid → 被回复用户（@mention），不是表情。
      原代码仅检查 type=11 和 title 字段，导致被回复者名字丢失。
    """
    if not isinstance(sp, dict):
        return {}

    reply_uid = ''
    reply_name = ''

    # === 策略1（核心修复）：从 content 块中的 type=4（含uid）提取被回复者 ===
    # 结构：["回复 ", {type=4, uid=xxx, text="昵称"}, " :正文"]
    content_blocks = sp.get('content', [])
    if isinstance(content_blocks, list):
        for block in content_blocks:
            if not isinstance(block, dict):
                continue
            btype = str(block.get('type', ''))

            # ★ 关键修复：type=4 + uid 不是表情，是被回复的用户
            if btype == '4':
                block_uid = str(block.get('uid', ''))
                if block_uid and block_uid != '0':
                    reply_uid = block_uid
                    reply_name = block.get('text', '')
                    # 如果 user_map 中有更完整的昵称，优先使用
                    if reply_uid in user_map:
                        u = user_map[reply_uid]
                        reply_name = (u.get('nick_name', '')
                                      or u.get('name_show', '')
                                      or u.get('user_name', '')
                                      or reply_name)
                    break  # 一条子评论只有一个被回复者

            # 兼容：部分 API 版本用 type=11
            elif btype == '11':
                block_uid = str(block.get('uid', ''))
                if block_uid and block_uid != '0':
                    reply_uid = block_uid
                    if not reply_name:
                        reply_name = (block.get('name_show', '')
                                      or block.get('name', '')
                                      or block.get('text', ''))
                    break

    # === 策略2：title 字段（部分版本 API）===
    if not reply_name:
        title = sp.get('title', '')
        if isinstance(title, str) and title.strip():
            reply_name = title.strip()

    # === 策略3：兼容其他 API 版本的专用字段 ===
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

    # === 用 reply_name 反查 user_map 补全 uid ===
    if reply_name and not reply_uid:
        for uid, u in user_map.items():
            if (u.get('nick_name') == reply_name
                    or u.get('name_show') == reply_name
                    or u.get('user_name') == reply_name):
                reply_uid = uid
                break

    # === 用 reply_uid 查 user_map 补全 name ===
    if reply_uid and not reply_name and reply_uid in user_map:
        u = user_map[reply_uid]
        reply_name = (u.get('nick_name', '')
                      or u.get('name_show', '')
                      or u.get('user_name', ''))

    if reply_uid or reply_name:
        return {"uid": reply_uid, "name": reply_name}
    return {}


def _fix_content_reply_to(content: str, reply_to: dict) -> str:
    """
    修复子评论正文中缺失的被回复者名字。
    将 "回复 \\n :" 等残缺格式替换为 "回复 @{name} :"
    """
    if not reply_to or not reply_to.get('name'):
        return content

    name = reply_to['name']
    pattern = r'回复\s*(?:@用户\d+)?\s*\n?\s*:'
    replacement = f'回复 @{name} :'
    fixed = re.sub(pattern, replacement, content, count=1)
    return fixed


def parse_content_blocks(content_blocks, user_map: dict = None) -> tuple:
    """
    解析客户端 API 返回的 content 结构化内容块。
    返回 (纯文本, 图片列表)

    ★ 修复说明：
      type=4 分两种情况：
        - 有 uid 字段 → 被回复的用户（@mention），提取 text 作为昵称
        - 无 uid 字段 → 贴吧自带表情图片，跳过
      原代码对 type=4 一律 pass，导致被回复者昵称从正文中消失。
    """
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
            # 纯文本
            text = block.get('text', '')
            if text:
                text_parts.append(text)

        elif block_type == '1':
            # 链接
            link = block.get('link', '') or block.get('text', '')
            if link:
                text_parts.append(link)

        elif block_type == '2':
            # 贴吧文字表情
            c = block.get('c', '') or block.get('text', '')
            if c:
                text_parts.append(c)

        elif block_type == '3':
            # 图片
            img_url = (block.get('origin_src', '')
                       or block.get('big_cdn_src', '')
                       or block.get('cdn_src', '')
                       or block.get('src', ''))
            if img_url:
                if img_url.startswith('//'):
                    img_url = 'https:' + img_url
                images.append(img_url)

        elif block_type == '4':
            # ★ 核心修复：type=4 分两种情况
            uid = str(block.get('uid', ''))
            if uid and uid != '0':
                # 有 uid → 被回复的用户（@mention），不是表情
                name = block.get('text', '')
                # 尝试从 user_map 获取更完整的昵称
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
            else:
                # 无 uid → 贴吧自带表情图片，跳过
                pass

        elif block_type == '5':
            # 视频
            video_url = block.get('link', '') or block.get('text', '')
            if video_url:
                text_parts.append(f'[视频: {video_url}]')

        elif block_type == '9':
            # 电话号码
            text = block.get('text', '')
            if text:
                text_parts.append(text)

        elif block_type == '10':
            # 语音
            text_parts.append('[语音]')

        elif block_type == '11':
            # @用户 mention（部分 API 版本）
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
            # 话题标签
            text = block.get('text', '')
            if text:
                text_parts.append(text)

        elif block_type == '20':
            # 短视频
            text_parts.append('[短视频]')

        else:
            # 未知类型，保守提取文本
            text = block.get('text', '')
            if text:
                text_parts.append(text)

    # 修改后
    content = '\n'.join(text_parts).strip()
    # 修复回复格式：将 "回复 \n昵称\n :" 统一替换为 "回复 (昵称) :"
    content = re.sub(r'(回复\s*)\n(.+?)\n(\s*:)', r'\1(\2)\3', content)
    return content, images


# ============================================================
# 核心爬虫类
# ============================================================
class TiebaPostScraper:

    def __init__(self, tid: str, max_pages: int = 100, fetch_sub_comments: bool = True):
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

    # ----------------------------------------------------------
    # 网络请求
    # ----------------------------------------------------------
    def _get(self, url, params=None, headers=None, timeout=15):
        for attempt in range(3):
            try:
                resp = self.session.get(url, params=params, headers=headers, timeout=timeout)
                resp.raise_for_status()
                return resp
            except requests.RequestException as e:
                logger.warning(f"GET 请求失败 (第{attempt+1}次): {url} -> {e}")
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
                logger.warning(f"POST 请求失败 (第{attempt+1}次): {url} -> {e}")
                if attempt < 2:
                    time.sleep(REQUEST_DELAY * (attempt + 1))
        return None

    # ----------------------------------------------------------
    # 方案一：贴吧客户端 API
    # ----------------------------------------------------------
    def _build_client_params(self, extra_params: dict) -> dict:
        params = dict(CLIENT_COMMON_PARAMS)
        params.update(extra_params)
        params['sign'] = calc_sign(params)
        return params

    def _scrape_via_client_api(self) -> bool:
        logger.info("使用贴吧客户端 API 方式...")

        page_num = 1
        while page_num <= self.max_pages:
            logger.info(f"正在爬取第 {page_num} 页...")

            params = self._build_client_params({
                "kz": self.tid,
                "pn": str(page_num),
                "rn": "30",
                "lz": "0",
                "r": str(int(time.time())),
            })

            resp = self._post(CLIENT_API_URL, data=params, headers=CLIENT_HEADERS)
            if not resp:
                logger.error(f"第 {page_num} 页请求失败")
                if page_num == 1:
                    return False
                break

            try:
                data = resp.json()
            except json.JSONDecodeError:
                logger.warning("客户端 API 返回非 JSON 数据")
                if page_num == 1:
                    return False
                break

            error_code = data.get('error_code', '') or data.get('error', {}).get('errno', '')
            if str(error_code) != '0' and str(error_code) != '':
                error_msg = data.get('error_msg', '') or data.get('error', {}).get('errmsg', '')
                logger.warning(f"客户端 API 返回错误: code={error_code}, msg={error_msg}")
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
                    logger.warning("第一页未找到 post_list，尝试备用方案")
                    return False
                logger.info(f"第 {page_num} 页无更多评论")
                break

            page_comments = self._parse_client_posts(post_list, data)
            self.result['comments'].extend(page_comments)
            logger.info(f"  -> 本页获取 {len(page_comments)} 条评论")

            page_info = data.get('page', {}) or data.get('data', {}).get('page', {})
            total_pages = safe_int(page_info.get('total_page', 0))
            if total_pages > 0:
                self.result['total_pages'] = total_pages
            if page_num >= self.result.get('total_pages', 1) and self.result['total_pages'] > 0:
                logger.info("已到达最后一页")
                break

            has_more = page_info.get('has_more', '1')
            if str(has_more) == '0':
                logger.info("has_more=0，已到达最后一页")
                break

            page_num += 1
            time.sleep(REQUEST_DELAY)

        return len(self.result['comments']) > 0

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
                        elif '回复' in sp_text and re.search(r'回复\s*\n?\s*:', sp_text):
                            logger.warning(
                                f"无法解析内联子评论的被回复者: "
                                f"title='{sp.get('title', '')}', "
                                f"author={sp.get('author', {}).get('name_show', '?')}"
                            )

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

    # ----------------------------------------------------------
    # 楼中楼子评论爬取（客户端 API）
    # ----------------------------------------------------------
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
                                    if not existing.get('user_name'):
                                        existing['user_name'] = u.get('name', '')

            subpost_list = data.get('subpost_list', [])
            if not subpost_list:
                subpost_list = data.get('data', {}).get('subpost_list', [])

            if not subpost_list:
                break

            # ★ 第一遍：预扫描所有子评论的作者 + 被回复者写入 user_map
            for sp in subpost_list:
                _resolve_sub_comment_user(sp, user_map)
                _resolve_reply_to(sp, user_map)

            # ★ 第二遍：解析内容（此时 user_map 已包含本批所有用户）
            for sp in subpost_list:
                sp_uid, sp_name = _resolve_sub_comment_user(sp, user_map)
                reply_to = _resolve_reply_to(sp, user_map)

                sp_content_blocks = sp.get('content', [])
                sp_text, sp_images = parse_content_blocks(sp_content_blocks, user_map)

                if reply_to:
                    sp_text = _fix_content_reply_to(sp_text, reply_to)
                elif '回复' in sp_text and re.search(r'回复\s*\n?\s*:', sp_text):
                    logger.warning(
                        f"无法解析子评论的被回复者: "
                        f"title='{sp.get('title', '')}', "
                        f"author={sp.get('author', {}).get('name_show', '?')}"
                    )

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

    # ----------------------------------------------------------
    # 方案二：移动端 Web API（备用）
    # ----------------------------------------------------------
    def _scrape_via_mobile_api(self) -> bool:
        logger.info("尝试移动端 Web API 方式...")

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
                logger.warning("移动端 API 返回非 JSON 数据")
                if page_num == 1:
                    return False
                break

            if data.get('errno', -1) != 0:
                logger.warning(f"移动端 API 返回错误: {data.get('errmsg', 'unknown')}")
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

                if not text_content and floor_num == 1:
                    first_content = thread_data.get('first_post_content', '')
                    if first_content:
                        text_content = clean_text(str(first_content))

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

    # ----------------------------------------------------------
    # 主入口
    # ----------------------------------------------------------
    def scrape(self) -> dict:
        logger.info("=" * 60)
        logger.info(f"开始爬取帖子: {self.tid}")
        logger.info("=" * 60)

        success = self._scrape_via_client_api()

        if not success:
            logger.info("客户端 API 方式失败，切换到移动端 Web API...")
            success = self._scrape_via_mobile_api()

        if not success:
            logger.error("所有方式均失败，请检查帖子ID是否正确或网络状况")
            return self.result

        if self.fetch_sub_comments:
            self._scrape_all_sub_comments()

        total_comments = len(self.result['comments'])
        total_sub = sum(len(c.get('sub_comments', [])) for c in self.result['comments'])

        logger.info(f"\n{'=' * 60}")
        logger.info("爬取完成!")
        logger.info(f"  帖子标题: {self.result['title']}")
        logger.info(f"  所属贴吧: {self.result['forum_name']}")
        logger.info(f"  总回复数: {self.result['total_replies']}")
        logger.info(f"  爬取楼层: {total_comments}")
        logger.info(f"  楼中楼回复: {total_sub}")
        logger.info("=" * 60)

        return self.result

    def _scrape_all_sub_comments(self):
        floors_needing_sub = [
            c for c in self.result['comments']
            if c.get('sub_comment_count', 0) > 0
            and len(c.get('sub_comments', [])) < c.get('sub_comment_count', 0)
        ]

        if not floors_needing_sub:
            logger.info("所有楼中楼评论已获取完毕，无需额外爬取")
            return

        logger.info(f"正在补充爬取 {len(floors_needing_sub)} 个楼层的楼中楼评论...")

        for i, comment in enumerate(floors_needing_sub):
            pid = comment.get('post_id', '')
            count = comment.get('sub_comment_count', 0)
            if pid and count > 0:
                logger.info(f"  [{i+1}/{len(floors_needing_sub)}] "
                            f"第{comment['floor']}楼 ({count}条子评论)")
                sub = self._fetch_sub_comments_client(self.tid, pid, count)
                if sub:
                    comment['sub_comments'] = sub
                time.sleep(REQUEST_DELAY * 0.5)

    def save_json(self, filepath: str):
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(self.result, f, ensure_ascii=False, indent=2)
        logger.info(f"数据已保存至: {filepath}")


# ============================================================
# 入口
# ============================================================
def main():
    default_input = "https://tieba.baidu.com/p/10570039525?fr=frs"

    if len(sys.argv) > 1:
        user_input = sys.argv[1]
    else:
        user_input = default_input
        logger.info(f"未指定参数，使用默认帖子: {user_input}")

    try:
        tid = extract_tid(user_input)
    except ValueError as e:
        logger.error(str(e))
        sys.exit(1)

    scraper = TiebaPostScraper(
        tid=tid,
        max_pages=50,
        fetch_sub_comments=True,
    )

    result = scraper.scrape()

    output_file = f"tieba_post_{tid}.json"
    scraper.save_json(output_file)

    print("\n" + "=" * 60)
    print("【结果预览】")
    print(f"  帖子ID:   {result['post_id']}")
    print(f"  标题:     {result['title']}")
    print(f"  贴吧:     {result['forum_name']}")
    print(f"  楼主:     {result['author'].get('nick_name', 'N/A')}")
    print(f"  发帖时间: {result['create_time']}")
    print(f"  评论数:   {result['total_replies']}")
    print(f"  分享数:   {result['share_count']}")
    print(f"  爬取楼层: {len(result['comments'])}")
    if result['comments']:
        print(f"\n  前3条评论预览:")
        for c in result['comments'][:3]:
            nick = c['user'].get('nick_name', '匿名')
            text = c['content'][:80] + ('...' if len(c['content']) > 80 else '')
            sub_count = len(c.get('sub_comments', []))
            print(f"    [{c['floor']}楼] {nick}: {text}")
            if sub_count > 0:
                print(f"           ↳ {sub_count} 条楼中楼回复")
    print("=" * 60)

    return output_file


if __name__ == '__main__':
    main()