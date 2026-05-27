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
  python tieba_scraper_v2.py <帖子URL或帖子ID>
  python tieba_scraper_v2.py https://tieba.baidu.com/p/10515321107
  python tieba_scraper_v2.py 10515321107

核心改进：
  1. 使用贴吧客户端 API（tiebac.baidu.com/c/f/pb/page）获取帖子内容
     - 该接口需要 MD5 签名但不需要登录
     - 返回完整的帖子内容（文字、图片、视频等）
  2. 使用 totalComment API 获取楼中楼子评论
  3. 移动端 Web API 作为备用方案
  4. 时间戳自动格式化
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
# 贴吧客户端 API（主要方案）
CLIENT_API_URL = "https://tiebac.baidu.com/c/f/pb/page"
# 楼中楼子评论 API（客户端）
CLIENT_FLOOR_API_URL = "https://tiebac.baidu.com/c/f/pb/floor"
# 移动端 Web API（备用方案）
MOBILE_API_URL = "https://tieba.baidu.com/mg/p/getPbData"
# totalComment API（获取楼中楼子评论的另一种方式）
TOTAL_COMMENT_API_URL = "https://tieba.baidu.com/p/totalComment"

# 客户端签名密钥（公开的贴吧客户端签名 salt）
SIGN_KEY = "tiebaclient!!!"

# 通用客户端参数
CLIENT_COMMON_PARAMS = {
    "_client_type": "2",        # 2=Android
    "_client_version": "12.57.1.0",
    "_os_version": "33",        # Android 13
    "_phone_imei": "000000000000000",
    "from": "tieba",
    "cuid": "baidutiebaapp",
}

# 请求头
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

# 请求间隔（秒）—— 尊重服务器
REQUEST_DELAY = 1.0


# ============================================================
# 工具函数
# ============================================================
def extract_tid(url_or_id: str) -> str:
    """从 URL 或纯数字中提取帖子 ID (tid)"""
    url_or_id = url_or_id.strip()
    if url_or_id.isdigit():
        return url_or_id
    match = re.search(r'/p/(\d+)', url_or_id)
    if match:
        return match.group(1)
    raise ValueError(f"无法从输入中提取帖子ID: {url_or_id}")


def clean_text(text: str) -> str:
    """清理文本中的多余空白"""
    if not text:
        return ""
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def safe_int(val, default=0):
    """安全转换为整数"""
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
    """将 Unix 时间戳格式化为可读时间字符串"""
    if not ts:
        return ""
    try:
        ts_val = int(ts)
        if ts_val > 1e12:  # 毫秒时间戳
            ts_val = ts_val // 1000
        return datetime.fromtimestamp(ts_val).strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError, OSError):
        return str(ts)


def calc_sign(params: dict) -> str:
    """
    计算贴吧客户端 API 的签名
    规则：将所有参数按 key 排序，拼接为 key=value 格式，
    末尾加上 SIGN_KEY，然后计算 MD5
    """
    sorted_params = sorted(params.items())
    sign_str = ''.join(f"{k}={v}" for k, v in sorted_params)
    sign_str += SIGN_KEY
    return hashlib.md5(sign_str.encode('utf-8')).hexdigest()


def parse_content_blocks(content_blocks) -> tuple:
    """
    解析客户端 API 返回的 content 结构化内容块
    返回 (纯文本, 图片列表)
    """
    text_parts = []
    images = []

    if not content_blocks:
        return "", []

    # content_blocks 可能是列表（结构化内容）或字符串
    if isinstance(content_blocks, str):
        return content_blocks.strip(), []

    if not isinstance(content_blocks, list):
        return str(content_blocks).strip(), []

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
            # 表情（文字表情）
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
            # 图片可能有描述
            desc = block.get('bsize', '')
        elif block_type == '4':
            # 表情图片（贴吧自带表情）
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
            voice_url = block.get('during', '')
            text_parts.append('[语音]')
        elif block_type == '11':
            # @用户
            text = block.get('text', '')
            if text:
                text_parts.append(text)
        elif block_type == '18':
            # 话题标签
            text = block.get('text', '')
            if text:
                text_parts.append(text)
        elif block_type == '20':
            # 小视频/短视频
            text_parts.append('[短视频]')
        else:
            # 未知类型，尝试提取文本
            text = block.get('text', '')
            if text:
                text_parts.append(text)

    content = '\n'.join(text_parts).strip()
    return content, images


# ============================================================
# 核心爬虫类
# ============================================================
class TiebaPostScraper:
    """百度贴吧帖子爬虫 v2 - 客户端API + 移动端API双模式"""

    def __init__(self, tid: str, max_pages: int = 100, fetch_sub_comments: bool = True):
        self.tid = tid
        self.max_pages = max_pages
        self.fetch_sub_comments = fetch_sub_comments
        self.session = requests.Session()

        # 最终结果
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
    def _get(self, url: str, params: dict = None, headers: dict = None,
             timeout: int = 15) -> Optional[requests.Response]:
        """带重试的 GET 请求"""
        for attempt in range(3):
            try:
                resp = self.session.get(
                    url, params=params, headers=headers, timeout=timeout
                )
                resp.raise_for_status()
                return resp
            except requests.RequestException as e:
                logger.warning(f"GET 请求失败 (第{attempt+1}次): {url} -> {e}")
                if attempt < 2:
                    time.sleep(REQUEST_DELAY * (attempt + 1))
        return None

    def _post(self, url: str, data: dict = None, headers: dict = None,
              timeout: int = 15) -> Optional[requests.Response]:
        """带重试的 POST 请求"""
        for attempt in range(3):
            try:
                resp = self.session.post(
                    url, data=data, headers=headers, timeout=timeout
                )
                resp.raise_for_status()
                return resp
            except requests.RequestException as e:
                logger.warning(f"POST 请求失败 (第{attempt+1}次): {url} -> {e}")
                if attempt < 2:
                    time.sleep(REQUEST_DELAY * (attempt + 1))
        return None

    # ----------------------------------------------------------
    # 方案一：贴吧客户端 API（主要方案）
    # ----------------------------------------------------------
    def _build_client_params(self, extra_params: dict) -> dict:
        """构建客户端 API 请求参数（含签名）"""
        params = dict(CLIENT_COMMON_PARAMS)
        params.update(extra_params)
        params['sign'] = calc_sign(params)
        return params

    def _scrape_via_client_api(self) -> bool:
        """通过贴吧客户端 API 获取帖子数据"""
        logger.info("使用贴吧客户端 API 方式...")

        page_num = 1
        while page_num <= self.max_pages:
            logger.info(f"正在爬取第 {page_num} 页...")

            params = self._build_client_params({
                "kz": self.tid,
                "pn": str(page_num),
                "rn": "30",         # 每页30条
                "lz": "0",          # 0=全部, 1=只看楼主
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

            # 检查返回状态
            error_code = data.get('error_code', '') or data.get('error', {}).get('errno', '')
            if str(error_code) != '0' and str(error_code) != '':
                error_msg = data.get('error_msg', '') or data.get('error', {}).get('errmsg', '')
                logger.warning(f"客户端 API 返回错误: code={error_code}, msg={error_msg}")
                if page_num == 1:
                    return False
                break

            # 第一页提取元信息
            if page_num == 1:
                self._parse_client_meta(data)

            # 提取楼层
            post_list = data.get('post_list', [])
            if not post_list:
                # 兼容不同嵌套结构
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

            # 检查是否最后一页
            page_info = data.get('page', {}) or data.get('data', {}).get('page', {})
            total_pages = safe_int(page_info.get('total_page', 0))
            if total_pages > 0:
                self.result['total_pages'] = total_pages
            if page_num >= self.result.get('total_pages', 1) and self.result['total_pages'] > 0:
                logger.info("已到达最后一页")
                break

            # 也检查 has_more 字段
            has_more = page_info.get('has_more', '1')
            if str(has_more) == '0':
                logger.info("has_more=0，已到达最后一页")
                break

            page_num += 1
            time.sleep(REQUEST_DELAY)

        return len(self.result['comments']) > 0

    def _parse_client_meta(self, data: dict):
        """从客户端 API 响应中提取帖子元信息"""
        # 帖子信息可能在不同层级
        thread = data.get('thread', {}) or data.get('data', {}).get('thread', {})
        forum = data.get('forum', {}) or data.get('data', {}).get('forum', {})
        page_info = data.get('page', {}) or data.get('data', {}).get('page', {})

        self.result['title'] = thread.get('title', '') or ''
        self.result['forum_name'] = forum.get('name', '') or ''
        self.result['total_replies'] = safe_int(thread.get('reply_num', 0))
        self.result['share_count'] = safe_int(thread.get('share_num', 0))
        self.result['total_pages'] = safe_int(page_info.get('total_page', 0))

        # 如果 thread 中有 author 信息
        author = thread.get('author', {})
        if author:
            self.result['author'] = {
                "user_id": str(author.get('id', '')),
                "user_name": author.get('name', ''),
                "nick_name": author.get('name_show', '') or author.get('name', ''),
            }

    def _parse_client_posts(self, post_list: list, full_data: dict) -> list:
        """解析客户端 API 返回的帖子列表"""
        comments = []
        thread = full_data.get('thread', {}) or full_data.get('data', {}).get('thread', {})
        thread_author_id = thread.get('author', {}).get('id', '')

        for post in post_list:
            author = post.get('author', {})
            floor_num = safe_int(post.get('floor', 0))
            post_id = str(post.get('id', ''))

            # 解析内容块
            content_blocks = post.get('content', [])
            text_content, images = parse_content_blocks(content_blocks)

            # 用户信息
            user = {
                "user_id": str(author.get('id', '')),
                "user_name": author.get('name', ''),
                "nick_name": author.get('name_show', '') or author.get('name', ''),
                "level_id": safe_int(author.get('level_id', 0)),
                "is_author": str(author.get('id', '')) == str(thread_author_id),
            }

            # 发帖时间
            raw_time = post.get('time', 0)
            post_time = format_timestamp(raw_time)

            # 点赞数
            agree_info = post.get('agree', {})
            like_count = 0
            if isinstance(agree_info, dict):
                like_count = safe_int(agree_info.get('agree_num', 0))
            elif isinstance(agree_info, (int, str)):
                like_count = safe_int(agree_info)

            # 子评论数
            sub_post_info = post.get('sub_post_number', 0)
            sub_comment_count = safe_int(sub_post_info)

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

            # 第1楼是主帖
            if floor_num == 1:
                self.result['content'] = text_content
                self.result['content_images'] = images
                self.result['create_time'] = post_time
                if not self.result['author'] or not self.result['author'].get('user_id'):
                    self.result['author'] = user

            # 客户端 API 可能已包含部分子评论
            sub_post_list = post.get('sub_post_list', {})
            if isinstance(sub_post_list, dict):
                sub_posts = sub_post_list.get('sub_post_list', [])
                if sub_posts:
                    for sp in sub_posts:
                        sp_author = sp.get('author', {})
                        sp_content_blocks = sp.get('content', [])
                        sp_text, sp_images = parse_content_blocks(sp_content_blocks)
                        sub_comment = {
                            "user_name": sp_author.get('name_show', '') or sp_author.get('name', ''),
                            "user_id": str(sp_author.get('id', '')),
                            "content": sp_text,
                            "time": format_timestamp(sp.get('time', 0)),
                        }
                        if sp_images:
                            sub_comment['images'] = sp_images
                        comment['sub_comments'].append(sub_comment)

            comments.append(comment)

        return comments

    # ----------------------------------------------------------
    # 楼中楼子评论爬取（客户端 API）
    # ----------------------------------------------------------
    def _fetch_sub_comments_client(self, tid: str, pid: str, total: int) -> list:
        """通过客户端 API 爬取楼中楼子评论"""
        sub_comments = []
        page = 1
        max_sub_pages = (total // 10) + 2

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

            # 提取子评论列表
            subpost_list = data.get('subpost_list', [])
            if not subpost_list:
                subpost_list = data.get('data', {}).get('subpost_list', [])

            if not subpost_list:
                break

            for sp in subpost_list:
                sp_author = sp.get('author', {})
                sp_content_blocks = sp.get('content', [])
                sp_text, sp_images = parse_content_blocks(sp_content_blocks)

                sub_comment = {
                    "user_name": sp_author.get('name_show', '') or sp_author.get('name', ''),
                    "user_id": str(sp_author.get('id', '')),
                    "content": sp_text,
                    "time": format_timestamp(sp.get('time', 0)),
                }
                if sp_images:
                    sub_comment['images'] = sp_images
                sub_comments.append(sub_comment)

            page += 1
            time.sleep(REQUEST_DELAY * 0.5)

        return sub_comments

    # ----------------------------------------------------------
    # 楼中楼子评论爬取（totalComment API - 备用）
    # ----------------------------------------------------------
    def _fetch_sub_comments_total(self, tid: str, pn: int = 1) -> dict:
        """
        通过 totalComment API 批量获取子评论
        返回 {pid: [sub_comments]} 字典
        """
        params = {
            "tid": tid,
            "fid": "0",
            "pn": str(pn),
            "see_lz": "0",
        }
        resp = self._get(
            TOTAL_COMMENT_API_URL,
            params=params,
            headers=MOBILE_HEADERS,
        )
        if not resp:
            return {}

        try:
            data = resp.json()
        except json.JSONDecodeError:
            return {}

        result = {}
        comment_list = data.get('data', {}).get('comment_list', {})
        if not isinstance(comment_list, dict):
            return {}

        for pid, comments_data in comment_list.items():
            sub_list = []
            comment_info = comments_data.get('comment_info', [])
            if not isinstance(comment_info, list):
                continue
            for ci in comment_info:
                sub = {
                    "user_name": ci.get('show_nickname', '') or ci.get('username', ''),
                    "user_id": str(ci.get('user_id', '')),
                    "content": clean_text(ci.get('content', '')),
                    "time": ci.get('now_time', ''),
                }
                sub_list.append(sub)
            if sub_list:
                result[str(pid)] = sub_list

        return result

    # ----------------------------------------------------------
    # 方案二：移动端 Web API（备用）
    # ----------------------------------------------------------
    def _scrape_via_mobile_api(self) -> bool:
        """通过移动端 API 获取帖子数据"""
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

            # 第一页解析元信息
            if page_num == 1:
                api_data = data.get('data', {})
                thread_data = api_data.get('thread', {})
                forum_data = api_data.get('forum', {})

                self.result['title'] = thread_data.get('title', '')
                self.result['forum_name'] = forum_data.get('name', '')
                self.result['total_replies'] = safe_int(thread_data.get('reply_num', 0))
                self.result['share_count'] = safe_int(thread_data.get('share_num', 0))

            # 解析帖子列表
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

                # 解析内容（移动端可能返回结构化内容或纯文本）
                content_raw = post.get('content', '')
                if isinstance(content_raw, list):
                    text_content, images = parse_content_blocks(content_raw)
                elif isinstance(content_raw, str):
                    text_content = clean_text(content_raw)
                    images = []
                else:
                    text_content = str(content_raw) if content_raw else ''
                    images = []

                # 如果内容为空，尝试从 first_post_content 获取（仅主楼）
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

                self.result['comments'].append(comment)

            # 检查分页
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
        """执行爬取，返回结构化数据"""
        logger.info("=" * 60)
        logger.info(f"开始爬取帖子: {self.tid}")
        logger.info("=" * 60)

        # 方案一：客户端 API
        success = self._scrape_via_client_api()

        # 方案二：如果客户端 API 失败，尝试移动端 Web API
        if not success:
            logger.info("客户端 API 方式失败，切换到移动端 Web API...")
            success = self._scrape_via_mobile_api()

        if not success:
            logger.error("所有方式均失败，请检查帖子ID是否正确或网络状况")
            return self.result

        # 爬取缺失的楼中楼子评论
        if self.fetch_sub_comments:
            self._scrape_all_sub_comments()

        # 统计汇总
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
        """遍历所有楼层，补充爬取楼中楼子评论"""
        # 找出有子评论但尚未爬取到的楼层
        floors_needing_sub = [
            c for c in self.result['comments']
            if c.get('sub_comment_count', 0) > 0
            and len(c.get('sub_comments', [])) < c.get('sub_comment_count', 0)
        ]

        if not floors_needing_sub:
            logger.info("所有楼中楼评论已获取完毕，无需额外爬取")
            return

        logger.info(f"正在补充爬取 {len(floors_needing_sub)} 个楼层的楼中楼评论...")

        # 方式一：先尝试 totalComment API 批量获取
        try:
            total_pages = max(self.result.get('total_pages', 1), 1)
            all_sub_by_pid = {}
            for pn in range(1, total_pages + 1):
                batch = self._fetch_sub_comments_total(self.tid, pn)
                all_sub_by_pid.update(batch)
                if batch:
                    logger.info(f"  totalComment API 第{pn}页: 获取 {len(batch)} 个楼层的子评论")
                time.sleep(REQUEST_DELAY * 0.5)

            # 合并到结果中
            if all_sub_by_pid:
                for comment in self.result['comments']:
                    pid = comment.get('post_id', '')
                    if pid in all_sub_by_pid and len(comment.get('sub_comments', [])) < comment.get('sub_comment_count', 0):
                        comment['sub_comments'] = all_sub_by_pid[pid]

        except Exception as e:
            logger.warning(f"totalComment API 批量获取失败: {e}")

        # 方式二：对仍然缺少子评论的楼层，逐个使用客户端 API
        still_missing = [
            c for c in self.result['comments']
            if c.get('sub_comment_count', 0) > 0
            and len(c.get('sub_comments', [])) < c.get('sub_comment_count', 0)
        ]

        if still_missing:
            logger.info(f"仍有 {len(still_missing)} 个楼层需要通过客户端 API 获取子评论...")
            for i, comment in enumerate(still_missing):
                pid = comment.get('post_id', '')
                count = comment.get('sub_comment_count', 0)
                if pid and count > 0:
                    logger.info(f"  [{i+1}/{len(still_missing)}] "
                                f"第{comment['floor']}楼 ({count}条子评论)")
                    sub = self._fetch_sub_comments_client(self.tid, pid, count)
                    if sub:
                        comment['sub_comments'] = sub
                    time.sleep(REQUEST_DELAY * 0.5)

    def save_json(self, filepath: str):
        """将结果保存为 JSON 文件"""
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(self.result, f, ensure_ascii=False, indent=2)
        logger.info(f"数据已保存至: {filepath}")


# ============================================================
# 入口
# ============================================================
def main():
    default_input = "https://tieba.baidu.com/p/10584752481?fr=frs"

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

    # 打印结果摘要
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