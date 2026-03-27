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

  # https://tieba.baidu.com/p/10591110434?fr=frs

核心改进：
  1. 使用贴吧客户端 API（tiebac.baidu.com/c/f/pb/page）获取帖子内容
     - 该接口需要 MD5 签名但不需要登录
     - 返回完整的帖子内容（文字、图片、视频等）
  2. 使用客户端 API（c/f/pb/floor）获取楼中楼子评论
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


def _extract_user_from_author_dict(author_dict: dict) -> dict:
    """
    从子评论/帖子自身的 author 字典中提取用户信息。
    用于 user_list 不包含该用户时的兜底提取。
    """
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
    """
    解析子评论的用户信息，返回 (uid, user_name)。
    优先从 user_map 查找，其次从子评论自身的 author 字典提取，
    并将新发现的用户写回 user_map。

    Args:
        sp: 子评论原始数据字典
        user_map: 全局用户映射（会被就地更新）

    Returns:
        (uid_str, display_name)
    """
    # 1. 获取 uid：优先 author_id 字段，其次 author.id
    sp_uid = str(sp.get('author_id', ''))
    sp_author = sp.get('author', {})
    if not sp_uid and isinstance(sp_author, dict):
        sp_uid = str(sp_author.get('id', ''))

    # 2. 从 user_map 查找
    if sp_uid and sp_uid in user_map:
        sp_name = (user_map[sp_uid].get('nick_name', '')
                   or user_map[sp_uid].get('name_show', '')
                   or user_map[sp_uid].get('user_name', ''))
        if sp_name:
            return sp_uid, sp_name

    # 3. user_map 中没有或 nick_name 为空 → 从 author 字典兜底提取
    if isinstance(sp_author, dict) and sp_author:
        extracted = _extract_user_from_author_dict(sp_author)
        if extracted:
            # 回写到 user_map，供后续查找（包括 parse_content_blocks 的 @用户 解析）
            if extracted['user_id'] not in user_map:
                user_map[extracted['user_id']] = extracted
            elif not user_map[extracted['user_id']].get('nick_name'):
                # 已存在但 nick_name 为空，更新之
                user_map[extracted['user_id']].update(
                    {k: v for k, v in extracted.items() if v}
                )
            sp_name = extracted.get('nick_name', '') or extracted.get('user_name', '')
            if sp_name:
                return sp_uid or extracted['user_id'], sp_name

    return sp_uid, ''


def parse_content_blocks(content_blocks, user_map: dict = None) -> tuple:
    """
    解析客户端 API 返回的 content 结构化内容块
    返回 (纯文本, 图片列表)

    Args:
        content_blocks: 内容块列表或字符串
        user_map: {user_id_str: user_info_dict} 用于解析 @用户 时回填昵称
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
            text_parts.append('[语音]')
        elif block_type == '11':
            # @用户 —— 关键修复：text 可能为空，需要从 uid 反查用户名
            text = block.get('text', '')
            if text:
                text_parts.append(text)
            else:
                # text 为空时，尝试通过 uid 从 user_map 查找昵称
                uid = str(block.get('uid', ''))
                if uid and uid in user_map:
                    u = user_map[uid]
                    name = u.get('name_show', '') or u.get('nick_name', '') or u.get('name', '')
                    if name:
                        text_parts.append(f'@{name}')
                elif uid:
                    # user_map 中也没有，保留 uid 作为标识
                    text_parts.append(f'@用户{uid}')
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
        self._user_map = {}  # {uid_str: user_info_dict} 全局用户信息映射

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

        # thread.author 是完整字典，直接提取
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
        """解析客户端 API 返回的帖子列表"""
        comments = []
        thread = full_data.get('thread', {}) or full_data.get('data', {}).get('thread', {})
        thread_author_id = str(thread.get('author', {}).get('id', ''))

        # 构建 user_map：完整用户信息在顶层 user_list 中，
        # 每个 post 只有 author_id（纯数字），没有 author 字典
        # 跨页累积，而不是每页重建
        user_list = full_data.get('user_list', []) or full_data.get('data', {}).get('user_list', [])
        user_map = self._user_map  # 复用已有的，跨页累积
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

        # 也把 thread.author 加进来（它是完整的字典）
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

        # 存储到实例，供子评论解析时使用
        self._user_map = user_map

        for post in post_list:
            floor_num = safe_int(post.get('floor', 0))
            post_id = str(post.get('id', ''))

            # ★ 关键修复：post 中没有 author 字典，只有 author_id 字段
            uid = str(post.get('author_id', ''))

            # 从 user_map 查找完整用户信息
            if uid and uid in user_map:
                u_info = user_map[uid]
                user_name = u_info.get('user_name', '')
                nick_name = u_info.get('nick_name', '') or user_name
                level_id = u_info.get('level_id', 0)
            else:
                # ★ 兜底：尝试从 post 自身的 author 字典提取
                post_author = post.get('author', {})
                if isinstance(post_author, dict) and post_author:
                    extracted = _extract_user_from_author_dict(post_author)
                    if extracted and extracted.get('user_id'):
                        uid = uid or extracted['user_id']
                        user_name = extracted.get('user_name', '')
                        nick_name = extracted.get('nick_name', '') or user_name
                        level_id = extracted.get('level_id', 0)
                        # 回写到 user_map
                        user_map[uid] = extracted
                    else:
                        user_name = ''
                        nick_name = ''
                        level_id = 0
                else:
                    user_name = ''
                    nick_name = ''
                    level_id = 0

            user = {
                "user_id": uid,
                "user_name": user_name,
                "nick_name": nick_name,
                "level_id": level_id,
                "is_author": uid == thread_author_id and uid != '',
            }

            # 解析内容块（传入 user_map 以解析 @用户）
            content_blocks = post.get('content', [])
            text_content, images = parse_content_blocks(content_blocks, user_map)

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

            # 第1楼是主帖：存到 result 元数据，但不加入 comments 列表
            if floor_num == 1:
                self.result['content'] = text_content
                self.result['content_images'] = images
                self.result['create_time'] = post_time
                self.result['like_count'] = like_count
                self.result['author'] = user
                continue  # 跳过，不加入 comments

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

            # 客户端 API 可能已包含部分子评论
            sub_post_list = post.get('sub_post_list', {})
            if isinstance(sub_post_list, dict):
                sub_posts = sub_post_list.get('sub_post_list', [])
                if sub_posts:
                    for sp in sub_posts:
                        # ★ 修复：使用统一的用户解析函数
                        sp_uid, sp_name = _resolve_sub_comment_user(sp, user_map)

                        sp_content_blocks = sp.get('content', [])
                        sp_text, sp_images = parse_content_blocks(sp_content_blocks, user_map)
                        sub_comment = {
                            "user_name": sp_name,
                            "user_id": sp_uid,
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
        # 使用已有的 user_map，并在过程中扩展
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

            # 楼中楼 API 也可能有 user_list，合并到 user_map
            floor_user_list = data.get('user_list', []) or data.get('data', {}).get('user_list', [])
            for u in floor_user_list:
                if isinstance(u, dict):
                    uid = str(u.get('id', ''))
                    if uid:
                        # ★ 修复：不再跳过已存在的 uid，而是补充缺失字段
                        if uid not in user_map:
                            user_map[uid] = {
                                "user_id": uid,
                                "user_name": u.get('name', ''),
                                "name_show": u.get('name_show', '') or u.get('name', ''),
                                "nick_name": u.get('name_show', '') or u.get('name', ''),
                                "level_id": safe_int(u.get('level_id', 0)),
                            }
                        else:
                            # 已存在但可能 nick_name 为空，补充
                            existing = user_map[uid]
                            if not existing.get('nick_name') and not existing.get('name_show'):
                                new_name = u.get('name_show', '') or u.get('name', '')
                                if new_name:
                                    existing['nick_name'] = new_name
                                    existing['name_show'] = new_name
                                    if not existing.get('user_name'):
                                        existing['user_name'] = u.get('name', '')

            # 提取子评论列表
            subpost_list = data.get('subpost_list', [])
            if not subpost_list:
                subpost_list = data.get('data', {}).get('subpost_list', [])

            if not subpost_list:
                break

            for sp in subpost_list:
                # ★ 修复：使用统一的用户解析函数
                sp_uid, sp_name = _resolve_sub_comment_user(sp, user_map)

                sp_content_blocks = sp.get('content', [])
                sp_text, sp_images = parse_content_blocks(sp_content_blocks, user_map)

                sub_comment = {
                    "user_name": sp_name,
                    "user_id": sp_uid,
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
                    continue  # 【修复 #1】主楼不重复加入评论列表

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

        # 通过客户端 API 逐楼获取子评论
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
        """将结果保存为 JSON 文件"""
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