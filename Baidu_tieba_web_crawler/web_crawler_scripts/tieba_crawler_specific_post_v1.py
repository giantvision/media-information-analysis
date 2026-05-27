#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
百度贴吧帖子爬虫 (Baidu Tieba Post Scraper)
=============================================
功能：爬取指定贴吧帖子的完整数据，包括：
  - 帖子标题、正文内容
  - 评论数量、点赞数、分享/转发数
  - 所有楼层的用户评论（含楼中楼回复）
  - 图片链接（不下载图片，仅保存URL）
  - 用户信息（用户名、ID、等级等）

输出：结构化 JSON 文件

使用方式：
  python tieba_scraper.py <帖子URL或帖子ID>
  python tieba_scraper.py https://tieba.baidu.com/p/10515321107
  python tieba_scraper.py 10515321107
"""

import re
import json
import time
import sys
import logging
from typing import Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

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
# PC 端页面 URL 模板
PC_POST_URL = "https://tieba.baidu.com/p/{tid}?pn={pn}"
# 移动端 JSON API（备用方案）
MOBILE_API_URL = "https://tieba.baidu.com/mg/p/getPbData"
# 楼中楼（子评论）API
FLOOR_API_URL = "https://tieba.baidu.com/p/comment?tid={tid}&pid={pid}&pn={pn}&t={t}"
# 通用请求头
PC_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Referer": "https://tieba.baidu.com/",
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

# 请求间隔（秒）—— 尊重服务器，避免被封
REQUEST_DELAY = 1.5


# ============================================================
# 工具函数
# ============================================================
def extract_tid(url_or_id: str) -> str:
    """从 URL 或纯数字中提取帖子 ID (tid)"""
    url_or_id = url_or_id.strip()
    # 纯数字
    if url_or_id.isdigit():
        return url_or_id
    # URL 格式: https://tieba.baidu.com/p/10515321107?...
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
        # 处理 "1.2万" 这种格式
        val_str = str(val).strip()
        if '万' in val_str:
            return int(float(val_str.replace('万', '')) * 10000)
        if '亿' in val_str:
            return int(float(val_str.replace('亿', '')) * 100000000)
        return int(val_str)
    except (ValueError, TypeError):
        return default


# ============================================================
# 核心爬虫类
# ============================================================
class TiebaPostScraper:
    """百度贴吧帖子爬虫 - PC端HTML解析 + 移动端API双模式"""

    def __init__(self, tid: str, max_pages: int = 100, fetch_sub_comments: bool = True):
        """
        Args:
            tid: 帖子ID
            max_pages: 最大爬取页数（每页约30条评论）
            fetch_sub_comments: 是否爬取楼中楼子评论
        """
        self.tid = tid
        self.max_pages = max_pages
        self.fetch_sub_comments = fetch_sub_comments
        self.session = requests.Session()
        self.session.headers.update(PC_HEADERS)

        # 最终结果
        self.result = {
            "post_id": tid,
            "post_url": f"https://tieba.baidu.com/p/{tid}",
            "scrape_time": time.strftime("%Y-%m-%d %H:%M:%S"),
            "title": "",
            "forum_name": "",          # 所属贴吧名
            "author": {},              # 楼主信息
            "content": "",             # 主楼正文
            "content_images": [],      # 主楼图片链接
            "create_time": "",         # 发帖时间
            "total_replies": 0,        # 总回复数
            "total_pages": 0,          # 总页数
            "share_count": 0,          # 分享数
            "comments": [],            # 所有楼层评论
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
                    url,
                    params=params,
                    headers=headers or PC_HEADERS,
                    timeout=timeout,
                )
                resp.raise_for_status()
                return resp
            except requests.RequestException as e:
                logger.warning(f"请求失败 (第{attempt+1}次): {url} -> {e}")
                if attempt < 2:
                    time.sleep(REQUEST_DELAY * (attempt + 1))
        return None

    # ----------------------------------------------------------
    # PC 端 HTML 解析（主要方案）
    # ----------------------------------------------------------
    def _parse_page_html(self, html: str, page_num: int) -> list:
        """解析单页 HTML，提取楼层评论列表"""
        soup = BeautifulSoup(html, 'lxml')
        comments = []

        # ---- 第一页提取帖子元信息 ----
        if page_num == 1:
            self._extract_post_meta(soup, html)

        # ---- 提取每个楼层 ----
        # 贴吧每个楼层在 <div class="l_post ..."> 中
        # 关键数据在 data-field 属性中（JSON格式）
        post_list = soup.find_all('div', class_='l_post')
        if not post_list:
            # 尝试备用选择器
            post_list = soup.find_all('div', attrs={'data-field': True, 'class': re.compile(r'l_post')})

        for post_div in post_list:
            comment = self._parse_single_floor(post_div)
            if comment:
                comments.append(comment)

        return comments

    def _extract_post_meta(self, soup: BeautifulSoup, html: str):
        """从第一页提取帖子的标题、吧名、总回复数等元数据"""
        # 标题
        title_tag = soup.find('h1', class_='core_title_txt') or soup.find('h3', class_='core_title_txt')
        if title_tag:
            self.result['title'] = clean_text(title_tag.get_text())
        else:
            # 从 <title> 标签提取
            title_tag = soup.find('title')
            if title_tag:
                t = title_tag.get_text()
                # 格式通常为 "帖子标题_贴吧名_百度贴吧"
                parts = t.split('_')
                if len(parts) >= 1:
                    self.result['title'] = clean_text(parts[0])

        # 所属贴吧名
        forum_tag = soup.find('a', class_='card_title_fname')
        if forum_tag:
            self.result['forum_name'] = clean_text(forum_tag.get_text())
        else:
            # 备用：从 HTML 中正则提取
            match = re.search(r'forum_name\s*[:=]\s*["\']([^"\']+)', html)
            if match:
                self.result['forum_name'] = match.group(1)

        # 总回复数
        reply_num_tag = soup.find('span', class_='red', attrs={'title': True})
        if reply_num_tag:
            self.result['total_replies'] = safe_int(reply_num_tag.get_text())
        else:
            # 从 JSON 数据中提取
            match = re.search(r'"reply_num"\s*:\s*(\d+)', html)
            if match:
                self.result['total_replies'] = int(match.group(1))

        # 分享数
        share_tag = soup.find('span', class_='share_btn_tsn')
        if share_tag:
            self.result['share_count'] = safe_int(share_tag.get_text())
        else:
            match = re.search(r'"share_num"\s*:\s*(\d+)', html)
            if match:
                self.result['share_count'] = int(match.group(1))

        # 总页数
        pager = soup.find('li', class_='l_reply_num')
        if pager:
            spans = pager.find_all('span', class_='red')
            if len(spans) >= 2:
                self.result['total_replies'] = safe_int(spans[0].get_text())
                self.result['total_pages'] = safe_int(spans[1].get_text())
        if self.result['total_pages'] == 0:
            match = re.search(r'"total_page"\s*:\s*(\d+)', html)
            if match:
                self.result['total_pages'] = int(match.group(1))
            else:
                self.result['total_pages'] = 1

    def _parse_single_floor(self, post_div) -> Optional[dict]:
        """解析单个楼层 div，返回评论字典"""
        # 从 data-field 属性解析 JSON 元数据
        data_field_raw = post_div.get('data-field', '{}')
        try:
            data_field = json.loads(data_field_raw)
        except (json.JSONDecodeError, TypeError):
            data_field = {}

        # 基本信息
        author_info = data_field.get('author', {})
        content_info = data_field.get('content', {})

        floor_num = content_info.get('post_no', 0)
        post_id = content_info.get('post_id', 0)

        # 用户信息
        user = {
            "user_id": str(author_info.get('user_id', '')),
            "user_name": author_info.get('user_name', ''),
            "nick_name": author_info.get('user_nickname', '') or author_info.get('user_name', ''),
            "level_id": author_info.get('level_id', 0),
            "is_author": bool(author_info.get('is_author', False)),  # 是否楼主
        }

        # 内容
        content_div = post_div.find('div', class_='d_post_content')
        if not content_div:
            content_div = post_div.find('div', id=re.compile(r'post_content_\d+'))

        text_content = ""
        images = []

        if content_div:
            # 提取图片链接
            for img in content_div.find_all('img'):
                src = img.get('src', '') or img.get('data-src', '') or img.get('original', '')
                if src and 'static' not in src and 'emoticon' not in src:
                    if not src.startswith('http'):
                        src = 'https:' + src if src.startswith('//') else src
                    images.append(src)
                # 贴吧表情包图片（class=BDE_Smiley）跳过
                if img.get('class') and 'BDE_Smiley' in ' '.join(img.get('class', [])):
                    continue

            # 提取纯文本
            text_content = clean_text(content_div.get_text(separator='\n'))

        # 发帖时间
        post_time = ""
        time_tag = post_div.find('span', class_='tail-info', string=re.compile(r'\d{4}-\d{2}-\d{2}'))
        if time_tag:
            post_time = clean_text(time_tag.get_text())
        else:
            # 从 data-field 提取
            date_val = content_info.get('date', '')
            if date_val:
                post_time = str(date_val)

        # 点赞数
        like_count = 0
        # 方式1: div.d_post_like > span.d_suite_value
        like_tag = post_div.find('div', class_='d_post_like') or post_div.find('span', class_='d_post_like')
        if like_tag:
            like_num = like_tag.find('span', class_='d_suite_value')
            if like_num:
                like_count = safe_int(like_num.get_text())
            elif like_tag.get_text(strip=True).isdigit():
                like_count = safe_int(like_tag.get_text(strip=True))
        # 方式2: 直接从 data-field 解析
        if like_count == 0:
            agree_info = content_info.get('agree', {})
            if isinstance(agree_info, dict):
                like_count = safe_int(agree_info.get('agree_num', 0))

        # 楼中楼（子评论）数量
        sub_comment_count = 0
        sub_comment_tag = post_div.find('a', class_='lzl_link_unfold')
        if sub_comment_tag:
            num_match = re.search(r'(\d+)', sub_comment_tag.get_text())
            if num_match:
                sub_comment_count = int(num_match.group(1))

        comment = {
            "floor": floor_num,
            "post_id": str(post_id),
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
            self.result['author'] = user

        return comment

    # ----------------------------------------------------------
    # 楼中楼子评论爬取
    # ----------------------------------------------------------
    def _fetch_sub_comments(self, tid: str, pid: str, total: int) -> list:
        """爬取指定楼层的楼中楼子评论"""
        sub_comments = []
        page = 1
        max_sub_pages = (total // 10) + 2  # 每页约10条

        while page <= max_sub_pages:
            url = FLOOR_API_URL.format(tid=tid, pid=pid, pn=page, t=int(time.time() * 1000))
            resp = self._get(url)
            if not resp:
                break

            soup = BeautifulSoup(resp.text, 'lxml')
            items = soup.find_all('li', class_='lzl_single_post')
            if not items:
                break

            for item in items:
                user_tag = item.find('a', class_='at')
                content_tag = item.find('span', class_='lzl_content_main')
                time_tag = item.find('span', class_='lzl_time')

                sub_comment = {
                    "user_name": clean_text(user_tag.get_text()) if user_tag else "",
                    "user_link": user_tag.get('href', '') if user_tag else "",
                    "content": clean_text(content_tag.get_text()) if content_tag else "",
                    "time": clean_text(time_tag.get_text()) if time_tag else "",
                }

                # 提取楼中楼图片
                if content_tag:
                    imgs = content_tag.find_all('img')
                    sub_imgs = []
                    for img in imgs:
                        src = img.get('src', '')
                        if src and 'emoticon' not in src:
                            sub_imgs.append(src)
                    if sub_imgs:
                        sub_comment['images'] = sub_imgs

                sub_comments.append(sub_comment)

            page += 1
            time.sleep(REQUEST_DELAY * 0.5)

        return sub_comments

    # ----------------------------------------------------------
    # 移动端 API 解析（备用方案）
    # ----------------------------------------------------------
    def _scrape_via_mobile_api(self) -> bool:
        """通过移动端 API 获取帖子数据（返回 JSON）"""
        logger.info("尝试移动端 API 方式...")
        params = {
            "kz": self.tid,
            "obj_param2": "chrome",
            "format": "json",
            "pn": 1,
            "rn": 30,
        }
        resp = self._get(MOBILE_API_URL, params=params, headers=MOBILE_HEADERS)
        if not resp:
            return False

        try:
            data = resp.json()
        except json.JSONDecodeError:
            logger.warning("移动端 API 返回非 JSON 数据")
            return False

        if data.get('errno', -1) != 0:
            logger.warning(f"移动端 API 返回错误: {data.get('errmsg', 'unknown')}")
            return False

        # 解析数据
        forum_data = data.get('data', {}).get('forum', {})
        thread_data = data.get('data', {}).get('thread', {})
        post_list = data.get('data', {}).get('post_list', [])

        self.result['title'] = thread_data.get('title', '')
        self.result['forum_name'] = forum_data.get('name', '')
        self.result['total_replies'] = safe_int(thread_data.get('reply_num', 0))
        self.result['share_count'] = safe_int(thread_data.get('share_num', 0))

        # 解析每个楼层
        for post in post_list:
            author = post.get('author', {})
            floor_num = safe_int(post.get('floor', 0))

            # 提取内容和图片
            content_parts = post.get('content', [])
            text_parts = []
            images = []
            for part in content_parts:
                if isinstance(part, dict):
                    ptype = part.get('type', '')
                    if ptype == '0':  # 文字
                        text_parts.append(part.get('text', ''))
                    elif ptype == '3':  # 图片
                        img_url = part.get('origin_src', '') or part.get('cdn_src', '') or part.get('src', '')
                        if img_url:
                            images.append(img_url)
                    elif ptype == '4':  # 表情
                        pass
                    elif ptype == '9':  # 视频
                        video_url = part.get('link', '')
                        if video_url:
                            text_parts.append(f'[视频: {video_url}]')

            content = '\n'.join(text_parts).strip()

            comment = {
                "floor": floor_num,
                "post_id": str(post.get('id', '')),
                "user": {
                    "user_id": str(author.get('id', '')),
                    "user_name": author.get('name', ''),
                    "nick_name": author.get('name_show', '') or author.get('name', ''),
                    "level_id": safe_int(author.get('level_id', 0)),
                    "is_author": author.get('id') == thread_data.get('author', {}).get('id'),
                },
                "content": content,
                "images": images,
                "post_time": post.get('time', ''),
                "like_count": safe_int(post.get('agree', {}).get('agree_num', 0)),
                "sub_comment_count": safe_int(post.get('sub_post_number', 0)),
                "sub_comments": [],
            }

            if floor_num == 1:
                self.result['content'] = content
                self.result['content_images'] = images
                self.result['create_time'] = post.get('time', '')
                self.result['author'] = comment['user']

            self.result['comments'].append(comment)

        return True

    # ----------------------------------------------------------
    # PC 端 HTML 翻页爬取（主要方案）
    # ----------------------------------------------------------
    def _scrape_via_pc_html(self) -> bool:
        """通过 PC 端 HTML 页面逐页爬取"""
        logger.info("使用 PC 端 HTML 解析方式...")

        page_num = 1
        while page_num <= self.max_pages:
            url = PC_POST_URL.format(tid=self.tid, pn=page_num)
            logger.info(f"正在爬取第 {page_num} 页: {url}")

            resp = self._get(url)
            if not resp:
                logger.error(f"第 {page_num} 页请求失败，停止爬取")
                break

            html = resp.text

            # 检查是否被重定向到登录页或其他页面
            if '贴吧' not in html and 'PageData' not in html:
                logger.warning("页面内容异常，可能被反爬或帖子不存在")
                if page_num == 1:
                    return False
                break

            # 解析当前页
            page_comments = self._parse_page_html(html, page_num)

            if not page_comments:
                if page_num == 1:
                    logger.warning("第一页未找到评论，尝试备用方案")
                    return False
                logger.info(f"第 {page_num} 页无更多评论，爬取结束")
                break

            self.result['comments'].extend(page_comments)
            logger.info(f"  -> 本页获取 {len(page_comments)} 条评论")

            # 检查是否已到最后一页
            total_pages = self.result.get('total_pages', 1)
            if page_num >= total_pages:
                logger.info("已到达最后一页")
                break

            page_num += 1
            time.sleep(REQUEST_DELAY)

        return len(self.result['comments']) > 0

    # ----------------------------------------------------------
    # 主入口
    # ----------------------------------------------------------
    def scrape(self) -> dict:
        """执行爬取，返回结构化数据"""
        logger.info(f"=" * 60)
        logger.info(f"开始爬取帖子: {self.tid}")
        logger.info(f"=" * 60)

        # 方案一：PC 端 HTML 解析
        success = self._scrape_via_pc_html()

        # 方案二：如果PC端失败，尝试移动端 API
        if not success:
            logger.info("PC端方式失败，切换到移动端 API...")
            success = self._scrape_via_mobile_api()

        if not success:
            logger.error("所有方式均失败，请检查帖子ID是否正确或网络状况")
            return self.result

        # 爬取楼中楼子评论
        if self.fetch_sub_comments:
            self._scrape_all_sub_comments()

        # 统计汇总
        total_comments = len(self.result['comments'])
        total_sub = sum(len(c.get('sub_comments', [])) for c in self.result['comments'])

        logger.info(f"\n{'=' * 60}")
        logger.info(f"爬取完成!")
        logger.info(f"  帖子标题: {self.result['title']}")
        logger.info(f"  所属贴吧: {self.result['forum_name']}")
        logger.info(f"  总回复数: {self.result['total_replies']}")
        logger.info(f"  爬取楼层: {total_comments}")
        logger.info(f"  楼中楼回复: {total_sub}")
        logger.info(f"{'=' * 60}")

        return self.result

    def _scrape_all_sub_comments(self):
        """遍历所有楼层，爬取有子评论的楼中楼"""
        floors_with_sub = [
            c for c in self.result['comments']
            if c.get('sub_comment_count', 0) > 0
        ]
        if not floors_with_sub:
            return

        logger.info(f"正在爬取 {len(floors_with_sub)} 个楼层的楼中楼评论...")
        for i, comment in enumerate(floors_with_sub):
            pid = comment.get('post_id', '')
            count = comment.get('sub_comment_count', 0)
            if pid and count > 0:
                logger.info(f"  [{i+1}/{len(floors_with_sub)}] "
                            f"第{comment['floor']}楼 ({count}条子评论)")
                sub = self._fetch_sub_comments(self.tid, pid, count)
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
    # 默认帖子 URL（用户指定的示例）
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

    # 创建爬虫并执行
    scraper = TiebaPostScraper(
        tid=tid,
        max_pages=50,           # 最多爬50页
        fetch_sub_comments=True  # 爬取楼中楼
    )

    result = scraper.scrape()

    # 保存结果
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
            print(f"    [{c['floor']}楼] {nick}: {text}")
    print("=" * 60)

    return output_file


if __name__ == '__main__':
    main()