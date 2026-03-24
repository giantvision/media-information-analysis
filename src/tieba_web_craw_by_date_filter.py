#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
百度贴吧爬虫 v5.2 - 断点续爬 + 实时存储 + 时间范围过滤版
=========================================================
v5.2 新增:
  ✅ 时间范围过滤 — 仅将 create_time 落在 [START_DATE_TIME, END_DATE_TIME] 内的帖子
     写入 JSONL，爬取主流程不变，打印信息同步展示过滤统计

使用方法: 按 Cell 顺序在 Colab 中运行
"""


# ============================================================================
# ===== CELL 1: 安装依赖 =====
# ============================================================================
# !pip install requests pandas -q


# ============================================================================
# ===== CELL 2: 配置参数 =====
# ============================================================================

TIEBA_NAME = "三国志战略版"     # 🎯 贴吧名称

# ──────── 爬取模式(三选一) ────────
SCRAPE_MODE = "pages"
SCRAPE_PAGES = 200              # 爬取页数

# SCRAPE_MODE = "hours"
# SCRAPE_HOURS = 24

# SCRAPE_MODE = "days"
# SCRAPE_DAYS = 7

# ──────── 时间范围过滤 ────────
# 仅保存 create_time 在此范围内的帖子 (含两端)
# 留空字符串 "" 表示不设该端限制
START_DATE_TIME = "2026-03-01"        # 起始时间，支持 "YYYY-MM-DD" 或 "YYYY-MM-DD HH:MM:SS"
END_DATE_TIME   = "2026-03-24"        # 结束时间，同上

# ──────── 高级配置 ────────
INCLUDE_TOP = False
MAX_SCAN_PAGES = 10000
DELAY_RANGE = (0.8, 1.8)
SORT_TYPE = 0              # 0 / 1 / "both"

SAVE_TO_DRIVE = True
OUTPUT_FILENAME = "tieba_sgz_mode-0_pages-2_20260324_run-12"

FORCE_RESTART = False

print(f"✅ 配置完成: [{TIEBA_NAME}吧] | 模式={SCRAPE_MODE} | 排序={SORT_TYPE}")
if START_DATE_TIME or END_DATE_TIME:
    print(f"   时间过滤: {START_DATE_TIME or '不限'} ~ {END_DATE_TIME or '不限'}")


# ============================================================================
# ===== CELL 3: 爬虫核心代码 =====
# ============================================================================

import hashlib
import json
import os
import random
import re
import time as _time
from collections import Counter
from datetime import datetime, timedelta
from typing import Optional
from urllib.parse import quote

import requests

# ---------------------------------------------------------------------------
# 签名 & 常量
# ---------------------------------------------------------------------------

SIGN_KEY = "tiebaclient!!!"
CLIENT_VERSION = "12.67.1.0"
API_ENDPOINTS = [
    "https://tieba.baidu.com/c/f/frs/page",
    "http://c.tieba.baidu.com/c/f/frs/page",
]

def calc_sign(params: dict) -> str:
    raw = "".join(f"{k}={params[k]}" for k in sorted(params.keys()))
    return hashlib.md5((raw + SIGN_KEY).encode("utf-8")).hexdigest().upper()


# ---------------------------------------------------------------------------
# 时间解析
# ---------------------------------------------------------------------------

def normalize_time(time_str) -> str:
    if isinstance(time_str, (int, float)):
        if time_str > 1_000_000_000:
            try:
                return datetime.fromtimestamp(int(time_str)).strftime("%Y-%m-%d %H:%M:%S")
            except (ValueError, OSError):
                pass
        return str(time_str)
    now = datetime.now()
    s = str(time_str).strip()
    if not s:
        return ""
    if s.isdigit() and len(s) >= 9:
        try:
            return datetime.fromtimestamp(int(s)).strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, OSError):
            pass
    m = re.match(r"(\d+)\s*分钟前", s)
    if m: return (now - timedelta(minutes=int(m.group(1)))).strftime("%Y-%m-%d %H:%M")
    m = re.match(r"(\d+)\s*小时前", s)
    if m: return (now - timedelta(hours=int(m.group(1)))).strftime("%Y-%m-%d %H:%M")
    if "刚刚" in s: return now.strftime("%Y-%m-%d %H:%M")
    m = re.match(r"(\d{4})-(\d{1,2})-(\d{1,2})\s+(\d{1,2}):(\d{2})", s)
    if m: return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d} {int(m.group(4)):02d}:{m.group(5)}"
    m = re.match(r"^(\d{1,2}):(\d{2})$", s)
    if m: return f"{now.strftime('%Y-%m-%d')} {int(m.group(1)):02d}:{m.group(2)}"
    m = re.match(r"昨天\s*(\d{1,2}):(\d{2})", s)
    if m:
        y = now - timedelta(days=1)
        return f"{y.strftime('%Y-%m-%d')} {int(m.group(1)):02d}:{m.group(2)}"
    m = re.match(r"^(\d{1,2})-(\d{1,2})$", s)
    if m:
        month, day = int(m.group(1)), int(m.group(2))
        year = now.year
        if month > now.month or (month == now.month and day > now.day): year -= 1
        return f"{year}-{month:02d}-{day:02d}"
    m = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})$", s)
    if m: return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    m = re.match(r"^(\d{4})-(\d{1,2})$", s)
    if m: return f"{m.group(1)}-{int(m.group(2)):02d}-01"
    return s


# ---------------------------------------------------------------------------
# [v5.2 新增] 时间范围过滤
# ---------------------------------------------------------------------------

def _parse_boundary(date_str: str) -> Optional[datetime]:
    """将用户配置的时间边界字符串解析为 datetime，支持多种格式"""
    if not date_str:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    return None


def _parse_create_time(ct_str: str) -> Optional[datetime]:
    """将帖子的 create_time 字段解析为 datetime"""
    if not ct_str:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(ct_str.strip(), fmt)
        except ValueError:
            continue
    return None


def build_time_filter(start_str: str, end_str: str):
    """
    根据配置构建过滤函数。
    返回: (filter_func, start_dt, end_dt)
      - filter_func(thread) -> bool : True 表示在范围内，应保留
      - start_dt / end_dt: 解析后的边界 (可能为 None)

    当 END_DATE_TIME 只到日期级别 (如 "2026-03-24") 时，自动视为该天结束
    即 2026-03-24 23:59:59，让当天的帖子全部包含在内。
    """
    start_dt = _parse_boundary(start_str)
    end_dt = _parse_boundary(end_str)

    # 若 end 只精确到日期，补齐为当天 23:59:59
    if end_dt and end_dt.hour == 0 and end_dt.minute == 0 and end_dt.second == 0:
        # 只有当原始字符串确实只有日期时才补齐
        if re.match(r"^\d{4}-\d{1,2}-\d{1,2}$", end_str.strip()):
            end_dt = end_dt.replace(hour=23, minute=59, second=59)

    if start_dt is None and end_dt is None:
        # 无过滤
        return (lambda t: True), None, None

    def _in_range(thread: dict) -> bool:
        ct = _parse_create_time(thread.get("create_time", ""))
        if ct is None:
            # create_time 无法解析时默认保留（避免丢数据）
            return True
        if start_dt and ct < start_dt:
            return False
        if end_dt and ct > end_dt:
            return False
        return True

    return _in_range, start_dt, end_dt


# 在模块加载时就构建好过滤器，供后续使用
TIME_FILTER, _FILTER_START, _FILTER_END = build_time_filter(START_DATE_TIME, END_DATE_TIME)
_HAS_TIME_FILTER = (_FILTER_START is not None) or (_FILTER_END is not None)


# ---------------------------------------------------------------------------
# API 请求 & 解析
# ---------------------------------------------------------------------------

def api_fetch(kw: str, pn: int, rn: int = 30, sort_type: int = 0) -> Optional[list[dict]]:
    params = {
        "_client_version": CLIENT_VERSION,
        "kw": kw,
        "pn": str(pn),
        "rn": str(rn),
        "sort_type": str(sort_type),
    }
    params["sign"] = calc_sign(params)
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": f"bdtb for Android {CLIENT_VERSION}",
        "Accept-Encoding": "gzip",
        "Connection": "keep-alive",
    }
    for endpoint in API_ENDPOINTS:
        try:
            resp = requests.post(endpoint, data=params, headers=headers, timeout=15)
            if resp.status_code != 200:
                continue
            data = resp.json()
            if str(data.get("error_code", "0")) != "0":
                continue
            thread_list = data.get("thread_list") or []
            if not thread_list:
                return []
            return parse_threads(thread_list)
        except (requests.RequestException, json.JSONDecodeError, KeyError):
            continue
    return None


def parse_threads(thread_list: list) -> list[dict]:
    threads = []
    for item in thread_list:
        if not isinstance(item, dict):
            continue
        tid = int(item.get("id", 0) or item.get("tid", 0) or 0)
        title = str(item.get("title", "") or "")
        if not title:
            continue

        abstract = ""
        abs_data = item.get("abstract") or item.get("first_post_content") or ""
        if isinstance(abs_data, list):
            abstract = "".join(str(a.get("text", "")) for a in abs_data if isinstance(a, dict))
        elif isinstance(abs_data, str):
            abstract = abs_data

        author_data = item.get("author") or {}
        author_name = ""
        if isinstance(author_data, dict):
            author_name = str(author_data.get("name_show", "") or author_data.get("name", "") or "")

        create_time_raw = item.get("create_time") or ""
        create_time = normalize_time(create_time_raw)

        last_time_raw = item.get("last_time_int") or ""
        last_time = normalize_time(last_time_raw)

        last_replyer = item.get("last_replyer") or {}
        last_reply_name = ""
        if isinstance(last_replyer, dict):
            last_reply_name = str(last_replyer.get("name_show", "") or last_replyer.get("name", "") or "")

        agree_data = item.get("agree") or {}
        if isinstance(agree_data, dict):
            agree_num = int(agree_data.get("agree_num", 0) or 0)
        else:
            agree_num = int(item.get("agree_num", 0) or 0)

        share_num = int(item.get("share_num", 0) or 0)

        threads.append({
            "thread_id": tid,
            "title": title,
            "abstract": abstract,
            "reply_count": int(item.get("reply_num", 0) or 0),
            "agree_num": agree_num,
            "share_num": share_num,
            "is_top": bool(item.get("is_top", 0)),
            "url": f"https://tieba.baidu.com/p/{tid}" if tid else "",
            "author": {"name": author_name, "name_id": author_name},
            "create_time": create_time,
            "create_time_raw": str(create_time_raw),
            "last_reply": {
                "author": last_reply_name,
                "time": last_time,
                "time_raw": str(last_time_raw),
            },
            "crawl_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })
    return threads


# ---------------------------------------------------------------------------
# Checkpoint 断点管理
# ---------------------------------------------------------------------------

class CheckpointManager:
    def __init__(self, base_path: str, kw: str):
        self.ckpt_path = f"{base_path}_checkpoint.json"
        self.data_path = f"{base_path}.jsonl"
        self.kw = kw
        self.state = None

    def load(self) -> Optional[dict]:
        if not os.path.exists(self.ckpt_path):
            return None
        try:
            with open(self.ckpt_path, "r", encoding="utf-8") as f:
                state = json.load(f)
            if state.get("kw") != self.kw:
                print(f"  ⚠️  Checkpoint 贴吧名不匹配 ({state.get('kw')} ≠ {self.kw})，忽略")
                return None
            self.state = state
            return state
        except (json.JSONDecodeError, KeyError) as e:
            print(f"  ⚠️  Checkpoint 文件损坏: {e}")
            return None

    def save(self, seen_ids: set, progress: dict, total_collected: int, total_filtered: int = 0):
        state = {
            "kw": self.kw,
            "seen_ids": list(seen_ids),
            "progress": progress,
            "total_collected": total_collected,
            "total_filtered": total_filtered,
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        tmp_path = self.ckpt_path + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False)
        os.replace(tmp_path, self.ckpt_path)

    def load_existing_data(self) -> tuple[list[dict], set]:
        threads = []
        seen = set()
        if not os.path.exists(self.data_path):
            return threads, seen
        try:
            with open(self.data_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        t = json.loads(line)
                        tid = t.get("thread_id", 0)
                        if tid and tid not in seen:
                            threads.append(t)
                            seen.add(tid)
                    except json.JSONDecodeError:
                        continue
        except Exception as e:
            print(f"  ⚠️  读取已有数据异常: {e}")
        return threads, seen

    def append_threads(self, threads: list[dict]):
        if not threads:
            return
        with open(self.data_path, "a", encoding="utf-8") as f:
            for t in threads:
                f.write(json.dumps(t, ensure_ascii=False) + "\n")

    def clear(self):
        for p in [self.ckpt_path, self.data_path]:
            if os.path.exists(p):
                os.remove(p)


# ---------------------------------------------------------------------------
# 核心爬虫 (带断点续爬 + 时间范围过滤)
# ---------------------------------------------------------------------------

class TiebaScraper:
    def __init__(self, kw, delay=(0.8, 1.8), sort_type=0, ckpt_mgr: Optional[CheckpointManager] = None):
        self.kw = kw
        self.delay = delay
        self.sort_type = sort_type
        self.ckpt = ckpt_mgr
        self._fails = 0

    def _fetch(self, pn: int, sort_type: int) -> Optional[list[dict]]:
        result = api_fetch(self.kw, pn=pn, sort_type=sort_type)
        if result is None:
            self._fails += 1
        else:
            self._fails = 0
        return result

    def scrape_by_pages(self, num_pages=1, include_top=False) -> list[dict]:
        sort_modes = [0, 1] if self.sort_type == "both" else [int(self.sort_type)]
        sort_names = {0: "回复时间", 1: "发帖时间"}

        all_threads, seen = [], set()
        filtered_count = 0          # [v5.2] 被时间过滤掉的帖子计数
        progress = {}

        if self.ckpt:
            state = self.ckpt.load()
            if state and not FORCE_RESTART:
                existing_threads, seen = self.ckpt.load_existing_data()
                all_threads = existing_threads
                progress = state.get("progress", {})
                filtered_count = state.get("total_filtered", 0)
                print(f"  🔖 从断点恢复: 已有 {len(all_threads)} 条 (范围外已过滤 {filtered_count} 条), seen={len(seen)}")
                print(f"     进度: {progress}")
            elif FORCE_RESTART and state:
                print(f"  🔄 强制重新开始 (忽略 checkpoint)")
                self.ckpt.clear()

        for sort in sort_modes:
            sort_key = str(sort)
            start_pn = progress.get(sort_key, 0) + 1
            stale_count = 0

            if start_pn > num_pages:
                print(f"\n  ✅ 排序{sort_names[sort]}已在之前完成 (pn={start_pn-1})")
                continue

            if len(sort_modes) > 1:
                print(f"\n  🔄 排序模式: {sort_names[sort]} (sort_type={sort})")
                if start_pn > 1:
                    print(f"     从 pn={start_pn} 续爬 (已跳过 {start_pn-1} 页)")
                print(f"  {'─'*50}")

            for pn in range(start_pn, num_pages + 1):
                result = self._fetch(pn, sort_type=sort)

                if result is None:
                    if self._fails >= 3:
                        print(f"  🛑 连续失败≥3次，终止")
                        break
                    print(f"  ⚠️  pn={pn} 请求失败")
                    continue

                if len(result) == 0:
                    print(f"  📭 pn={pn} 返回空，已到底")
                    progress[sort_key] = num_pages
                    if self.ckpt:
                        self.ckpt.save(seen, progress, len(all_threads), filtered_count)
                    break

                added, duped, topped, out_of_range = 0, 0, 0, 0
                new_threads = []
                for t in result:
                    if t["thread_id"] in seen:
                        duped += 1
                        continue
                    seen.add(t["thread_id"])
                    if not include_top and t["is_top"]:
                        topped += 1
                        continue
                    # [v5.2] 时间范围过滤
                    if _HAS_TIME_FILTER and not TIME_FILTER(t):
                        out_of_range += 1
                        filtered_count += 1
                        continue
                    all_threads.append(t)
                    new_threads.append(t)
                    added += 1

                # [v5.2] 打印信息加入 "范围外" 字段
                extra = f", 范围外:{out_of_range}" if _HAS_TIME_FILTER else ""
                print(
                    f"  📡 pn={pn:>4} → +{added:>3}条 "
                    f"(重复:{duped:>2}, 置顶:{topped}{extra}) "
                    f"| 范围内累计 {len(all_threads)}"
                )

                if self.ckpt and new_threads:
                    self.ckpt.append_threads(new_threads)
                progress[sort_key] = pn
                if self.ckpt:
                    self.ckpt.save(seen, progress, len(all_threads), filtered_count)

                if added == 0:
                    stale_count += 1
                    if stale_count >= 5:
                        print(f"  ⏹️ 连续{stale_count}页无新增，该排序已到底")
                        progress[sort_key] = num_pages
                        if self.ckpt:
                            self.ckpt.save(seen, progress, len(all_threads), filtered_count)
                        break
                else:
                    stale_count = 0

                _time.sleep(random.uniform(*self.delay))

        # [v5.2] 完成时打印过滤汇总
        if _HAS_TIME_FILTER:
            print(f"\n  🕐 时间过滤汇总: 范围内保留 {len(all_threads)} 条, 范围外过滤 {filtered_count} 条")

        return all_threads

    def scrape_by_time(self, hours=0, days=0, max_scan_pages=100, include_top=False) -> list[dict]:
        cutoff = datetime.now() - timedelta(hours=hours, days=days)
        print(f"  ⏰ 截止: {cutoff.strftime('%Y-%m-%d %H:%M')}")

        sort_modes = [0, 1] if self.sort_type == "both" else [int(self.sort_type)]
        sort_names = {0: "回复时间", 1: "发帖时间"}

        all_threads, seen = [], set()
        filtered_count = 0
        progress = {}

        if self.ckpt:
            state = self.ckpt.load()
            if state and not FORCE_RESTART:
                existing, seen = self.ckpt.load_existing_data()
                all_threads = existing
                progress = state.get("progress", {})
                filtered_count = state.get("total_filtered", 0)
                print(f"  🔖 从断点恢复: 已有 {len(all_threads)} 条 (范围外已过滤 {filtered_count} 条)")
            elif FORCE_RESTART and state:
                self.ckpt.clear()

        for sort in sort_modes:
            sort_key = str(sort)
            start_pn = progress.get(sort_key, 0) + 1
            stale_count = 0
            should_stop = False

            if len(sort_modes) > 1:
                print(f"\n  🔄 排序模式: {sort_names[sort]}")
                if start_pn > 1:
                    print(f"     从 pn={start_pn} 续爬")

            for pn in range(start_pn, max_scan_pages + 1):
                result = self._fetch(pn, sort_type=sort)
                if result is None:
                    if self._fails >= 3:
                        break
                    continue
                if len(result) == 0:
                    progress[sort_key] = max_scan_pages
                    if self.ckpt:
                        self.ckpt.save(seen, progress, len(all_threads), filtered_count)
                    break

                added, duped, expired, out_of_range = 0, 0, 0, 0
                new_threads = []
                for t in result:
                    tid = t["thread_id"]
                    if tid in seen:
                        duped += 1; continue
                    seen.add(tid)
                    if not include_top and t["is_top"]:
                        continue
                    rt = t["last_reply"]["time"]
                    if rt:
                        try:
                            dt = None
                            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
                                try:
                                    dt = datetime.strptime(rt, fmt); break
                                except ValueError:
                                    continue
                            if dt and dt < cutoff:
                                should_stop = True; expired += 1; continue
                        except Exception:
                            pass
                    # [v5.2] 时间范围过滤
                    if _HAS_TIME_FILTER and not TIME_FILTER(t):
                        out_of_range += 1
                        filtered_count += 1
                        continue
                    all_threads.append(t)
                    new_threads.append(t)
                    added += 1

                extra = f", 范围外:{out_of_range}" if _HAS_TIME_FILTER else ""
                print(
                    f"  📡 pn={pn:>4} → +{added:>3}条 "
                    f"(重复:{duped:>2}, 过期:{expired}{extra}) "
                    f"| 范围内累计 {len(all_threads)}"
                )

                if self.ckpt and new_threads:
                    self.ckpt.append_threads(new_threads)
                progress[sort_key] = pn
                if self.ckpt:
                    self.ckpt.save(seen, progress, len(all_threads), filtered_count)

                if should_stop:
                    print(f"  ⏹️ 到达时间截止范围"); break
                if added == 0:
                    stale_count += 1
                    if stale_count >= 5:
                        print(f"  ⏹️ 连续{stale_count}页无新增"); break
                else:
                    stale_count = 0
                _time.sleep(random.uniform(*self.delay))

        if _HAS_TIME_FILTER:
            print(f"\n  🕐 时间过滤汇总: 范围内保留 {len(all_threads)} 条, 范围外过滤 {filtered_count} 条")

        return all_threads


print("✅ 爬虫核心代码加载完成 (v5.2 时间范围过滤版)")


# ============================================================================
# ===== CELL 4: 执行爬取 =====
# ============================================================================

# --- 初始化路径 ---
if SAVE_TO_DRIVE:
    try:
        from google.colab import drive
        drive.mount("/content/drive")
        base_path = f"/content/drive/MyDrive/web_crawl_data/tieba_sgz/{OUTPUT_FILENAME}"
    except ImportError:
        base_path = OUTPUT_FILENAME
else:
    base_path = OUTPUT_FILENAME

# --- 初始化 checkpoint ---
ckpt = CheckpointManager(base_path=base_path, kw=TIEBA_NAME)

# --- 显示已有状态 ---
existing_state = ckpt.load() if not FORCE_RESTART else None
if existing_state:
    print(f"📂 发现断点记录:")
    print(f"   已爬帖子: {existing_state['total_collected']} 条")
    if existing_state.get("total_filtered"):
        print(f"   范围外过滤: {existing_state['total_filtered']} 条")
    print(f"   进度: {existing_state['progress']}")
    print(f"   更新于: {existing_state['updated_at']}")
    print(f"   → 将从断点处继续爬取\n")
else:
    if FORCE_RESTART:
        ckpt.clear()
        print(f"🔄 强制重新开始\n")
    else:
        print(f"📂 未发现断点，从头开始爬取\n")

print(f"🚀 开始爬取 [{TIEBA_NAME}吧]")
sort_desc = {0: "按回复时间", 1: "按发帖时间", "both": "双模式合并"}
print(f"   排序: {sort_desc.get(SORT_TYPE, SORT_TYPE)} | 含置顶: {INCLUDE_TOP}")
if _HAS_TIME_FILTER:
    s = _FILTER_START.strftime('%Y-%m-%d %H:%M:%S') if _FILTER_START else "不限"
    e = _FILTER_END.strftime('%Y-%m-%d %H:%M:%S') if _FILTER_END else "不限"
    print(f"   🕐 时间过滤: {s} ~ {e}")
print(f"   数据文件: {base_path}.jsonl")
print(f"   断点文件: {base_path}_checkpoint.json")
print("─" * 55)

scraper = TiebaScraper(kw=TIEBA_NAME, delay=DELAY_RANGE, sort_type=SORT_TYPE, ckpt_mgr=ckpt)

if SCRAPE_MODE == "pages":
    threads = scraper.scrape_by_pages(num_pages=SCRAPE_PAGES, include_top=INCLUDE_TOP)
elif SCRAPE_MODE == "hours":
    threads = scraper.scrape_by_time(hours=SCRAPE_HOURS, max_scan_pages=MAX_SCAN_PAGES, include_top=INCLUDE_TOP)
elif SCRAPE_MODE == "days":
    threads = scraper.scrape_by_time(days=SCRAPE_DAYS, max_scan_pages=MAX_SCAN_PAGES, include_top=INCLUDE_TOP)
else:
    threads = []

print("─" * 55)
print(f"🎉 完成！范围内共 {len(threads)} 条帖子")
print(f"   数据已实时保存到: {base_path}.jsonl")


# ============================================================================
# ===== CELL 5: 查看结果 =====
# ============================================================================

import pandas as pd

if threads:
    flat = [{
        "标题": t["title"],
        "回复数": t["reply_count"],
        "作者": t["author"]["name"],
        "发帖时间": t.get("create_time", ""),
        "最后回复时间": t["last_reply"]["time"],
        "最后回复者": t["last_reply"]["author"],
        "摘要": (t["abstract"][:50] + "...") if len(t["abstract"]) > 50 else t["abstract"],
        "帖子链接": t["url"],
    } for t in threads]
    df = pd.DataFrame(flat)
    range_label = ""
    if _HAS_TIME_FILTER:
        s = _FILTER_START.strftime('%Y-%m-%d') if _FILTER_START else "∞"
        e = _FILTER_END.strftime('%Y-%m-%d') if _FILTER_END else "∞"
        range_label = f" (时间范围: {s} ~ {e})"
    print(f"\n📊 共 {len(df)} 条帖子{range_label}:\n")
    try:
        display(df)
    except NameError:
        print(df.head(20).to_string(index=False))
else:
    print("⚠️ 未获取到数据 (指定时间范围内无匹配帖子)")


# ============================================================================
# ===== CELL 6: 数据分析 =====
# ============================================================================

if threads and len(threads) > 5:
    range_label = ""
    if _HAS_TIME_FILTER:
        s = _FILTER_START.strftime('%Y-%m-%d') if _FILTER_START else "∞"
        e = _FILTER_END.strftime('%Y-%m-%d') if _FILTER_END else "∞"
        range_label = f" [{s} ~ {e}]"

    print(f"\n📈 数据概览 - {TIEBA_NAME}吧{range_label}")
    print("=" * 50)
    print(f"  帖子总数:    {len(threads)}")
    print(f"  总回复数:    {df['回复数'].sum()}")
    print(f"  平均回复:    {df['回复数'].mean():.1f}")
    print(f"  最高回复:    {df['回复数'].max()}")
    print(f"  活跃作者数:  {df['作者'].nunique()}")

    print(f"\n🔥 回复数 TOP 10:")
    top10 = df.nlargest(10, "回复数")[["标题", "回复数", "作者", "发帖时间"]].reset_index(drop=True)
    top10.index += 1
    try:
        display(top10)
    except NameError:
        print(top10.to_string())

    print(f"\n👤 发帖最多的作者 TOP 5:")
    for name, count in df["作者"].value_counts().head(5).items():
        print(f"  {name}: {count}条")

    ct = df["发帖时间"].dropna().astype(str)
    dates = []
    for t in ct:
        m = re.match(r"(\d{4}-\d{2}-\d{2})", t)
        if m:
            dates.append(m.group(1))
    if dates:
        date_counts = Counter(dates).most_common(10)
        print(f"\n📅 发帖日期分布 TOP 10{range_label}:")
        for d, c in date_counts:
            print(f"  {d}: {c}条")