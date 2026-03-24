#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
百度贴吧爬虫 v5.0 - 断点续爬 + 实时存储版
==========================================
v5.0 新增:
  ✅ 1. 发帖时间字段 (create_time) — 从 API 原始数据提取
  ✅ 2. 实时存储 — 每页爬完立即追加写入 JSONL 文件，断电不丢数据
  ✅ 3. 断点续爬 — checkpoint 记录爬取进度，中断后再次运行自动跳过已爬页

使用方法: 按 Cell 顺序在 Colab 中运行
  - 首次运行: 正常爬取，自动保存 checkpoint
  - 中断后再次运行: 自动检测 checkpoint，从断点处继续
  - 想重新爬: 把 FORCE_RESTART 设为 True
"""


# ============================================================================
# ===== CELL 1: 安装依赖 =====
# ============================================================================
# !pip install requests pandas -q


# ============================================================================
# ===== CELL 2: 配置参数 =====
# ============================================================================

TIEBA_NAME = "王于兴师"     # 🎯 贴吧名称

# ──────── 爬取模式(三选一) ────────
SCRAPE_MODE = "pages"
SCRAPE_PAGES = 2              # 爬取页数

# SCRAPE_MODE = "hours"
# SCRAPE_HOURS = 24

# SCRAPE_MODE = "days"
# SCRAPE_DAYS = 7

# ──────── 高级配置 ────────
INCLUDE_TOP = False
MAX_SCAN_PAGES = 10000
DELAY_RANGE = (0.8, 1.8)
SORT_TYPE = 0              # 0 / 1 / "both"

SAVE_TO_DRIVE = True
OUTPUT_FILENAME = "tieba_wyxs_mode-0_pages-2_20260324_run-10"  # 不带后缀，程序会自动加 .jsonl / .json / _checkpoint.json

FORCE_RESTART = False           # True=忽略checkpoint强制从头爬

print(f"✅ 配置完成: [{TIEBA_NAME}吧] | 模式={SCRAPE_MODE} | 排序={SORT_TYPE}")


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

        # 摘要
        abstract = ""
        abs_data = item.get("abstract") or item.get("first_post_content") or ""
        if isinstance(abs_data, list):
            abstract = "".join(str(a.get("text", "")) for a in abs_data if isinstance(a, dict))
        elif isinstance(abs_data, str):
            abstract = abs_data

        # 作者
        author_data = item.get("author") or {}
        author_name = ""
        if isinstance(author_data, dict):
            author_name = str(author_data.get("name_show", "") or author_data.get("name", "") or "")

        # 发帖时间
        create_time_raw = item.get("create_time") or ""
        create_time = normalize_time(create_time_raw)

        # 最后回复时间
        last_time_raw = item.get("last_time_int") or ""
        last_time = normalize_time(last_time_raw)

        # 最后回复人
        last_replyer = item.get("last_replyer") or {}
        last_reply_name = ""
        if isinstance(last_replyer, dict):
            last_reply_name = str(last_replyer.get("name_show", "") or last_replyer.get("name", "") or "")

        # ===== [v5.1 新增] 点赞数 =====
        agree_data = item.get("agree") or {}
        if isinstance(agree_data, dict):
            agree_num = int(agree_data.get("agree_num", 0) or 0)
        else:
            agree_num = int(item.get("agree_num", 0) or 0)

        # ===== [v5.1 新增] 分享数 =====
        share_num = int(item.get("share_num", 0) or 0)

        threads.append({
            "thread_id": tid,
            "title": title,
            "abstract": abstract,
            "reply_count": int(item.get("reply_num", 0) or 0),
            "agree_num": agree_num,       # [v5.1] 点赞数
            "share_num": share_num,       # [v5.1] 分享数
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
# [v5 新增] Checkpoint 断点管理
# ---------------------------------------------------------------------------

class CheckpointManager:
    """
    管理爬取进度的断点续爬机制。

    文件结构:
      {OUTPUT_FILENAME}_checkpoint.json — 进度状态
      {OUTPUT_FILENAME}.jsonl            — 已爬数据 (每行一条帖子JSON)
    """

    def __init__(self, base_path: str, kw: str):
        self.ckpt_path = f"{base_path}_checkpoint.json"
        self.data_path = f"{base_path}.jsonl"
        self.kw = kw
        self.state = None

    def load(self) -> Optional[dict]:
        """加载 checkpoint，返回 state 或 None"""
        if not os.path.exists(self.ckpt_path):
            return None
        try:
            with open(self.ckpt_path, "r", encoding="utf-8") as f:
                state = json.load(f)
            # 校验：贴吧名称必须匹配
            if state.get("kw") != self.kw:
                print(f"  ⚠️  Checkpoint 贴吧名不匹配 ({state.get('kw')} ≠ {self.kw})，忽略")
                return None
            self.state = state
            return state
        except (json.JSONDecodeError, KeyError) as e:
            print(f"  ⚠️  Checkpoint 文件损坏: {e}")
            return None

    def save(self, seen_ids: set, progress: dict, total_collected: int):
        """保存 checkpoint"""
        state = {
            "kw": self.kw,
            "seen_ids": list(seen_ids),
            "progress": progress,
            "total_collected": total_collected,
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        # 先写临时文件再原子替换，防止写入中断导致文件损坏
        tmp_path = self.ckpt_path + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False)
        os.replace(tmp_path, self.ckpt_path)

    def load_existing_data(self) -> tuple[list[dict], set]:
        """从 JSONL 文件加载已爬取的数据"""
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
        """追加写入新帖子到 JSONL 文件"""
        if not threads:
            return
        with open(self.data_path, "a", encoding="utf-8") as f:
            for t in threads:
                f.write(json.dumps(t, ensure_ascii=False) + "\n")

    def clear(self):
        """清除 checkpoint 和数据文件（强制重新开始时调用）"""
        for p in [self.ckpt_path, self.data_path]:
            if os.path.exists(p):
                os.remove(p)


# ---------------------------------------------------------------------------
# 核心爬虫 (带断点续爬)
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

        # --- 加载 checkpoint ---
        all_threads, seen = [], set()
        progress = {}  # {sort_str: last_completed_pn}

        if self.ckpt:
            state = self.ckpt.load()
            if state and not FORCE_RESTART:
                existing_threads, seen = self.ckpt.load_existing_data()
                all_threads = existing_threads
                progress = state.get("progress", {})
                print(f"  🔖 从断点恢复: 已有 {len(all_threads)} 条, seen={len(seen)}")
                print(f"     进度: {progress}")
            elif FORCE_RESTART and state:
                print(f"  🔄 强制重新开始 (忽略 checkpoint)")
                self.ckpt.clear()

        for sort in sort_modes:
            sort_key = str(sort)
            start_pn = progress.get(sort_key, 0) + 1  # 从上次完成的下一页开始
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
                    progress[sort_key] = num_pages  # 标记完成
                    if self.ckpt:
                        self.ckpt.save(seen, progress, len(all_threads))
                    break

                # 去重 & 过滤
                added, duped, topped = 0, 0, 0
                new_threads = []
                for t in result:
                    if t["thread_id"] in seen:
                        duped += 1
                        continue
                    seen.add(t["thread_id"])
                    if not include_top and t["is_top"]:
                        topped += 1
                        continue
                    all_threads.append(t)
                    new_threads.append(t)
                    added += 1

                print(
                    f"  📡 pn={pn:>4} → +{added:>3}条 "
                    f"(重复:{duped:>2}, 置顶:{topped}) "
                    f"| 累计 {len(all_threads)}"
                )

                # [v5] 实时追加存储 + 更新 checkpoint
                if self.ckpt and new_threads:
                    self.ckpt.append_threads(new_threads)
                progress[sort_key] = pn
                if self.ckpt:
                    self.ckpt.save(seen, progress, len(all_threads))

                # 停滞检测
                if added == 0:
                    stale_count += 1
                    if stale_count >= 5:
                        print(f"  ⏹️ 连续{stale_count}页无新增，该排序已到底")
                        progress[sort_key] = num_pages
                        if self.ckpt:
                            self.ckpt.save(seen, progress, len(all_threads))
                        break
                else:
                    stale_count = 0

                _time.sleep(random.uniform(*self.delay))

        return all_threads

    def scrape_by_time(self, hours=0, days=0, max_scan_pages=100, include_top=False) -> list[dict]:
        cutoff = datetime.now() - timedelta(hours=hours, days=days)
        print(f"  ⏰ 截止: {cutoff.strftime('%Y-%m-%d %H:%M')}")

        sort_modes = [0, 1] if self.sort_type == "both" else [int(self.sort_type)]
        sort_names = {0: "回复时间", 1: "发帖时间"}

        all_threads, seen = [], set()
        progress = {}

        if self.ckpt:
            state = self.ckpt.load()
            if state and not FORCE_RESTART:
                existing, seen = self.ckpt.load_existing_data()
                all_threads = existing
                progress = state.get("progress", {})
                print(f"  🔖 从断点恢复: 已有 {len(all_threads)} 条")
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
                        self.ckpt.save(seen, progress, len(all_threads))
                    break

                added, duped, expired = 0, 0, 0
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
                    all_threads.append(t)
                    new_threads.append(t)
                    added += 1

                print(
                    f"  📡 pn={pn:>4} → +{added:>3}条 "
                    f"(重复:{duped:>2}, 过期:{expired}) "
                    f"| 累计 {len(all_threads)}"
                )

                if self.ckpt and new_threads:
                    self.ckpt.append_threads(new_threads)
                progress[sort_key] = pn
                if self.ckpt:
                    self.ckpt.save(seen, progress, len(all_threads))

                if should_stop:
                    print(f"  ⏹️ 到达时间截止范围"); break
                if added == 0:
                    stale_count += 1
                    if stale_count >= 5:
                        print(f"  ⏹️ 连续{stale_count}页无新增"); break
                else:
                    stale_count = 0
                _time.sleep(random.uniform(*self.delay))

        return all_threads


print("✅ 爬虫核心代码加载完成 (v5.0 断点续爬版)")


# ============================================================================
# ===== CELL 4: 执行爬取 =====
# ============================================================================

# --- 初始化路径 ---
if SAVE_TO_DRIVE:
    try:
        from google.colab import drive
        drive.mount("/content/drive")
        base_path = f"/content/drive/MyDrive/web_crawl_data/tieba_wyxs/{OUTPUT_FILENAME}"
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
print(f"🎉 完成！共 {len(threads)} 条帖子")
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
        "发帖时间": t.get("create_time", ""),       # [v5 新增]
        "最后回复时间": t["last_reply"]["time"],
        "最后回复者": t["last_reply"]["author"],
        "摘要": (t["abstract"][:50] + "...") if len(t["abstract"]) > 50 else t["abstract"],
        "帖子链接": t["url"],
    } for t in threads]
    df = pd.DataFrame(flat)
    print(f"\n📊 共 {len(df)} 条帖子:\n")
    try:
        display(df)
    except NameError:
        print(df.head(20).to_string(index=False))
else:
    print("⚠️ 未获取到数据")



# ============================================================================
# ===== CELL 6: 数据分析 =====
# ============================================================================

if threads and len(threads) > 5:
    print(f"\n📈 数据概览 - {TIEBA_NAME}吧")
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

    # 发帖时间分布
    ct = df["发帖时间"].dropna().astype(str)
    dates = []
    for t in ct:
        m = re.match(r"(\d{4}-\d{2}-\d{2})", t)
        if m:
            dates.append(m.group(1))
    if dates:
        date_counts = Counter(dates).most_common(10)
        print(f"\n📅 发帖日期分布 TOP 10:")
        for d, c in date_counts:
            print(f"  {d}: {c}条")