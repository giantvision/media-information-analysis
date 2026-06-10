#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
百度贴吧爬虫 v6.0 — 脚本化 + Agent 友好版
==========================================

三种调用方式:

1. 命令行:
   python tieba_web_crawler_with_date_filter_v2.py --tieba "率土之滨" --mode pages --pages 20 \
       --start "2026-03-01" --end "2026-03-24" --output-dir /Users/yangdafu/workspace/media-information-analysis/tieba_dataset 
2. Python 脚本 / Agent 调用:
   from tieba_scraper import run, ScrapeConfig
   cfg = ScrapeConfig(tieba_name="三国志战略版", scrape_pages=200)
   result = run(cfg)

3. JSON 配置文件:
   python tieba_scraper.py --config my_task.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import re
import sys
import time as _time
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Optional

import requests

# ============================================================================
# 版本
# ============================================================================

__version__ = "6.0.0"

# ============================================================================
# 配置数据类
# ============================================================================

@dataclass
class ScrapeConfig:
    """所有可配置字段，均有合理默认值。"""

    # ── 目标 ──
    tieba_name: str = "三国志战略版"

    # ── 爬取模式: "pages" | "hours" | "days" ──
    scrape_mode: str = "pages"
    scrape_pages: int = 200
    scrape_hours: int = 24
    scrape_days: int = 7

    # ── 时间范围过滤 (仅影响存储，不影响爬取) ──
    #    支持 "YYYY-MM-DD" 或 "YYYY-MM-DD HH:MM:SS"，空字符串表示不限
    start_date_time: str = ""
    end_date_time: str = ""

    # ── 排序 ──
    sort_type: int | str = 0          # 0 / 1 / "both"

    # ── 高级 ──
    include_top: bool = False
    max_scan_pages: int = 10000
    delay_min: float = 0.8
    delay_max: float = 1.8

    # ── 输出 ──
    output_dir: str = ""              # 为空时使用当前目录
    output_filename: str = ""         # 为空时自动生成
    save_to_drive: bool = False       # Colab Google Drive 挂载
    force_restart: bool = False

    # ── 分析 ──
    run_analysis: bool = True         # 爬取完成后是否打印统计分析
    analysis_top_n: int = 10          # TOP N 排行

    # ── 运行时生成 (外部不需要设置) ──
    base_path: str = field(default="", init=False, repr=False)

    # ------------------------------------------------------------------
    # 工厂方法
    # ------------------------------------------------------------------

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "ScrapeConfig":
        """从字典创建，忽略未知字段。"""
        valid = {f.name for f in cls.__dataclass_fields__.values() if f.init}
        return cls(**{k: v for k, v in d.items() if k in valid})

    @classmethod
    def from_json(cls, path: str) -> "ScrapeConfig":
        """从 JSON 配置文件创建。"""
        with open(path, "r", encoding="utf-8") as f:
            return cls.from_dict(json.load(f))

    @classmethod
    def from_cli(cls, argv: list[str] | None = None) -> "ScrapeConfig":
        """从命令行参数创建。"""
        p = argparse.ArgumentParser(
            description="百度贴吧爬虫 v6.0",
            formatter_class=argparse.RawDescriptionHelpFormatter,
        )

        # 支持从 JSON 文件加载全部配置
        p.add_argument("--config", type=str, default="",
                        help="JSON 配置文件路径 (提供后其他参数仅作覆盖)")

        g = p.add_argument_group("目标")
        g.add_argument("--tieba", dest="tieba_name", type=str, help="贴吧名称")

        g = p.add_argument_group("爬取模式")
        g.add_argument("--mode", dest="scrape_mode", choices=["pages", "hours", "days"])
        g.add_argument("--pages", dest="scrape_pages", type=int)
        g.add_argument("--hours", dest="scrape_hours", type=int)
        g.add_argument("--days", dest="scrape_days", type=int)

        g = p.add_argument_group("时间范围过滤")
        g.add_argument("--start", dest="start_date_time", type=str,
                        help='起始时间, 如 "2026-03-01"')
        g.add_argument("--end", dest="end_date_time", type=str,
                        help='结束时间, 如 "2026-03-24"')

        g = p.add_argument_group("排序与高级")
        g.add_argument("--sort", dest="sort_type", type=str,
                        help='0=回复时间 1=发帖时间 both=双模式')
        g.add_argument("--include-top", dest="include_top", action="store_true")
        g.add_argument("--max-scan", dest="max_scan_pages", type=int)
        g.add_argument("--delay-min", dest="delay_min", type=float)
        g.add_argument("--delay-max", dest="delay_max", type=float)

        g = p.add_argument_group("输出")
        g.add_argument("--output-dir", dest="output_dir", type=str)
        g.add_argument("--output", dest="output_filename", type=str,
                        help="输出文件名 (不含后缀)")
        g.add_argument("--drive", dest="save_to_drive", action="store_true")
        g.add_argument("--force-restart", dest="force_restart", action="store_true")

        g = p.add_argument_group("分析")
        g.add_argument("--no-analysis", dest="run_analysis", action="store_false")
        g.add_argument("--top-n", dest="analysis_top_n", type=int)

        args = p.parse_args(argv)

        # 基础: JSON 文件 → 再用 CLI 显式参数覆盖
        if args.config:
            cfg = cls.from_json(args.config)
        else:
            cfg = cls()

        # 把 CLI 中显式传入的参数覆盖到 cfg 上
        cli_dict = vars(args)
        cli_dict.pop("config", None)
        for k, v in cli_dict.items():
            if v is not None:
                # sort_type 特殊处理: "both" 保留字符串，否则转 int
                if k == "sort_type":
                    v = v if v == "both" else int(v)
                setattr(cfg, k, v)

        return cfg

    # ------------------------------------------------------------------
    # 序列化
    # ------------------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d.pop("base_path", None)
        return d

    def to_json(self, path: str):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, ensure_ascii=False, indent=2)

    # ------------------------------------------------------------------
    # 路径解析
    # ------------------------------------------------------------------

    def resolve_paths(self):
        """根据配置确定 base_path (无后缀)。"""
        if self.save_to_drive:
            try:
                from google.colab import drive  # type: ignore
                drive.mount("/content/drive")
                drive_dir = f"/content/drive/MyDrive/web_crawl_data"
                os.makedirs(drive_dir, exist_ok=True)
                root = drive_dir
            except ImportError:
                root = self.output_dir or "."
        else:
            root = self.output_dir or "."

        os.makedirs(root, exist_ok=True)

        fname = self.output_filename
        if not fname:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            name_tag = self.tieba_name[:10].replace(" ", "_")
            mode_tag = self.scrape_mode
            sort_tag = self.sort_type
            fname = f"tieba_{name_tag}_sort-{sort_tag}_{mode_tag}_{ts}"

        self.base_path = os.path.join(root, fname)


# ============================================================================
# 常量 & 签名
# ============================================================================

_SIGN_KEY = "tiebaclient!!!"
_CLIENT_VERSION = "12.67.1.0"
_API_ENDPOINTS = [
    "https://tieba.baidu.com/c/f/frs/page",
    "http://c.tieba.baidu.com/c/f/frs/page",
]


def _calc_sign(params: dict) -> str:
    raw = "".join(f"{k}={params[k]}" for k in sorted(params.keys()))
    return hashlib.md5((raw + _SIGN_KEY).encode("utf-8")).hexdigest().upper()


# ============================================================================
# 时间工具
# ============================================================================

_TIME_FORMATS = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d")


def normalize_time(time_str) -> str:
    """将各种时间表示统一为标准字符串。"""
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
    if m:
        return (now - timedelta(minutes=int(m.group(1)))).strftime("%Y-%m-%d %H:%M")
    m = re.match(r"(\d+)\s*小时前", s)
    if m:
        return (now - timedelta(hours=int(m.group(1)))).strftime("%Y-%m-%d %H:%M")
    if "刚刚" in s:
        return now.strftime("%Y-%m-%d %H:%M")
    m = re.match(r"(\d{4})-(\d{1,2})-(\d{1,2})\s+(\d{1,2}):(\d{2})", s)
    if m:
        return (f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d} "
                f"{int(m.group(4)):02d}:{m.group(5)}")
    m = re.match(r"^(\d{1,2}):(\d{2})$", s)
    if m:
        return f"{now.strftime('%Y-%m-%d')} {int(m.group(1)):02d}:{m.group(2)}"
    m = re.match(r"昨天\s*(\d{1,2}):(\d{2})", s)
    if m:
        y = now - timedelta(days=1)
        return f"{y.strftime('%Y-%m-%d')} {int(m.group(1)):02d}:{m.group(2)}"
    m = re.match(r"^(\d{1,2})-(\d{1,2})$", s)
    if m:
        month, day = int(m.group(1)), int(m.group(2))
        year = now.year
        if month > now.month or (month == now.month and day > now.day):
            year -= 1
        return f"{year}-{month:02d}-{day:02d}"
    m = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})$", s)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    m = re.match(r"^(\d{4})-(\d{1,2})$", s)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-01"
    return s


def _parse_dt(date_str: str) -> Optional[datetime]:
    """通用时间字符串 → datetime。"""
    if not date_str:
        return None
    for fmt in _TIME_FORMATS:
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    return None


# ============================================================================
# 时间范围过滤器
# ============================================================================

@dataclass
class TimeFilter:
    """封装时间范围过滤逻辑，方便外部检查边界值。"""
    start_dt: Optional[datetime] = None
    end_dt: Optional[datetime] = None

    @property
    def active(self) -> bool:
        return self.start_dt is not None or self.end_dt is not None

    def in_range(self, thread: dict) -> bool:
        if not self.active:
            return True
        ct = _parse_dt(thread.get("create_time", ""))
        if ct is None:
            return True          # 无法解析时默认保留
        if self.start_dt and ct < self.start_dt:
            return False
        if self.end_dt and ct > self.end_dt:
            return False
        return True

    def label(self) -> str:
        s = self.start_dt.strftime("%Y-%m-%d %H:%M:%S") if self.start_dt else "不限"
        e = self.end_dt.strftime("%Y-%m-%d %H:%M:%S") if self.end_dt else "不限"
        return f"{s} ~ {e}"

    def short_label(self) -> str:
        s = self.start_dt.strftime("%Y-%m-%d") if self.start_dt else "∞"
        e = self.end_dt.strftime("%Y-%m-%d") if self.end_dt else "∞"
        return f"[{s} ~ {e}]"

    @classmethod
    def from_config(cls, cfg: ScrapeConfig) -> "TimeFilter":
        start_dt = _parse_dt(cfg.start_date_time)
        end_dt = _parse_dt(cfg.end_date_time)
        # 日期级别的 end 自动补齐到 23:59:59
        if end_dt and end_dt.hour == 0 and end_dt.minute == 0 and end_dt.second == 0:
            if re.match(r"^\d{4}-\d{1,2}-\d{1,2}$", cfg.end_date_time.strip()):
                end_dt = end_dt.replace(hour=23, minute=59, second=59)
        return cls(start_dt=start_dt, end_dt=end_dt)


# ============================================================================
# API 层
# ============================================================================

def api_fetch(kw: str, pn: int, rn: int = 30, sort_type: int = 0) -> Optional[list[dict]]:
    params = {
        "_client_version": _CLIENT_VERSION,
        "kw": kw,
        "pn": str(pn),
        "rn": str(rn),
        "sort_type": str(sort_type),
    }
    params["sign"] = _calc_sign(params)
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": f"bdtb for Android {_CLIENT_VERSION}",
        "Accept-Encoding": "gzip",
        "Connection": "keep-alive",
    }
    for endpoint in _API_ENDPOINTS:
        try:
            resp = requests.post(endpoint, data=params, headers=headers, timeout=15)
            if resp.status_code != 200:
                continue
            data = resp.json()
            if str(data.get("error_code", "0")) != "0":
                continue
            thread_list = data.get("thread_list") or []
            return _parse_threads(thread_list) if thread_list else []
        except (requests.RequestException, json.JSONDecodeError, KeyError):
            continue
    return None


def _parse_threads(thread_list: list) -> list[dict]:
    threads = []
    for item in thread_list:
        if not isinstance(item, dict):
            continue
        tid = int(item.get("id", 0) or item.get("tid", 0) or 0)
        title = str(item.get("title", "") or "")
        if not title:
            continue

        # 摘要
        abs_data = item.get("abstract") or item.get("first_post_content") or ""
        if isinstance(abs_data, list):
            abstract = "".join(str(a.get("text", "")) for a in abs_data if isinstance(a, dict))
        else:
            abstract = str(abs_data)

        # 作者
        ad = item.get("author") or {}
        author_name = str(ad.get("name_show", "") or ad.get("name", "") or "") if isinstance(ad, dict) else ""

        # 最后回复人
        lr = item.get("last_replyer") or {}
        last_reply_name = str(lr.get("name_show", "") or lr.get("name", "") or "") if isinstance(lr, dict) else ""

        # 点赞
        ag = item.get("agree") or {}
        agree_num = int(ag.get("agree_num", 0) or 0) if isinstance(ag, dict) else int(item.get("agree_num", 0) or 0)

        threads.append({
            "thread_id": tid,
            "title": title,
            "abstract": abstract,
            "reply_count": int(item.get("reply_num", 0) or 0),
            "agree_num": agree_num,
            "share_num": int(item.get("share_num", 0) or 0),
            "is_top": bool(item.get("is_top", 0)),
            "url": f"https://tieba.baidu.com/p/{tid}" if tid else "",
            "author": {"name": author_name, "name_id": author_name},
            "create_time": normalize_time(item.get("create_time") or ""),
            "create_time_raw": str(item.get("create_time") or ""),
            "last_reply": {
                "author": last_reply_name,
                "time": normalize_time(item.get("last_time_int") or ""),
                "time_raw": str(item.get("last_time_int") or ""),
            },
            "crawl_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })
    return threads


# ============================================================================
# Checkpoint 管理
# ============================================================================

class CheckpointManager:
    def __init__(self, base_path: str, kw: str):
        self.ckpt_path = f"{base_path}_checkpoint.json"
        self.data_path = f"{base_path}.jsonl"
        self.kw = kw

    def load(self) -> Optional[dict]:
        if not os.path.exists(self.ckpt_path):
            return None
        try:
            with open(self.ckpt_path, "r", encoding="utf-8") as f:
                state = json.load(f)
            if state.get("kw") != self.kw:
                print(f"  ⚠️  Checkpoint 贴吧名不匹配 ({state.get('kw')} ≠ {self.kw})，忽略")
                return None
            return state
        except (json.JSONDecodeError, KeyError) as e:
            print(f"  ⚠️  Checkpoint 损坏: {e}")
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
        tmp = self.ckpt_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False)
        os.replace(tmp, self.ckpt_path)

    def load_existing_data(self) -> tuple[list[dict], set]:
        threads, seen = [], set()
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
        for p in (self.ckpt_path, self.data_path):
            if os.path.exists(p):
                os.remove(p)


# ============================================================================
# 爬虫核心
# ============================================================================

class TiebaScraper:
    def __init__(self, cfg: ScrapeConfig, ckpt: CheckpointManager, tf: TimeFilter):
        self.cfg = cfg
        self.ckpt = ckpt
        self.tf = tf
        self._fails = 0

    def _fetch(self, pn: int, sort_type: int) -> Optional[list[dict]]:
        result = api_fetch(self.cfg.tieba_name, pn=pn, sort_type=sort_type)
        self._fails = self._fails + 1 if result is None else 0
        return result

    @property
    def _delay(self) -> tuple[float, float]:
        return (self.cfg.delay_min, self.cfg.delay_max)

    # ------------------------------------------------------------------

    def _restore_checkpoint(self) -> tuple[list[dict], set, dict, int]:
        """尝试从 checkpoint 恢复状态，返回 (threads, seen, progress, filtered)。"""
        state = self.ckpt.load()
        if state and not self.cfg.force_restart:
            threads, seen = self.ckpt.load_existing_data()
            progress = state.get("progress", {})
            filtered = state.get("total_filtered", 0)
            print(f"  🔖 断点恢复: {len(threads)} 条 (范围外 {filtered} 条), seen={len(seen)}")
            print(f"     进度: {progress}")
            return threads, seen, progress, filtered
        if self.cfg.force_restart and state:
            print("  🔄 强制重新开始")
            self.ckpt.clear()
        return [], set(), {}, 0

    def _save(self, seen, progress, n_collected, n_filtered):
        self.ckpt.save(seen, progress, n_collected, n_filtered)

    # ------------------------------------------------------------------

    def scrape_by_pages(self) -> list[dict]:
        cfg = self.cfg
        sort_modes = [0, 1] if cfg.sort_type == "both" else [int(cfg.sort_type)]
        sort_names = {0: "回复时间", 1: "发帖时间"}

        all_threads, seen, progress, filtered = self._restore_checkpoint()

        for sort in sort_modes:
            sk = str(sort)
            start_pn = progress.get(sk, 0) + 1
            stale = 0

            if start_pn > cfg.scrape_pages:
                print(f"\n  ✅ 排序{sort_names[sort]}已完成")
                continue

            if len(sort_modes) > 1:
                print(f"\n  🔄 排序: {sort_names[sort]} (sort={sort})")
                if start_pn > 1:
                    print(f"     续爬 pn={start_pn} (跳过 {start_pn - 1} 页)")
                print(f"  {'─' * 50}")

            for pn in range(start_pn, cfg.scrape_pages + 1):
                result = self._fetch(pn, sort)
                if result is None:
                    if self._fails >= 3:
                        print("  🛑 连续失败≥3，终止"); break
                    print(f"  ⚠️  pn={pn} 失败"); continue
                if not result:
                    print(f"  📭 pn={pn} 空，到底")
                    progress[sk] = cfg.scrape_pages
                    self._save(seen, progress, len(all_threads), filtered); break

                added, duped, topped, oor = 0, 0, 0, 0
                batch = []
                for t in result:
                    if t["thread_id"] in seen:
                        duped += 1; continue
                    seen.add(t["thread_id"])
                    if not cfg.include_top and t["is_top"]:
                        topped += 1; continue
                    if self.tf.active and not self.tf.in_range(t):
                        oor += 1; filtered += 1; continue
                    all_threads.append(t)
                    batch.append(t)
                    added += 1

                oor_s = f", 范围外:{oor}" if self.tf.active else ""
                print(
                    f"  📡 pn={pn:>4} → +{added:>3} "
                    f"(重复:{duped:>2}, 置顶:{topped}{oor_s}) "
                    f"| 累计 {len(all_threads)}"
                )

                if batch:
                    self.ckpt.append_threads(batch)
                progress[sk] = pn
                self._save(seen, progress, len(all_threads), filtered)

                if added == 0:
                    stale += 1
                    if stale >= 5:
                        print(f"  ⏹️ 连续{stale}页无新增，到底")
                        progress[sk] = cfg.scrape_pages
                        self._save(seen, progress, len(all_threads), filtered); break
                else:
                    stale = 0
                _time.sleep(random.uniform(*self._delay))

        if self.tf.active:
            print(f"\n  🕐 过滤汇总: 保留 {len(all_threads)}, 过滤 {filtered}")
        return all_threads

    # ------------------------------------------------------------------

    def scrape_by_time(self) -> list[dict]:
        cfg = self.cfg
        hours = cfg.scrape_hours if cfg.scrape_mode == "hours" else 0
        days = cfg.scrape_days if cfg.scrape_mode == "days" else 0
        cutoff = datetime.now() - timedelta(hours=hours, days=days)
        print(f"  ⏰ 截止: {cutoff.strftime('%Y-%m-%d %H:%M')}")

        sort_modes = [0, 1] if cfg.sort_type == "both" else [int(cfg.sort_type)]
        sort_names = {0: "回复时间", 1: "发帖时间"}

        all_threads, seen, progress, filtered = self._restore_checkpoint()

        for sort in sort_modes:
            sk = str(sort)
            start_pn = progress.get(sk, 0) + 1
            stale = 0
            should_stop = False

            if len(sort_modes) > 1:
                print(f"\n  🔄 排序: {sort_names[sort]}")
                if start_pn > 1:
                    print(f"     续爬 pn={start_pn}")

            for pn in range(start_pn, cfg.max_scan_pages + 1):
                result = self._fetch(pn, sort)
                if result is None:
                    if self._fails >= 3:
                        break
                    continue
                if not result:
                    progress[sk] = cfg.max_scan_pages
                    self._save(seen, progress, len(all_threads), filtered); break

                added, duped, expired, oor = 0, 0, 0, 0
                batch = []
                for t in result:
                    if t["thread_id"] in seen:
                        duped += 1; continue
                    seen.add(t["thread_id"])
                    if not cfg.include_top and t["is_top"]:
                        continue
                    rt = t["last_reply"]["time"]
                    if rt:
                        dt = _parse_dt(rt)
                        if dt and dt < cutoff:
                            should_stop = True; expired += 1; continue
                    if self.tf.active and not self.tf.in_range(t):
                        oor += 1; filtered += 1; continue
                    all_threads.append(t)
                    batch.append(t)
                    added += 1

                oor_s = f", 范围外:{oor}" if self.tf.active else ""
                print(
                    f"  📡 pn={pn:>4} → +{added:>3} "
                    f"(重复:{duped:>2}, 过期:{expired}{oor_s}) "
                    f"| 累计 {len(all_threads)}"
                )

                if batch:
                    self.ckpt.append_threads(batch)
                progress[sk] = pn
                self._save(seen, progress, len(all_threads), filtered)

                if should_stop:
                    print("  ⏹️ 到达截止时间"); break
                if added == 0:
                    stale += 1
                    if stale >= 5:
                        print(f"  ⏹️ 连续{stale}页无新增"); break
                else:
                    stale = 0
                _time.sleep(random.uniform(*self._delay))

        if self.tf.active:
            print(f"\n  🕐 过滤汇总: 保留 {len(all_threads)}, 过滤 {filtered}")
        return all_threads


# ============================================================================
# 分析输出
# ============================================================================

def print_analysis(threads: list[dict], cfg: ScrapeConfig, tf: TimeFilter):
    """打印统计分析。可独立调用。"""
    try:
        import pandas as pd
    except ImportError:
        print("  ⚠️ pandas 未安装，跳过分析")
        return

    if not threads:
        print("⚠️ 无数据" + (" (指定时间范围内无匹配)" if tf.active else ""))
        return

    flat = [{
        "标题": t["title"],
        "回复数": t["reply_count"],
        "点赞数": t.get("agree_num", 0),
        "作者": t["author"]["name"],
        "发帖时间": t.get("create_time", ""),
        "最后回复时间": t["last_reply"]["time"],
        "最后回复者": t["last_reply"]["author"],
        "摘要": (t["abstract"][:50] + "...") if len(t["abstract"]) > 50 else t["abstract"],
        "链接": t["url"],
    } for t in threads]
    df = pd.DataFrame(flat)

    rl = f" {tf.short_label()}" if tf.active else ""
    print(f"\n📊 共 {len(df)} 条帖子{rl}")
    print(df.head(20).to_string(index=False))

    if len(threads) <= 5:
        return

    top_n = cfg.analysis_top_n
    print(f"\n📈 数据概览 — {cfg.tieba_name}吧{rl}")
    print("=" * 50)
    print(f"  帖子总数:   {len(threads)}")
    print(f"  总回复数:   {df['回复数'].sum()}")
    print(f"  平均回复:   {df['回复数'].mean():.1f}")
    print(f"  最高回复:   {df['回复数'].max()}")
    print(f"  活跃作者:   {df['作者'].nunique()}")

    print(f"\n🔥 回复数 TOP {top_n}:")
    print(df.nlargest(top_n, "回复数")[["标题", "回复数", "作者", "发帖时间"]].to_string())

    print(f"\n👤 发帖最多的作者 TOP 5:")
    for name, count in df["作者"].value_counts().head(5).items():
        print(f"  {name}: {count}条")

    dates = []
    for t in df["发帖时间"].dropna().astype(str):
        m = re.match(r"(\d{4}-\d{2}-\d{2})", t)
        if m:
            dates.append(m.group(1))
    if dates:
        print(f"\n📅 发帖日期分布 TOP 10{rl}:")
        for d, c in Counter(dates).most_common(10):
            print(f"  {d}: {c}条")


# ============================================================================
# 主入口: run()
# ============================================================================

@dataclass
class ScrapeResult:
    """run() 的返回值，方便程序化使用。"""
    threads: list[dict]
    total_count: int
    data_file: str
    checkpoint_file: str
    config: ScrapeConfig


def run(cfg: ScrapeConfig | dict | None = None, **overrides) -> ScrapeResult:
    """
    程序化入口 — 供 Python 脚本或 LLM Agent 调用。

    示例:
        # 方式 A: 传 ScrapeConfig
        result = run(ScrapeConfig(tieba_name="火影忍者", scrape_pages=50))

        # 方式 B: 传字典
        result = run({"tieba_name": "火影忍者", "scrape_pages": 50})

        # 方式 C: 关键字参数
        result = run(tieba_name="火影忍者", scrape_pages=50)
    """
    # --- 构建 config ---
    if cfg is None:
        cfg = ScrapeConfig.from_dict(overrides)
    elif isinstance(cfg, dict):
        merged = {**cfg, **overrides}
        cfg = ScrapeConfig.from_dict(merged)
    else:
        for k, v in overrides.items():
            if hasattr(cfg, k):
                setattr(cfg, k, v)

    cfg.resolve_paths()
    tf = TimeFilter.from_config(cfg)

    # --- 打印配置摘要 ---
    sort_desc = {0: "按回复时间", 1: "按发帖时间", "both": "双模式合并"}
    print(f"\n🚀 [{cfg.tieba_name}吧] mode={cfg.scrape_mode} sort={sort_desc.get(cfg.sort_type, cfg.sort_type)}")
    if tf.active:
        print(f"   🕐 时间过滤: {tf.label()}")
    print(f"   📁 {cfg.base_path}.jsonl")
    print("─" * 55)

    # --- checkpoint ---
    ckpt = CheckpointManager(cfg.base_path, cfg.tieba_name)
    if not cfg.force_restart:
        state = ckpt.load()
        if state:
            print(f"📂 断点: {state['total_collected']}条, 进度={state['progress']}, "
                  f"更新于 {state['updated_at']}")
    elif cfg.force_restart:
        ckpt.clear()
        print("🔄 强制重新开始")

    # --- 爬取 ---
    scraper = TiebaScraper(cfg, ckpt, tf)

    if cfg.scrape_mode == "pages":
        threads = scraper.scrape_by_pages()
    elif cfg.scrape_mode in ("hours", "days"):
        threads = scraper.scrape_by_time()
    else:
        raise ValueError(f"未知 scrape_mode: {cfg.scrape_mode}")

    print("─" * 55)
    print(f"🎉 完成！范围内共 {len(threads)} 条")
    print(f"   数据: {cfg.base_path}.jsonl")

    # --- 分析 ---
    if cfg.run_analysis:
        print_analysis(threads, cfg, tf)

    return ScrapeResult(
        threads=threads,
        total_count=len(threads),
        data_file=f"{cfg.base_path}.jsonl",
        checkpoint_file=f"{cfg.base_path}_checkpoint.json",
        config=cfg,
    )


# ============================================================================
# CLI 入口
# ============================================================================

def main(argv: list[str] | None = None):
    cfg = ScrapeConfig.from_cli(argv)
    result = run(cfg)
    print(f"\n✅ 共 {result.total_count} 条, 文件: {result.data_file}")


if __name__ == "__main__":
    main()