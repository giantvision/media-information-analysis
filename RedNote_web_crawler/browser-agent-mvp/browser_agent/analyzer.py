"""Network snapshot analyzer for recipe discovery.

The analyzer helps with the RedNote workflow:
1. Run a real browser task and save snapshots.
2. Inspect candidate JSON endpoints.
3. Tune Recipe url_pattern, item_path, and field paths.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, asdict
from typing import Any
from urllib.parse import urlparse


@dataclass(frozen=True)
class EndpointSummary:
    host: str
    path: str
    method: str
    count: int
    statuses: list[int]
    content_types: list[str]
    json_array_paths: list[str]
    keyword_hits: dict[str, int]
    example_response_id: str | None
    example_url: str


def analyze_snapshots(raw_snapshots: list[dict[str, Any]], keywords: list[str] | None = None) -> list[dict[str, Any]]:
    keywords = keywords or ["comment", "note", "feed", "user", "cursor", "has_more"]
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for snapshot in raw_snapshots:
        parsed = urlparse(snapshot.get("url", ""))
        key = (
            parsed.netloc,
            parsed.path,
            snapshot.get("method", "GET").upper(),
        )
        grouped[key].append(snapshot)

    summaries = []
    for (host, path, method), items in grouped.items():
        bodies = [item.get("body") for item in items]
        json_array_paths = sorted({p for body in bodies for p in find_array_paths(body)})
        keyword_hits = {
            keyword: sum(1 for body in bodies if contains_keyword(body, keyword))
            for keyword in keywords
        }
        content_types = sorted({item.get("content_type", "") for item in items if item.get("content_type")})
        statuses = sorted({int(item.get("status", 0)) for item in items})
        example = next((item for item in items if isinstance(item.get("body"), (dict, list))), items[0])
        summaries.append(
            EndpointSummary(
                host=host,
                path=path,
                method=method,
                count=len(items),
                statuses=statuses,
                content_types=content_types,
                json_array_paths=json_array_paths[:20],
                keyword_hits={k: v for k, v in keyword_hits.items() if v},
                example_response_id=example.get("response_id"),
                example_url=example.get("url", ""),
            )
        )
    summaries.sort(key=score_summary, reverse=True)
    return [asdict(summary) for summary in summaries]


def score_summary(summary: EndpointSummary) -> tuple[int, int, int]:
    keyword_score = sum(summary.keyword_hits.values())
    json_score = len(summary.json_array_paths)
    count_score = summary.count
    return keyword_score, json_score, count_score


def contains_keyword(value: Any, keyword: str) -> bool:
    keyword = keyword.lower()
    if value is None:
        return False
    if isinstance(value, str):
        return keyword in value.lower()
    if isinstance(value, dict):
        return any(keyword in str(k).lower() or contains_keyword(v, keyword) for k, v in value.items())
    if isinstance(value, list):
        return any(contains_keyword(item, keyword) for item in value)
    return keyword in str(value).lower()


def find_array_paths(value: Any, path: str = "$") -> list[str]:
    paths: list[str] = []
    if isinstance(value, list):
        paths.append(path + "[*]")
        for item in value[:3]:
            paths.extend(find_array_paths(item, path + "[*]"))
    elif isinstance(value, dict):
        for key, item in value.items():
            safe_key = key if key.replace("_", "").isalnum() else f"['{key}']"
            child_path = f"{path}.{safe_key}" if safe_key == key else f"{path}{safe_key}"
            paths.extend(find_array_paths(item, child_path))
    return paths


def load_snapshots_from_result(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        if isinstance(data.get("snapshots"), list):
            return data["snapshots"]
        if isinstance(data.get("result"), dict) and isinstance(data["result"].get("snapshots"), list):
            return data["result"]["snapshots"]
    return []
