"""Field transforms used by recipes."""

from __future__ import annotations

import hashlib
import re
from typing import Any


def parse_count(value: Any) -> int | float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return value
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    multiplier = 1
    if text.endswith("万"):
        multiplier = 10_000
        text = text[:-1]
    elif text.lower().endswith("k"):
        multiplier = 1_000
        text = text[:-1]
    elif text.lower().endswith("m"):
        multiplier = 1_000_000
        text = text[:-1]
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return None
    number = float(match.group(0))
    result = number * multiplier
    return int(result) if result.is_integer() else result


def strip(value: Any) -> Any:
    return value.strip() if isinstance(value, str) else value


def lower(value: Any) -> Any:
    return value.lower() if isinstance(value, str) else value


def content_hash(values: list[Any]) -> str:
    normalized = "|".join("" if v is None else str(v) for v in values)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


TRANSFORMS = {
    "parse_count": parse_count,
    "strip": strip,
    "lower": lower,
}


def apply_transform(value: Any, transform: str | None) -> Any:
    if not transform:
        return value
    func = TRANSFORMS.get(transform)
    if not func:
        raise ValueError(f"Unknown transform: {transform}")
    return func(value)
