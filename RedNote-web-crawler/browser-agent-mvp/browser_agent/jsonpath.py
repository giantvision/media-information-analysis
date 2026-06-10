"""Small JSONPath subset for recipe-driven extraction.

Supported forms:
- $.a.b.c
- $.a[0].b
- $.items[*].id
- $['a'].b

This intentionally avoids an external dependency for the MVP. It is not a
complete JSONPath implementation.
"""

from __future__ import annotations

import re
from typing import Any, Iterable


_TOKEN_RE = re.compile(
    r"""
    (?:
      \.([A-Za-z_][A-Za-z0-9_-]*)      # .name
      |\['([^']+)'\]                   # ['name']
      |\[(\d+|\*)\]                    # [0] or [*]
    )
    """,
    re.VERBOSE,
)


class JsonPathError(ValueError):
    pass


def _parse(path: str) -> list[str | int]:
    if path == "$":
        return []
    if not path.startswith("$"):
        raise JsonPathError(f"JSONPath must start with '$': {path}")
    pos = 1
    tokens: list[str | int] = []
    while pos < len(path):
        match = _TOKEN_RE.match(path, pos)
        if not match:
            raise JsonPathError(f"Unsupported JSONPath near {path[pos:]} in {path}")
        dot_name, quoted_name, index = match.groups()
        if dot_name is not None:
            tokens.append(dot_name)
        elif quoted_name is not None:
            tokens.append(quoted_name)
        elif index == "*":
            tokens.append("*")
        else:
            tokens.append(int(index))
        pos = match.end()
    return tokens


def query(data: Any, path: str) -> list[Any]:
    values = [data]
    for token in _parse(path):
        next_values: list[Any] = []
        for value in values:
            if token == "*":
                if isinstance(value, list):
                    next_values.extend(value)
                elif isinstance(value, dict):
                    next_values.extend(value.values())
            elif isinstance(token, int):
                if isinstance(value, list) and 0 <= token < len(value):
                    next_values.append(value[token])
            else:
                if isinstance(value, dict) and token in value:
                    next_values.append(value[token])
        values = next_values
        if not values:
            break
    return values


def first(data: Any, path: str, default: Any = None) -> Any:
    values = query(data, path)
    if not values:
        return default
    return values[0]


def flatten(items: Iterable[Any]) -> list[Any]:
    out: list[Any] = []
    for item in items:
        if isinstance(item, list):
            out.extend(item)
        else:
            out.append(item)
    return out
