"""Network snapshot types and matching helpers."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class NetworkSnapshot:
    url: str
    method: str
    status: int
    content_type: str
    body: Any
    request: dict[str, Any] | None = None
    response_id: str | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any], index: int = 0) -> "NetworkSnapshot":
        body = data.get("body", data.get("response_body"))
        if isinstance(body, str):
            try:
                body = json.loads(body)
            except json.JSONDecodeError:
                pass
        return cls(
            url=data.get("url", ""),
            method=data.get("method", "GET").upper(),
            status=int(data.get("status", 0)),
            content_type=data.get("content_type", ""),
            body=body,
            request=data.get("request"),
            response_id=data.get("response_id") or f"resp_{index}",
        )

    def matches(self, source: dict[str, Any]) -> bool:
        pattern = source.get("url_pattern")
        method = source.get("method")
        content_type = source.get("content_type")
        if pattern and pattern not in self.url:
            return False
        if method and method.upper() != self.method:
            return False
        if content_type and content_type.lower() not in self.content_type.lower():
            return False
        return True
