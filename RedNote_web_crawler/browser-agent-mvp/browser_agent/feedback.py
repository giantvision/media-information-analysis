"""Feedback Memory for extractor/runtime events."""

from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any


@dataclass(frozen=True)
class FeedbackEvent:
    event_type: str
    site: str
    recipe_version: str
    payload: dict[str, Any]
    created_at: str


class FeedbackMemory:
    def __init__(self) -> None:
        self.events: list[FeedbackEvent] = []

    def emit(self, event_type: str, site: str, recipe_version: str, payload: dict[str, Any]) -> FeedbackEvent:
        event = FeedbackEvent(
            event_type=event_type,
            site=site,
            recipe_version=recipe_version,
            payload=payload,
            created_at=datetime.now(timezone.utc).isoformat(),
        )
        self.events.append(event)
        return event

    def observe_extraction(self, result: dict[str, Any]) -> list[FeedbackEvent]:
        site = result["site"]
        version = result["recipe_version"]
        health = result["health"]
        emitted: list[FeedbackEvent] = []
        if health["healthy"]:
            emitted.append(self.emit("task_success", site, version, health))
        else:
            emitted.append(self.emit("recipe_health_failed", site, version, health))
        for field, rate in health.get("missing_rates", {}).items():
            if rate > 0:
                emitted.append(
                    self.emit(
                        "field_missing",
                        site,
                        version,
                        {"field": field, "missing_rate": rate},
                    )
                )
        return emitted

    def to_dicts(self) -> list[dict[str, Any]]:
        return [asdict(event) for event in self.events]
