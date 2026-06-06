"""Recipe DSL model and validation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


class RecipeError(ValueError):
    pass


@dataclass(frozen=True)
class Recipe:
    site: str
    version: str
    raw: dict[str, Any]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Recipe":
        site = data.get("site")
        version = data.get("version") or data.get("recipe_version")
        if not site:
            raise RecipeError("recipe.site is required")
        if not version:
            raise RecipeError("recipe.version is required")
        if not isinstance(data.get("network_sources", []), list):
            raise RecipeError("recipe.network_sources must be a list")
        if not isinstance(data.get("fields", {}), dict):
            raise RecipeError("recipe.fields must be an object")
        return cls(site=site, version=version, raw=data)

    @property
    def network_sources(self) -> list[dict[str, Any]]:
        return self.raw.get("network_sources", [])

    @property
    def datasets(self) -> list[dict[str, Any]]:
        datasets = self.raw.get("datasets")
        if isinstance(datasets, list) and datasets:
            return datasets
        return [
            {
                "name": "default",
                "network_sources": self.network_sources,
                "fields": self.fields,
                "dedupe": self.dedupe,
                "health_check": self.health_check,
            }
        ]

    @property
    def fields(self) -> dict[str, dict[str, Any]]:
        return self.raw.get("fields", {})

    @property
    def health_check(self) -> dict[str, Any]:
        return self.raw.get("health_check", {})

    @property
    def dedupe(self) -> dict[str, Any]:
        return self.raw.get("dedupe", {})
