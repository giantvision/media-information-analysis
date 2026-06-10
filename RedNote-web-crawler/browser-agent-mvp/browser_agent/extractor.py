"""Recipe-driven network extractor with field-level fallback and audit."""

from __future__ import annotations

from typing import Any, Callable

from . import jsonpath
from .network import NetworkSnapshot
from .recipe import Recipe
from .transforms import apply_transform, content_hash

DomQuery = Callable[[str, str | None], Any]


class NetworkExtractor:
    def __init__(self, recipe: Recipe):
        self.recipe = recipe

    def extract(
        self,
        snapshots: list[NetworkSnapshot],
        dom_query: DomQuery | None = None,
        runtime_state: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        datasets = {}
        all_records = []
        for dataset in self.recipe.datasets:
            source_items = self._collect_items(snapshots, dataset)
            records = []
            for item in source_items:
                record = self._extract_record(item, dom_query, runtime_state, dataset)
                self._attach_dedupe(record, dataset)
                records.append(record)
            health = self._health(records, dataset)
            datasets[dataset.get("name", "default")] = {
                "records": records,
                "health": health,
            }
            all_records.extend(records)
        default = datasets.get("default")
        records = default["records"] if default else all_records
        health = default["health"] if default else self._combined_health(datasets)
        return {
            "site": self.recipe.site,
            "recipe_version": self.recipe.version,
            "records": records,
            "datasets": datasets,
            "health": health,
        }

    def _collect_items(self, snapshots: list[NetworkSnapshot], dataset: dict[str, Any]) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for source in dataset.get("network_sources", []):
            source_name = source.get("name", "network")
            item_path = source.get("item_path") or source.get("data_path") or "$"
            for snapshot in snapshots:
                if not snapshot.matches(source):
                    continue
                if not isinstance(snapshot.body, (dict, list)):
                    continue
                matches = jsonpath.query(snapshot.body, item_path)
                for match in matches:
                    items.append(
                        {
                            "_source_name": source_name,
                            "_response_id": snapshot.response_id,
                            "_item": match,
                            "_response_body": snapshot.body,
                        }
                    )
        return items

    def _extract_record(
        self,
        source_item: dict[str, Any],
        dom_query: DomQuery | None,
        runtime_state: dict[str, Any] | None,
        dataset: dict[str, Any],
    ) -> dict[str, Any]:
        record: dict[str, Any] = {
            "_source_audit": {},
            "_raw_refs": {"response_id": source_item["_response_id"]},
            "_recipe_version": self.recipe.version,
        }
        record["_dataset"] = dataset.get("name", "default")
        for field_name, spec in dataset.get("fields", {}).items():
            value, audit = self._extract_field(field_name, spec, source_item, dom_query, runtime_state)
            record[field_name] = value
            record["_source_audit"][field_name] = audit
        return record

    def _extract_field(
        self,
        field_name: str,
        spec: dict[str, Any],
        source_item: dict[str, Any],
        dom_query: DomQuery | None,
        runtime_state: dict[str, Any] | None,
    ) -> tuple[Any, str | None]:
        candidates = [spec] + list(spec.get("fallback", []))
        for candidate in candidates:
            value, audit = self._try_candidate(field_name, candidate, source_item, dom_query, runtime_state)
            value = apply_transform(value, candidate.get("transform") or spec.get("transform"))
            if value not in (None, "", []):
                return value, audit
        return None, None

    def _try_candidate(
        self,
        field_name: str,
        spec: dict[str, Any],
        source_item: dict[str, Any],
        dom_query: DomQuery | None,
        runtime_state: dict[str, Any] | None,
    ) -> tuple[Any, str | None]:
        source = spec.get("source", "network")
        if source == "network":
            path = spec.get("path")
            if not path:
                return None, None
            value = jsonpath.first(source_item["_item"], path)
            audit = f"network.{source_item['_source_name']}.{path}"
            return value, audit
        if source == "runtime":
            if not runtime_state:
                return None, None
            path = spec.get("path")
            if not path:
                return None, None
            return jsonpath.first(runtime_state, path), f"runtime.{path}"
        if source == "dom":
            if not dom_query:
                return None, None
            selector = spec.get("selector")
            if not selector:
                return None, None
            attr = spec.get("attr")
            return dom_query(selector, attr), f"dom.{selector}"
        return None, None

    def _attach_dedupe(self, record: dict[str, Any], dataset: dict[str, Any]) -> None:
        dedupe = dataset.get("dedupe", self.recipe.dedupe)
        primary_key = dedupe.get("primary_key")
        if primary_key and record.get(primary_key) not in (None, ""):
            record["_item_key"] = str(record[primary_key])
        hash_fields = dedupe.get("hash_fields", [])
        if hash_fields:
            record["_content_hash"] = content_hash([record.get(field) for field in hash_fields])

    def _health(self, records: list[dict[str, Any]], dataset: dict[str, Any]) -> dict[str, Any]:
        check = dataset.get("health_check", self.recipe.health_check)
        required = check.get("required_fields", [])
        min_items = int(check.get("min_items_per_page", 0))
        max_missing_rate = float(check.get("max_missing_rate", 1.0))
        missing: dict[str, int] = {field: 0 for field in required}
        for record in records:
            for field in required:
                if record.get(field) in (None, "", []):
                    missing[field] += 1
        total = len(records)
        missing_rates = {
            field: (count / total if total else 1.0) for field, count in missing.items()
        }
        healthy = total >= min_items and all(rate <= max_missing_rate for rate in missing_rates.values())
        return {
            "healthy": healthy,
            "record_count": total,
            "missing_rates": missing_rates,
        }

    def _combined_health(self, datasets: dict[str, dict[str, Any]]) -> dict[str, Any]:
        total = sum(item["health"]["record_count"] for item in datasets.values())
        healthy = all(item["health"]["healthy"] for item in datasets.values())
        return {
            "healthy": healthy,
            "record_count": total,
            "missing_rates": {},
        }
