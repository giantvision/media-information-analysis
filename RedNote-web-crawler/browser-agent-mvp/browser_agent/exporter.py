"""Export extraction results to JSONL or CSV."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any


def extract_dataset_records(data: dict[str, Any], dataset: str) -> list[dict[str, Any]]:
    extraction = data.get("extraction") or data.get("result") or data
    datasets = extraction.get("datasets", {})
    if dataset in datasets:
        return datasets[dataset].get("records", [])
    if dataset == "default":
        return extraction.get("records", [])
    return []


def export_records(records: list[dict[str, Any]], output_path: str | Path, fmt: str | None = None) -> None:
    path = Path(output_path)
    fmt = fmt or path.suffix.lstrip(".").lower()
    if fmt == "jsonl":
        path.write_text(
            "\n".join(json.dumps(record, ensure_ascii=False) for record in records) + ("\n" if records else ""),
            encoding="utf-8",
        )
        return
    if fmt == "json":
        path.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")
        return
    if fmt == "csv":
        fieldnames = sorted({key for record in records for key in record.keys() if not key.startswith("_")})
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for record in records:
                writer.writerow({key: stringify(record.get(key)) for key in fieldnames})
        return
    raise ValueError(f"Unsupported export format: {fmt}")


def stringify(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)
