"""Command line entry points for the Browser Agent MVP."""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from .extractor import NetworkExtractor
from .feedback import FeedbackMemory
from .network import NetworkSnapshot
from .recipe import Recipe


def _load_json(path: Path):
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def extract_cmd(args: argparse.Namespace) -> int:
    recipe = Recipe.from_dict(_load_json(Path(args.recipe)))
    raw_snapshots = _load_json(Path(args.snapshots))
    if isinstance(raw_snapshots, dict):
        raw_snapshots = raw_snapshots.get("snapshots", [])
    snapshots = [NetworkSnapshot.from_dict(item, index=i) for i, item in enumerate(raw_snapshots)]
    extractor = NetworkExtractor(recipe)
    result = extractor.extract(snapshots)
    memory = FeedbackMemory()
    events = memory.observe_extraction(result)
    output = {"result": result, "feedback_events": [event.__dict__ for event in events]}
    print(json.dumps(output, ensure_ascii=False, indent=2))
    return 0


def validate_recipe_cmd(args: argparse.Namespace) -> int:
    recipe = Recipe.from_dict(_load_json(Path(args.recipe)))
    print(json.dumps({"valid": True, "site": recipe.site, "version": recipe.version}, ensure_ascii=False))
    return 0


def record_cmd(args: argparse.Namespace) -> int:
    from .recorder import record_url

    snapshots = asyncio.run(record_url(args.url, args.output, wait_ms=args.wait_ms))
    print(json.dumps({"saved": args.output, "snapshot_count": len(snapshots)}, ensure_ascii=False))
    return 0


def run_cmd(args: argparse.Namespace) -> int:
    from .runtime import run_sync

    recipe = Recipe.from_dict(_load_json(Path(args.recipe)))
    result = run_sync(
        recipe=recipe,
        url=args.url,
        output_path=args.output,
        user_data_dir=args.user_data_dir,
        wait_ms=args.wait_ms,
        headless=args.headless,
    )
    summary = {
        "url": result["url"],
        "snapshot_count": result["snapshot_count"],
        "health": result["extraction"]["health"],
        "datasets": {
            name: dataset["health"]
            for name, dataset in result["extraction"]["datasets"].items()
        },
        "saved": args.output,
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


def analyze_snapshots_cmd(args: argparse.Namespace) -> int:
    from .analyzer import analyze_snapshots, load_snapshots_from_result

    data = _load_json(Path(args.input))
    snapshots = load_snapshots_from_result(data)
    keywords = args.keyword or None
    summaries = analyze_snapshots(snapshots, keywords=keywords)
    if args.limit:
        summaries = summaries[: args.limit]
    print(json.dumps({"endpoint_count": len(summaries), "endpoints": summaries}, ensure_ascii=False, indent=2))
    return 0


def export_cmd(args: argparse.Namespace) -> int:
    from .exporter import export_records, extract_dataset_records

    data = _load_json(Path(args.input))
    records = extract_dataset_records(data, args.dataset)
    export_records(records, args.output, fmt=args.format)
    print(json.dumps({"dataset": args.dataset, "record_count": len(records), "saved": args.output}, ensure_ascii=False))
    return 0


def open_session_cmd(args: argparse.Namespace) -> int:
    from .runtime import open_session_sync

    open_session_sync(args.url, args.user_data_dir, headless=args.headless)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(prog="browser-agent")
    subparsers = parser.add_subparsers(required=True)

    validate = subparsers.add_parser("validate-recipe")
    validate.add_argument("recipe")
    validate.set_defaults(func=validate_recipe_cmd)

    extract = subparsers.add_parser("extract")
    extract.add_argument("--recipe", required=True)
    extract.add_argument("--snapshots", required=True)
    extract.set_defaults(func=extract_cmd)

    record = subparsers.add_parser("record")
    record.add_argument("url")
    record.add_argument("--output", required=True)
    record.add_argument("--wait-ms", type=int, default=3000)
    record.set_defaults(func=record_cmd)

    run = subparsers.add_parser("run")
    run.add_argument("url")
    run.add_argument("--recipe", required=True)
    run.add_argument("--output")
    run.add_argument("--user-data-dir")
    run.add_argument("--wait-ms", type=int, default=3000)
    run.add_argument("--headless", action="store_true")
    run.set_defaults(func=run_cmd)

    analyze = subparsers.add_parser("analyze-snapshots")
    analyze.add_argument("input")
    analyze.add_argument("--keyword", action="append")
    analyze.add_argument("--limit", type=int, default=20)
    analyze.set_defaults(func=analyze_snapshots_cmd)

    export = subparsers.add_parser("export")
    export.add_argument("input")
    export.add_argument("--dataset", default="default")
    export.add_argument("--output", required=True)
    export.add_argument("--format", choices=["json", "jsonl", "csv"])
    export.set_defaults(func=export_cmd)

    open_session_parser = subparsers.add_parser("open-session")
    open_session_parser.add_argument("url")
    open_session_parser.add_argument("--user-data-dir", required=True)
    open_session_parser.add_argument("--headless", action="store_true")
    open_session_parser.set_defaults(func=open_session_cmd)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
