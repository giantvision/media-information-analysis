import json
import unittest
from pathlib import Path

from browser_agent.extractor import NetworkExtractor
from browser_agent.network import NetworkSnapshot
from browser_agent.recipe import Recipe


ROOT = Path(__file__).resolve().parents[1]


def load_json(name):
    return json.loads((ROOT / "examples" / name).read_text(encoding="utf-8"))


class ExtractorTest(unittest.TestCase):
    def test_extracts_records_with_audit_and_health(self):
        recipe = Recipe.from_dict(load_json("search_recipe.json"))
        snapshots = [
            NetworkSnapshot.from_dict(item, index=i)
            for i, item in enumerate(load_json("search_snapshots.json"))
        ]

        result = NetworkExtractor(recipe).extract(snapshots)

        self.assertTrue(result["health"]["healthy"])
        self.assertEqual(len(result["records"]), 2)
        self.assertEqual(result["records"][0]["item_id"], "note_001")
        self.assertEqual(result["records"][0]["like_count"], 12000)
        self.assertEqual(result["records"][0]["_source_audit"]["title"], "network.search_feed.$.title")
        self.assertEqual(result["records"][0]["_item_key"], "note_001")
        self.assertIn("_content_hash", result["records"][0])

    def test_extracts_named_datasets(self):
        recipe = Recipe.from_dict(
            {
                "site": "demo",
                "version": "1",
                "datasets": [
                    {
                        "name": "post",
                        "network_sources": [
                            {
                                "name": "post_api",
                                "url_pattern": "/post",
                                "method": "GET",
                                "content_type": "json",
                                "item_path": "$.data.items[*]",
                            }
                        ],
                        "fields": {"post_id": {"source": "network", "path": "$.id"}},
                        "dedupe": {"primary_key": "post_id"},
                        "health_check": {"required_fields": ["post_id"], "min_items_per_page": 1},
                    },
                    {
                        "name": "comments",
                        "network_sources": [
                            {
                                "name": "comment_api",
                                "url_pattern": "/comments",
                                "method": "GET",
                                "content_type": "json",
                                "item_path": "$.data.comments[*]",
                            }
                        ],
                        "fields": {"comment_id": {"source": "network", "path": "$.id"}},
                        "dedupe": {"primary_key": "comment_id"},
                        "health_check": {"required_fields": ["comment_id"], "min_items_per_page": 1},
                    },
                ],
            }
        )
        snapshots = [
            NetworkSnapshot.from_dict(
                {
                    "url": "https://example.test/post",
                    "method": "GET",
                    "status": 200,
                    "content_type": "application/json",
                    "body": {"data": {"items": [{"id": "p1"}]}},
                }
            ),
            NetworkSnapshot.from_dict(
                {
                    "url": "https://example.test/comments",
                    "method": "GET",
                    "status": 200,
                    "content_type": "application/json",
                    "body": {"data": {"comments": [{"id": "c1"}, {"id": "c2"}]}},
                },
                index=1,
            ),
        ]

        result = NetworkExtractor(recipe).extract(snapshots)

        self.assertTrue(result["health"]["healthy"])
        self.assertEqual(len(result["datasets"]["post"]["records"]), 1)
        self.assertEqual(len(result["datasets"]["comments"]["records"]), 2)
        self.assertEqual(result["datasets"]["comments"]["records"][0]["_dataset"], "comments")


if __name__ == "__main__":
    unittest.main()
