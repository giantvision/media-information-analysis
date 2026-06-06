import json
import tempfile
import unittest
from pathlib import Path

from browser_agent.analyzer import analyze_snapshots
from browser_agent.exporter import export_records, extract_dataset_records


class AnalyzerExporterTest(unittest.TestCase):
    def test_analyze_snapshots_finds_candidate_array_paths(self):
        snapshots = [
            {
                "response_id": "r1",
                "url": "https://www.xiaohongshu.com/api/sns/web/v2/comment/page?note_id=n1",
                "method": "GET",
                "status": 200,
                "content_type": "application/json",
                "body": {
                    "data": {
                        "cursor": "c2",
                        "comments": [
                            {"id": "c1", "content": "hello"},
                            {"id": "c2", "content": "world"},
                        ],
                    }
                },
            }
        ]

        result = analyze_snapshots(snapshots, keywords=["comment", "cursor"])

        self.assertEqual(result[0]["path"], "/api/sns/web/v2/comment/page")
        self.assertIn("$.data.comments[*]", result[0]["json_array_paths"])
        self.assertGreater(result[0]["keyword_hits"]["comment"], 0)

    def test_export_dataset_records_to_jsonl(self):
        data = {
            "extraction": {
                "datasets": {
                    "comments": {
                        "records": [
                            {"comment_id": "c1", "content": "hello", "_source_audit": {}},
                            {"comment_id": "c2", "content": "world", "_source_audit": {}},
                        ]
                    }
                }
            }
        }
        records = extract_dataset_records(data, "comments")
        with tempfile.TemporaryDirectory() as temp_dir:
            output = Path(temp_dir) / "comments.jsonl"
            export_records(records, output)
            lines = output.read_text(encoding="utf-8").splitlines()

        self.assertEqual(len(lines), 2)
        self.assertEqual(json.loads(lines[0])["comment_id"], "c1")


if __name__ == "__main__":
    unittest.main()
