import unittest

from browser_agent import jsonpath


class JsonPathTest(unittest.TestCase):
    def test_jsonpath_query_list_items(self):
        data = {"data": {"items": [{"id": 1}, {"id": 2}]}}

        self.assertEqual(jsonpath.query(data, "$.data.items[*].id"), [1, 2])

    def test_jsonpath_first_missing_returns_default(self):
        self.assertEqual(jsonpath.first({"a": 1}, "$.missing", default="x"), "x")


if __name__ == "__main__":
    unittest.main()
