from __future__ import annotations

import unittest

from search_app.catalog import SearchCatalog
from search_app.query_builder import build_search_request


class QueryBuilderTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.catalog = SearchCatalog()

    def test_mixed_fields_generate_expected_dsl(self) -> None:
        result = build_search_request(
            entity_key="publication",
            query_tree={
                "type": "group",
                "logic": "and",
                "children": [
                    {"type": "rule", "field": "doi", "operator": "eq", "value": "10.1000/demo"},
                    {
                        "type": "rule",
                        "field": "publication_year",
                        "operator": "between",
                        "range": {"from": 2020, "to": 2024},
                    },
                    {"type": "rule", "field": "title", "operator": "match_phrase", "value": "immune response"},
                ],
            },
            select_fields=["publication_id", "title", "publication_year"],
            sort=[{"field": "publication_year", "order": "desc"}],
            page=1,
            page_size=20,
            search_after=None,
            catalog=self.catalog,
        )
        must_clauses = result.query["bool"]["must"]
        self.assertEqual(len(must_clauses), 3)
        self.assertIn("match_phrase", must_clauses[2])
        self.assertEqual(result.sql_mode, "sql+filter")
        self.assertIn("SELECT publication_id, title, publication_year FROM publication_v1", result.sql)

    def test_nested_rules_are_grouped_into_nested_query(self) -> None:
        result = build_search_request(
            entity_key="person",
            query_tree={
                "type": "group",
                "logic": "and",
                "children": [
                    {
                        "type": "rule",
                        "field": "source_refs.source_file",
                        "operator": "eq",
                        "value": "scholars.xlsx",
                        "nested_path": "source_refs",
                    },
                    {
                        "type": "rule",
                        "field": "source_refs.row_index",
                        "operator": "gte",
                        "value": 10,
                        "nested_path": "source_refs",
                    },
                ],
            },
            select_fields=["person_id", "name_original"],
            sort=[],
            page=1,
            page_size=20,
            search_after=None,
            catalog=self.catalog,
        )
        self.assertIn("nested", result.query)
        nested_query = result.query["nested"]
        self.assertEqual(nested_query["path"], "source_refs")
        self.assertEqual(nested_query["query"]["bool"]["must"][0]["term"]["source_refs.source_file"], "scholars.xlsx")
        self.assertEqual(
            nested_query["query"]["bool"]["must"][1]["range"]["source_refs.row_index"]["gte"],
            10,
        )

    def test_unsortable_field_is_ignored_with_warning(self) -> None:
        result = build_search_request(
            entity_key="publication",
            query_tree={"type": "group", "logic": "and", "children": []},
            select_fields=["publication_id", "title"],
            sort=[{"field": "title", "order": "asc"}],
            page=1,
            page_size=20,
            search_after=None,
            catalog=self.catalog,
        )
        self.assertTrue(result.warnings)
        self.assertTrue(any("不支持排序" in item for item in result.warnings))

    def test_empty_rule_is_treated_as_match_all(self) -> None:
        result = build_search_request(
            entity_key="person",
            query_tree={
                "type": "group",
                "logic": "and",
                "children": [{"type": "rule", "field": "", "operator": ""}],
            },
            select_fields=["person_id", "name_original"],
            sort=[],
            page=1,
            page_size=20,
            search_after=None,
            catalog=self.catalog,
        )
        self.assertEqual(result.query, {"match_all": {}})
        self.assertEqual(result.body["query"], {"match_all": {}})


if __name__ == "__main__":
    unittest.main()
