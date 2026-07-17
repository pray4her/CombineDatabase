from __future__ import annotations

import unittest

from search_app.catalog import SearchCatalog


class SearchCatalogTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.catalog = SearchCatalog()

    def test_all_entities_are_loaded(self) -> None:
        entities = self.catalog.list_entities()
        self.assertEqual(len(entities), 7)
        self.assertEqual({item["entity_key"] for item in entities}, {
            "person",
            "publication",
            "person_publication",
            "author_occurrence",
            "author_identifier_claim",
            "author_affiliation_claim",
            "author_email_claim",
        })

    def test_person_fields_include_nested_source_refs(self) -> None:
        entity = self.catalog.get_entity("person")
        field_paths = {field_meta.field_path for field_meta in entity.fields}
        self.assertIn("source_refs.source_file", field_paths)
        self.assertIn("chinese_identity", field_paths)
        self.assertIn("qs_top200_rank", field_paths)
        self.assertIn("world_top500_rank", field_paths)
        nested_field = self.catalog.get_field("person", "source_refs.source_file")
        self.assertTrue(nested_field.is_nested)
        self.assertEqual(nested_field.nested_path, "source_refs")

    def test_author_email_claim_includes_world_top500_rank(self) -> None:
        entity = self.catalog.get_entity("author_email_claim")
        field_paths = {field_meta.field_path for field_meta in entity.fields}
        self.assertIn("world_top500_rank", field_paths)

    def test_author_occurrence_includes_chinese_identity(self) -> None:
        entity = self.catalog.get_entity("author_occurrence")
        field_paths = {field_meta.field_path for field_meta in entity.fields}
        self.assertIn("chinese_identity", field_paths)

    def test_text_field_is_not_sortable(self) -> None:
        title_field = self.catalog.get_field("publication", "title")
        self.assertEqual(title_field.field_type, "text")
        self.assertFalse(title_field.sortable)


if __name__ == "__main__":
    unittest.main()
