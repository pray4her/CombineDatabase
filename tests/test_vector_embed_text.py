"""Tests for checkpointed vector embedding helpers (no live API)."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from vector_embed_publications_v1 import (
    EMBEDDING_DIMENSION,
    EmbeddingCheckpoint,
    build_embedding_text,
    parse_embedding,
)


class BuildEmbeddingTextTests(unittest.TestCase):
    def test_requires_title(self) -> None:
        self.assertIsNone(build_embedding_text({"abstract": "only abstract"}))

    def test_omits_empty_sections(self) -> None:
        text = build_embedding_text(
            {
                "title": "Sample Title",
                "abstract": "Sample abstract body",
                "author_keywords": ["kw1", "kw2"],
                "keywords_plus": [],
                "research_areas": None,
            }
        )
        assert text is not None
        self.assertIn("Title:\nSample Title", text)
        self.assertIn("Abstract:\nSample abstract body", text)
        self.assertIn("Author Keywords:\nkw1; kw2", text)
        self.assertNotIn("Keywords Plus:", text)
        self.assertNotIn("Research Areas:", text)

    def test_truncates_abstract_before_dropping_keywords(self) -> None:
        text = build_embedding_text(
            {
                "title": "T",
                "abstract": "A" * 5000,
                "author_keywords": ["alpha"],
                "keywords_plus": ["beta"],
                "research_areas": ["gamma"],
            },
            max_chars=200,
        )
        assert text is not None
        self.assertIn("Title:\nT", text)
        self.assertIn("Author Keywords:\nalpha", text)
        self.assertIn("Keywords Plus:\nbeta", text)
        self.assertIn("Research Areas:\ngamma", text)
        self.assertLessEqual(len(text), 200)


class EmbeddingCheckpointTests(unittest.TestCase):
    def test_put_get_and_resume(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "embeddings.sqlite3"
            checkpoint = EmbeddingCheckpoint(db_path)
            vector = [float(i % 7) for i in range(EMBEDDING_DIMENSION)]
            checkpoint.put_many([("pub-1", vector)])
            checkpoint.close()

            resumed = EmbeddingCheckpoint(db_path)
            self.assertTrue(resumed.has("pub-1"))
            self.assertFalse(resumed.has("pub-missing"))
            loaded = resumed.get("pub-1")
            assert loaded is not None
            self.assertEqual(len(loaded), EMBEDDING_DIMENSION)
            self.assertEqual(loaded[0], vector[0])
            self.assertEqual(resumed.count(), 1)
            resumed.close()

    def test_parse_embedding_rejects_bad_size(self) -> None:
        self.assertIsNone(parse_embedding([1.0, 2.0]))
        self.assertIsNone(parse_embedding(None))


if __name__ == "__main__":
    unittest.main()
