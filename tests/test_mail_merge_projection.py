from __future__ import annotations

import sqlite3
from contextlib import closing
import tempfile
import unittest
from pathlib import Path

from mail_merge_adapter import load_candidates_from_records_db
from mail_merge_projection import PROJECTION_COLUMNS, build_mail_merge_projection


class MailMergeProjectionTests(unittest.TestCase):
    def candidate(self, **overrides: object) -> dict[str, object]:
        candidate: dict[str, object] = {
            "full_name": "Dr. Ada Lovelace",
            "short_name": "Ada",
            "email": "ada@example.org",
            "similarity": 0.8,
            "email_validity": "unknown",
            "research_areas": ["Machine Learning", "Networks"],
            "wos_categories": ["Computer Science"],
            "institution": "Raw University",
            "institution_norm": "Normalized University",
            "country": "GB",
            "paper_evidences": [{"title": "Anchor", "source_title": "Journal", "publication_year": 2024}],
        }
        candidate.update(overrides)
        return candidate

    def test_gate_omits_incomplete_invalid_and_low_similarity_contacts(self) -> None:
        candidates = [
            self.candidate(full_name="", short_name=""),
            self.candidate(email=""),
            self.candidate(paper_evidences=[{"title": ""}]),
            self.candidate(email_validity="smtp_fail"),
            self.candidate(similarity=0.59),
            self.candidate(email="valid@example.org", email_validity="unknown"),
            self.candidate(email="passed@example.org", email_validity="passed"),
        ]
        rows = build_mail_merge_projection(candidates)
        self.assertEqual([row["email"] for row in rows], ["valid@example.org", "passed@example.org"])

    def test_selection_uses_required_ranking_order(self) -> None:
        candidate = self.candidate(paper_evidences=[
            {"title": "new non-corresponding", "publication_year": 2025, "author_order": 1, "times_cited": 99},
            {"title": "older corresponding", "publication_year": 2020, "author_order": 8, "is_corresponding_author": True},
            {"title": "earlier author", "publication_year": 2025, "author_order": 1, "is_corresponding_author": True},
            {"title": "later author", "publication_year": 2026, "author_order": 2, "is_corresponding_author": True},
        ])
        row = build_mail_merge_projection([candidate])[0]
        self.assertEqual(row["anchor_title"], "earlier author")

        candidate["paper_evidences"] = [
            {"title": "older", "publication_year": 2022, "author_order": 1, "times_cited": 100},
            {"title": "newer", "publication_year": 2023, "author_order": 1, "times_cited": 1},
            {"title": "more cited", "publication_year": 2023, "author_order": 1, "times_cited": 10, "similarity": 0.7},
            {"title": "more similar", "publication_year": 2023, "author_order": 1, "times_cited": 10, "similarity": 0.9},
        ]
        self.assertEqual(build_mail_merge_projection([candidate])[0]["anchor_title"], "more similar")

    def test_fallbacks_and_default_contract(self) -> None:
        row = build_mail_merge_projection([self.candidate(
            full_name="", short_name="Ada", research_areas=[], wos_categories="Physics | Astronomy",
            institution_norm="", institution="Original Institute",
        )])[0]
        self.assertEqual(row["recipient_name"], "Ada")
        self.assertEqual(row["research_area_primary"], "Physics")
        self.assertEqual(row["institution"], "Original Institute")
        self.assertEqual(tuple(row), PROJECTION_COLUMNS)
        self.assertNotIn("ethnic_chinese", row)
        self.assertNotIn("qs_rank", row)

    def test_equal_evidence_uses_input_order_deterministically(self) -> None:
        candidate = self.candidate(paper_evidences=[
            {"title": "first", "author_order": 1, "publication_year": 2024, "times_cited": 2, "similarity": 0.8},
            {"title": "second", "author_order": 1, "publication_year": 2024, "times_cited": 2, "similarity": 0.8},
        ])
        rows = [build_mail_merge_projection([candidate])[0] for _ in range(3)]
        self.assertEqual([row["anchor_title"] for row in rows], ["first", "first", "first"])


class MailMergeAdapterSmokeTests(unittest.TestCase):
    def test_loads_and_groups_records_by_lowercase_email(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "records.db"
            with closing(sqlite3.connect(db_path)) as connection, connection:
                connection.execute("""
                    CREATE TABLE records (
                        id TEXT, short_name TEXT, full_name TEXT, email TEXT, country TEXT,
                        institution TEXT, institution_norm TEXT, research_areas TEXT,
                        wos_categories TEXT, similarity REAL, email_validity TEXT, title TEXT,
                        source_title TEXT, publication_year INTEGER, author_order INTEGER,
                        is_corresponding_author TEXT, times_cited INTEGER
                    )
                """)
                connection.executemany("INSERT INTO records VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", [
                    ("1", "Ada", "Ada Lovelace", "ADA@example.org", "GB", "Raw", "Norm", "AI; Math", None, .8, "passed", "First", "J", 2023, 2, "false", 4),
                    ("2", "Ada", "Ada Lovelace", "ada@example.org", "GB", "Raw", "Norm", "AI; Math", None, .9, "passed", "Second", "J", 2024, 1, "true", 5),
                ])
            candidates = load_candidates_from_records_db(db_path)
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["research_areas"], ["AI", "Math"])
        self.assertTrue(candidates[0]["paper_evidences"][1]["is_corresponding_author"])
        self.assertEqual(build_mail_merge_projection(candidates)[0]["anchor_title"], "Second")


if __name__ == "__main__":
    unittest.main()

