from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from institution_rankings import InstitutionRankings, parse_rank_value, registrable_domain


class InstitutionRankingsTests(unittest.TestCase):
    def test_parse_rank_value_supports_tied_rank(self) -> None:
        self.assertEqual(parse_rank_value("=17"), 17)
        self.assertEqual(parse_rank_value("1"), 1)
        self.assertIsNone(parse_rank_value("N/A"))

    def test_matcher_handles_aliases_and_long_affiliations(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            base = Path(tmp_dir)
            qs_path = base / "qs.json"
            world_path = base / "world.json"
            qs_path.write_text(
                json.dumps(
                    [
                        {
                            "rank": "1",
                            "university": "Massachusetts Institute of Technology (MIT)",
                            "country": "United States",
                            "score": "100",
                            "domain": "mit.edu",
                        },
                        {
                            "rank": "2",
                            "university": "University of Oxford",
                            "country": "United Kingdom",
                            "score": "99",
                            "domain": "ox.ac.uk",
                        },
                        {
                            "rank": "3",
                            "university": "Monash University",
                            "country": "Australia",
                            "score": "98",
                            "domain": "monash.edu",
                        },
                        {
                            "rank": "10",
                            "university": "Alpha University",
                            "country": "China",
                            "score": "95",
                            "domain": "alpha.cn",
                        },
                        {
                            "rank": "11",
                            "university": "Alpha University",
                            "country": "United States",
                            "score": "94",
                            "domain": "alpha.edu",
                        },
                    ],
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            world_path.write_text("[]", encoding="utf-8")

            rankings = InstitutionRankings.from_paths(qs_path, world_path)

            self.assertEqual(
                rankings.match_institution("MIT", "United States")["qs_top200_rank"],
                1,
            )
            self.assertEqual(
                rankings.match_institution("Univ Oxford", "United Kingdom")["qs_top200_rank"],
                2,
            )
            self.assertEqual(
                rankings.match_institution(
                    "Department of Anatomy and Developmental Biology, Monash University, Clayton, Victoria, Australia",
                    "Australia",
                )["qs_top200_rank"],
                3,
            )
            self.assertIsNone(rankings.match_institution("Alpha Univ")["qs_top200_rank"])

    def test_world_top500_domain_matching_supports_company_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            base = Path(tmp_dir)
            qs_path = base / "qs.json"
            world_path = base / "world.json"
            qs_path.write_text("[]", encoding="utf-8")
            world_path.write_text(
                json.dumps(
                    [
                        {
                            "rank": "1",
                            "company": "沃尔玛\nWALMART",
                            "country": "美国",
                            "website": "http://www.stock.walmart.com/",
                            "domain": "walmart.com",
                        },
                        {
                            "rank": "3",
                            "company": "国家电网有限公司\nSTATE GRID",
                            "country": "中国",
                            "website": "http://www.sgcc.com.cn/",
                            "domain": "sgcc.com.cn",
                        },
                    ],
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            rankings = InstitutionRankings.from_paths(qs_path, world_path)

            self.assertEqual(
                rankings.best_world_top500_rank(["mail.walmart.com"], countries=["United States"])["world_top500_rank"],
                1,
            )
            self.assertEqual(
                rankings.best_world_top500_rank(["corp.sgcc.com.cn"], countries=["China"])["world_top500_rank"],
                3,
            )
            self.assertIsNone(rankings.best_world_top500_rank(["foo.example.com"])["world_top500_rank"])

    def test_registrable_domain_normalization(self) -> None:
        self.assertEqual(registrable_domain("https://www.stock.walmart.com/"), "walmart.com")
        self.assertEqual(registrable_domain("mail.walmart.com"), "walmart.com")
        self.assertEqual(registrable_domain("corp.sgcc.com.cn"), "sgcc.com.cn")


if __name__ == "__main__":
    unittest.main()
