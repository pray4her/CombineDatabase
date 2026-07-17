from __future__ import annotations

import unittest

from etl_transform_schema_v1 import (
    CHINESE_IDENTITY_DOMESTIC,
    CHINESE_IDENTITY_FOREIGN,
    CHINESE_IDENTITY_OVERSEAS,
    classify_chinese_identity,
    is_likely_chinese_name,
    parse_name_parts,
)
from match_person_clusters_v1 import merge_chinese_identity


class ChineseIdentityTests(unittest.TestCase):
    def test_is_likely_chinese_name_uses_existing_surname_rules(self) -> None:
        self.assertTrue(is_likely_chinese_name(parse_name_parts("Wang, Li")))
        self.assertTrue(is_likely_chinese_name(parse_name_parts("Lim, Wei")))
        self.assertFalse(is_likely_chinese_name(parse_name_parts("Kim, Min")))

    def test_classify_domestic_chinese_overrides_name_signal(self) -> None:
        self.assertEqual(
            classify_chinese_identity(parse_name_parts("Nguyen, An"), ["China"]),
            CHINESE_IDENTITY_DOMESTIC,
        )

    def test_classify_overseas_chinese_for_non_china_non_blocked_country(self) -> None:
        self.assertEqual(
            classify_chinese_identity(parse_name_parts("Wang, Li"), ["United States"]),
            CHINESE_IDENTITY_OVERSEAS,
        )

    def test_classify_foreign_for_blocked_asian_country(self) -> None:
        self.assertEqual(
            classify_chinese_identity(parse_name_parts("Lee, Min"), ["South Korea"]),
            CHINESE_IDENTITY_FOREIGN,
        )
        self.assertEqual(
            classify_chinese_identity(parse_name_parts("Wang, Li"), ["Japan"]),
            CHINESE_IDENTITY_FOREIGN,
        )

    def test_classify_foreign_without_country_evidence(self) -> None:
        self.assertEqual(
            classify_chinese_identity(parse_name_parts("Wang, Li"), []),
            CHINESE_IDENTITY_FOREIGN,
        )

    def test_classify_overseas_when_any_country_is_non_blocked(self) -> None:
        self.assertEqual(
            classify_chinese_identity(parse_name_parts("Wang, Li"), ["Japan", "United States"]),
            CHINESE_IDENTITY_OVERSEAS,
        )

    def test_classify_domestic_when_china_appears_with_other_countries(self) -> None:
        self.assertEqual(
            classify_chinese_identity(parse_name_parts("Wang, Li"), ["China", "United States"]),
            CHINESE_IDENTITY_DOMESTIC,
        )

    def test_merge_chinese_identity_priority(self) -> None:
        self.assertEqual(
            merge_chinese_identity([CHINESE_IDENTITY_FOREIGN, CHINESE_IDENTITY_OVERSEAS]),
            CHINESE_IDENTITY_OVERSEAS,
        )
        self.assertEqual(
            merge_chinese_identity([CHINESE_IDENTITY_FOREIGN, CHINESE_IDENTITY_DOMESTIC]),
            CHINESE_IDENTITY_DOMESTIC,
        )
        self.assertEqual(
            merge_chinese_identity([CHINESE_IDENTITY_FOREIGN]),
            CHINESE_IDENTITY_FOREIGN,
        )


if __name__ == "__main__":
    unittest.main()
