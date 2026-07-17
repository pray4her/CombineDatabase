from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set
from urllib.parse import urlparse


SPACE_RE = re.compile(r"\s+")
PARENS_RE = re.compile(r"\(([^()]*)\)")
SPLIT_RE = re.compile(r"[;,]")
LABEL_RE = re.compile(r"[^a-z0-9-]+")

COUNTRY_ALIASES = {
    "united states": "United States",
    "united states of america": "United States",
    "peoples r china": "China",
    "people s r china": "China",
    "china": "China",
    "中华人民共和国": "China",
    "中国": "China",
    "hong kong": "Hong Kong",
    "macao": "Macau",
    "macau": "Macau",
    "usa": "United States",
    "u s a": "United States",
    "us": "United States",
    "美国": "United States",
    "uk": "United Kingdom",
    "u k": "United Kingdom",
    "england": "United Kingdom",
    "scotland": "United Kingdom",
    "wales": "United Kingdom",
    "英国": "United Kingdom",
    "德国": "Germany",
    "法国": "France",
    "日本": "Japan",
    "韩国": "South Korea",
    "荷兰": "Netherlands",
    "瑞士": "Switzerland",
    "瑞典": "Sweden",
    "加拿大": "Canada",
    "澳大利亚": "Australia",
    "印度": "India",
    "新加坡": "Singapore",
    "沙特阿拉伯": "Saudi Arabia",
    "阿联酋": "United Arab Emirates",
    "阿拉伯联合酋长国": "United Arab Emirates",
    "爱尔兰": "Ireland",
    "西班牙": "Spain",
    "意大利": "Italy",
    "比利时": "Belgium",
    "丹麦": "Denmark",
    "挪威": "Norway",
    "芬兰": "Finland",
    "巴西": "Brazil",
    "墨西哥": "Mexico",
    "土耳其": "Turkey",
    "泰国": "Thailand",
    "马来西亚": "Malaysia",
    "印度尼西亚": "Indonesia",
}

TOKEN_EXPANSIONS = {
    "univ": "university",
    "inst": "institute",
    "tech": "technology",
    "coll": "college",
    "acad": "academy",
    "med": "medical",
    "sci": "science",
    "ctr": "center",
    "dept": "department",
    "sch": "school",
}

STOPWORDS = {"of", "the", "at", "for", "and"}
MULTI_PART_PUBLIC_SUFFIXES = {
    "com.cn",
    "edu.cn",
    "gov.cn",
    "org.cn",
    "ac.uk",
    "co.uk",
    "org.uk",
    "gov.uk",
    "com.au",
    "edu.au",
    "org.au",
    "com.br",
    "com.mx",
    "co.jp",
    "com.sg",
}


def normalize_text(value: object) -> Optional[str]:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    normalized = unicodedata.normalize("NFKD", raw)
    cleaned: List[str] = []
    for ch in normalized:
        if unicodedata.combining(ch):
            continue
        if ch.isalnum():
            cleaned.append(ch.lower())
        else:
            cleaned.append(" ")
    result = SPACE_RE.sub(" ", "".join(cleaned)).strip()
    return result or None


def normalize_country(value: object) -> Optional[str]:
    normalized = normalize_text(value)
    if not normalized:
        return None
    return COUNTRY_ALIASES.get(normalized, str(value).strip() or None)


def normalize_domain(value: object) -> Optional[str]:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    candidate = raw
    if "://" in candidate:
        candidate = urlparse(candidate).netloc or urlparse(candidate).path
    candidate = candidate.split("/", 1)[0].split("?", 1)[0].split("#", 1)[0]
    candidate = candidate.split("@")[-1].split(":", 1)[0].strip().lower().strip(".")
    if candidate.startswith("www."):
        candidate = candidate[4:]
    labels: List[str] = []
    for label in candidate.split("."):
        cleaned = LABEL_RE.sub("", label)
        if cleaned:
            labels.append(cleaned)
    if not labels:
        return None
    return ".".join(labels)


def registrable_domain(value: object) -> Optional[str]:
    normalized = normalize_domain(value)
    if not normalized:
        return None
    labels = normalized.split(".")
    if len(labels) <= 2:
        return normalized
    suffix = ".".join(labels[-2:])
    if suffix in MULTI_PART_PUBLIC_SUFFIXES and len(labels) >= 3:
        return ".".join(labels[-3:])
    suffix3 = ".".join(labels[-3:])
    if suffix3 in MULTI_PART_PUBLIC_SUFFIXES and len(labels) >= 4:
        return ".".join(labels[-4:])
    return ".".join(labels[-2:])


def parse_rank_value(value: object) -> Optional[int]:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    if raw.startswith("="):
        raw = raw[1:].strip()
    if raw.isdigit():
        return int(raw)
    return None


def expand_institution_tokens(value: object) -> Optional[str]:
    normalized = normalize_text(value)
    if not normalized:
        return None
    tokens = []
    for token in normalized.split():
        tokens.append(TOKEN_EXPANSIONS.get(token, token))
    return " ".join(tokens) or None


def institution_fingerprint(value: object) -> Optional[str]:
    expanded = expand_institution_tokens(value)
    if not expanded:
        return None
    tokens = [token for token in expanded.split() if token not in STOPWORDS]
    return " ".join(tokens) or None


def unique_preserve(values: Iterable[Optional[str]]) -> List[str]:
    seen: Set[str] = set()
    ordered: List[str] = []
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def extract_parenthetical_aliases(value: object) -> List[str]:
    raw = str(value or "")
    aliases: List[str] = []
    for match in PARENS_RE.finditer(raw):
        inner = match.group(1).strip()
        if not inner:
            continue
        aliases.append(inner)
    return aliases


def institution_aliases(value: object) -> List[str]:
    raw = str(value or "").strip()
    if not raw:
        return []
    no_parens = PARENS_RE.sub(" ", raw)
    aliases = [
        expand_institution_tokens(raw),
        institution_fingerprint(raw),
        expand_institution_tokens(no_parens),
        institution_fingerprint(no_parens),
    ]
    for alias in extract_parenthetical_aliases(raw):
        aliases.append(expand_institution_tokens(alias))
        aliases.append(institution_fingerprint(alias))
    aliases.extend(expand_institution_tokens(part) for part in raw.splitlines() if part.strip())
    aliases.extend(institution_fingerprint(part) for part in raw.splitlines() if part.strip())
    return unique_preserve(aliases)


def institution_candidate_aliases(value: object) -> List[str]:
    raw = str(value or "").strip()
    if not raw:
        return []
    segments = [raw]
    segments.extend(part.strip() for part in SPLIT_RE.split(raw) if part.strip())
    aliases: List[Optional[str]] = []
    for segment in segments:
        aliases.extend(institution_aliases(segment))
    return unique_preserve(aliases)


@dataclass(frozen=True)
class RankingEntry:
    rank: Optional[int]
    display_name: str
    country: Optional[str]
    country_norm: Optional[str]
    domain_norm: Optional[str]
    name_aliases: Sequence[str]


class RankingDataset:
    def __init__(self, label: str, entries: Sequence[RankingEntry]) -> None:
        self.label = label
        self.entries = list(entries)
        self.alias_index: Dict[str, List[RankingEntry]] = {}
        self.domain_index: Dict[str, List[RankingEntry]] = {}
        for entry in self.entries:
            for alias in entry.name_aliases:
                self.alias_index.setdefault(alias, []).append(entry)
            if entry.domain_norm:
                self.domain_index.setdefault(entry.domain_norm, []).append(entry)

    @classmethod
    def from_path(cls, label: str, path: Optional[Path]) -> "RankingDataset":
        if path is None:
            return cls(label, [])
        if not path.exists():
            raise FileNotFoundError(f"{label} ranking file not found: {path}")
        raw_data = json.loads(path.read_text(encoding="utf-8"))
        entries: List[RankingEntry] = []
        for item in raw_data:
            if not isinstance(item, dict):
                continue
            display_name = str(item.get("university") or item.get("company") or "").strip()
            if not display_name:
                continue
            domain = registrable_domain(item.get("domain")) or registrable_domain(item.get("website"))
            entries.append(
                RankingEntry(
                    rank=parse_rank_value(item.get("rank")),
                    display_name=display_name,
                    country=str(item.get("country")).strip() if item.get("country") not in (None, "") else None,
                    country_norm=normalize_country(item.get("country")),
                    domain_norm=domain,
                    name_aliases=institution_aliases(display_name),
                )
            )
        return cls(label, entries)

    def match_rank(self, institution: object, country: object = None) -> Optional[int]:
        country_norm = normalize_country(country)
        for alias in institution_candidate_aliases(institution):
            entries = self.alias_index.get(alias, [])
            if not entries:
                continue
            rank = self._resolve_entries(entries, country_norm)
            if rank is not None:
                return rank
        return None

    def match_rank_by_domain(self, domain: object, country: object = None) -> Optional[int]:
        domain_norm = registrable_domain(domain)
        if not domain_norm:
            return None
        entries = self.domain_index.get(domain_norm, [])
        if not entries:
            return None
        return self._resolve_entries(entries, normalize_country(country), prefer_country=False)

    def _resolve_entries(
        self,
        entries: Sequence[RankingEntry],
        country_norm: Optional[str],
        prefer_country: bool = True,
    ) -> Optional[int]:
        if country_norm:
            same_country = [entry for entry in entries if entry.country_norm == country_norm]
            if same_country:
                ranks = [entry.rank for entry in same_country if entry.rank is not None]
                return min(ranks) if ranks else None
            if prefer_country:
                return None

        distinct_countries = {entry.country_norm for entry in entries if entry.country_norm}
        if len(distinct_countries) > 1:
            return None
        ranks = [entry.rank for entry in entries if entry.rank is not None]
        return min(ranks) if ranks else None


class InstitutionRankings:
    def __init__(self, qs_top200: RankingDataset, world_top500: RankingDataset) -> None:
        self.qs_top200 = qs_top200
        self.world_top500 = world_top500

    @classmethod
    def from_paths(
        cls,
        qs_top200_path: Optional[Path],
        world_top500_path: Optional[Path],
    ) -> "InstitutionRankings":
        return cls(
            qs_top200=RankingDataset.from_path("qs_top200", qs_top200_path),
            world_top500=RankingDataset.from_path("world_top500", world_top500_path),
        )

    def best_qs_ranks(
        self,
        institutions: Iterable[object],
        countries: Optional[Iterable[object]] = None,
    ) -> Dict[str, Optional[int]]:
        best = {"qs_top200_rank": None}
        country_values = list(countries or [])
        country_candidates = country_values if country_values else [None]
        for institution in institutions:
            for country in country_candidates:
                rank = self.qs_top200.match_rank(institution, country)
                if rank is None:
                    continue
                current = best["qs_top200_rank"]
                if current is None or rank < current:
                    best["qs_top200_rank"] = rank
        return best

    def match_institution(self, institution: object, country: object = None) -> Dict[str, Optional[int]]:
        return {
            "qs_top200_rank": self.qs_top200.match_rank(institution, country),
            "world_top500_rank": self.world_top500.match_rank(institution, country),
        }

    def best_world_top500_rank(
        self,
        domains: Iterable[object],
        countries: Optional[Iterable[object]] = None,
    ) -> Dict[str, Optional[int]]:
        best = {"world_top500_rank": None}
        country_values = list(countries or [])
        country_candidates = country_values if country_values else [None]
        for domain in domains:
            for country in country_candidates:
                rank = self.world_top500.match_rank_by_domain(domain, country)
                if rank is None:
                    continue
                current = best["world_top500_rank"]
                if current is None or rank < current:
                    best["world_top500_rank"] = rank
        return best


def merge_best_rank(*values: Optional[int]) -> Optional[int]:
    available = [value for value in values if value is not None]
    return min(available) if available else None
