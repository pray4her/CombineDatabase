import argparse
import difflib
import hashlib
import json
import os
import re
import shutil
import unicodedata
from concurrent.futures import ProcessPoolExecutor, as_completed
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter

from institution_rankings import InstitutionRankings, merge_best_rank
from pipeline_storage import (
    file_size_or_none,
    iter_parquet_rows,
    write_bulk as storage_write_bulk,
    write_jsonl as storage_write_jsonl,
    write_pylist_parquet,
)

try:
    import ijson
    from ijson.common import ObjectBuilder
except Exception:  # pragma: no cover
    ijson = None
    ObjectBuilder = None


ORCID_RE = re.compile(r"\b\d{4}-\d{4}-\d{4}-[\dX]{4}\b", re.IGNORECASE)
SPACE_RE = re.compile(r"\s+")
NON_ALNUM_RE = re.compile(r"[^a-z0-9 ]+")
EMAIL_RE = re.compile(r"^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$", re.IGNORECASE)

_COMMON_CHINESE_SURNAMES = {
    "LI", "WANG", "ZHANG", "LIU", "CHEN", "YANG", "ZHAO", "HUANG", "ZHOU", "WU",
    "XU", "SUN", "MA", "ZHU", "HU", "GUO", "HE", "GAO", "LIN", "LUO",
    "ZHENG", "LIANG", "XIE", "SONG", "TANG", "HAN", "FENG", "PENG", "CUI", "JIANG",
    "QIAN", "QIN", "YU", "LU", "SHI", "YAO", "CAO", "DENG", "YUAN", "XIAO",
    "XIONG", "TAN", "QIU", "REN", "YAN", "DONG", "CHENG", "LAI", "FAN", "JIN",
    "JIA", "NI", "SHEN", "LIAO", "LAN", "QIAO", "OU", "HONG", "CAI", "PAN",
    "TIAN", "DU", "DAI", "XIA", "ZHONG", "YI", "ZOU", "SU", "GU", "HOU",
    "WEI", "TAO", "FANG", "BAI", "HAO", "KONG", "SHAO", "MENG", "QUAN",
    "WAN", "LEI", "BO", "YIN", "CHI", "CHANG", "MIAO", "LUAN", "YOU", "GE",
    "GONG", "XING", "RONG", "WENG", "JI", "PING", "BAO", "MU",
    "CHAN", "WONG", "LEE", "CHEUNG", "LAU", "NG", "YEUNG", "YU", "TSANG",
    "CHUI", "HO", "KWOK", "SUNG", "POON", "CHUNG", "LEUNG", "LAM", "CHIANG", "FONG",
    "MOK", "HUI", "CHOI", "SIN", "TSUI", "YIP", "LUK", "SIT", "TAM", "YIM",
    "KAM", "KWAN", "TSE", "AU", "CHIU", "CHOW", "KO", "LO", "SIU", "YUEN",
    "YAU", "FUNG", "CHU", "SHUM", "YIU", "TIN", "TUNG", "NGAN", "LOK", "HA",
    "MO", "HUNG", "KUI", "SHEK", "LIM", "CHUA", "GOH", "ONG", "TEH", "TEO",
    "KOH", "YEW", "TEE", "SOO", "KHOO", "YONG", "FOO", "CHEAH", "TIAH", "GAN",
    "SIM", "NEO", "HENG", "QUEK", "AW", "SEOW", "LIAW", "HOO", "OON", "TOH",
    "DING", "XUE", "YE", "CONG", "YUE", "CEN", "XUN", "PU", "ZHA",
    "SHUI", "JIAO", "ZHUANG", "QU", "YAN", "MU", "BU", "SHA", "NA", "HE",
}

_LIKELY_KOREAN_SURNAMES = {
    "KIM", "PARK", "JEONG", "MOON", "SHIN", "KANG", "CHO", "YUN", "JANG", "LIM",
}

CHINESE_IDENTITY_DOMESTIC = "国内华人"
CHINESE_IDENTITY_OVERSEAS = "海外华人"
CHINESE_IDENTITY_FOREIGN = "外国人"

_NON_CHINESE_ASIAN_COUNTRY_SET = {
    "Vietnam",
    "Japan",
    "South Korea",
    "North Korea",
}

COUNTRY_NORMALIZATION = {
    "peoples r china": "China",
    "people s r china": "China",
    "china": "China",
    "hong kong": "Hong Kong",
    "macao": "Macau",
    "macau": "Macau",
    "usa": "United States",
    "u s a": "United States",
    "united states": "United States",
    "united states of america": "United States",
    "uk": "United Kingdom",
    "u k": "United Kingdom",
    "england": "United Kingdom",
    "scotland": "United Kingdom",
    "wales": "United Kingdom",
    "republic of korea": "South Korea",
    "korea south": "South Korea",
    "south korea": "South Korea",
    "korea north": "North Korea",
    "north korea": "North Korea",
    "korea": "South Korea",
    "japan": "Japan",
    "vietnam": "Vietnam",
}

ENTITY_EXPECTED_TYPES = {
    "person": {
        "person_id": "str",
        "canonical_person_id": "str",
        "legacy_person_id": "str",
        "name_original": "str",
        "name_norm": "str",
        "name_aliases": "list",
        "matched_by_orcid": "bool",
        "orcid": "str",
        "researcher_ids": "list",
        "country": "str",
        "countries_observed": "list",
        "current_affiliation": "str",
        "chinese_identity": "str",
        "qs_top200_rank": "int",
        "world_top500_rank": "int",
        "affiliations": "list",
        "addresses": "list",
        "primary_email": "str",
        "emails": "list",
        "subjects": "list",
        "keywords": "list",
        "interests": "list",
        "h_index": "int",
        "i10_index": "int",
        "total_citations": "int",
        "average_citations": "float",
        "articles": "int",
        "google_scholar_url": "str",
        "sci_profile_url": "str",
        "match_methods": "list",
        "cluster_size": "int",
        "source_systems": "list",
        "source_refs": "list",
        "created_at": "str",
        "updated_at": "str",
    },
    "publication": {
        "publication_id": "str",
        "doi": "str",
        "doi_link": "str",
        "ut_wos_id": "str",
        "pubmed_id": "str",
        "title": "str",
        "abstract": "str",
        "document_type": "str",
        "publication_type": "str",
        "publication_year": "int",
        "publication_date_text": "str",
        "volume": "str",
        "issue": "str",
        "article_number": "str",
        "number_of_pages": "int",
        "source_title": "str",
        "journal_abbr": "str",
        "journal_iso_abbr": "str",
        "issn": "str",
        "eissn": "str",
        "language": "str",
        "publisher": "str",
        "publisher_city": "str",
        "publisher_address": "str",
        "author_keywords": "list",
        "keywords_plus": "list",
        "wos_categories": "list",
        "research_areas": "list",
        "web_of_science_index": "list",
        "times_cited_wos_core": "int",
        "times_cited_all_db": "int",
        "usage_180d": "int",
        "usage_since_2013": "int",
        "open_access_designation": "str",
        "funding_orgs": "list",
        "funding_text": "str",
        "source_refs": "list",
        "created_at": "str",
        "updated_at": "str",
    },
    "person_publication": {
        "relation_id": "str",
        "person_id": "str",
        "publication_id": "str",
        "relation_type": "str",
        "author_order": "int",
        "author_name_in_paper": "str",
        "match_score": "float",
        "match_confidence": "str",
        "match_rules_hit": "list",
        "is_corresponding_author": "bool",
        "source_refs": "list",
        "created_at": "str",
        "updated_at": "str",
    },
    "author_occurrence": {
        "occurrence_id": "str",
        "publication_id": "str",
        "author_order": "int",
        "author_name_raw": "str",
        "author_name_norm": "str",
        "surname_norm": "str",
        "given_names_norm": "str",
        "initials": "str",
        "name_aliases": "list",
        "orcid": "str",
        "researcher_ids": "list",
        "is_corresponding_author": "bool",
        "chinese_identity": "str",
        "source_refs": "list",
        "created_at": "str",
        "updated_at": "str",
    },
    "author_identifier_claim": {
        "claim_id": "str",
        "occurrence_id": "str",
        "publication_id": "str",
        "claim_type": "str",
        "claim_value": "str",
        "matched_name_raw": "str",
        "match_rule": "str",
        "confidence": "str",
        "score": "float",
        "source_refs": "list",
        "created_at": "str",
        "updated_at": "str",
    },
    "author_affiliation_claim": {
        "claim_id": "str",
        "occurrence_id": "str",
        "publication_id": "str",
        "source_field": "str",
        "institution": "str",
        "address_text": "str",
        "country_raw": "str",
        "country_norm": "str",
        "qs_top200_rank": "int",
        "world_top500_rank": "int",
        "is_reprint": "bool",
        "confidence": "str",
        "score": "float",
        "source_refs": "list",
        "created_at": "str",
        "updated_at": "str",
    },
    "author_email_claim": {
        "claim_id": "str",
        "occurrence_id": "str",
        "publication_id": "str",
        "email": "str",
        "email_domain": "str",
        "world_top500_rank": "int",
        "local_part": "str",
        "binding_rule": "str",
        "confidence": "str",
        "score": "float",
        "source_refs": "list",
        "created_at": "str",
        "updated_at": "str",
    },
}


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def sha1_text(text):
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def normalize_text(value):
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    s = unicodedata.normalize("NFKD", s)
    cleaned = []
    for ch in s:
        if unicodedata.combining(ch):
            continue
        if ch.isalnum():
            cleaned.append(ch.lower())
        elif ch.isspace():
            cleaned.append(" ")
        else:
            cleaned.append(" ")
    normalized = SPACE_RE.sub(" ", "".join(cleaned)).strip()
    return normalized or None


def split_semicolon(value):
    if value is None:
        return []
    s = str(value).strip()
    if not s:
        return []
    return sorted({p.strip() for p in s.split(";") if p and p.strip()})


def unique_preserve(values):
    seen = set()
    output = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        output.append(value)
    return output


def compact_raw_keys(raw_keys):
    keys = tuple(str(key) for key in raw_keys if key)
    if not keys:
        return []
    key_set = frozenset(keys)
    alias_map = {
        frozenset({"Author Full Names", "ORCIDs", "Researcher Ids", "Addresses", "Reprint Addresses"}): "frontiers_author_core",
        frozenset({"Email Addresses", "Reprint Addresses", "Author Full Names"}): "frontiers_email_bind",
        frozenset({"ORCIDs", "Author Full Names"}): "frontiers_orcid",
        frozenset({"Researcher Ids", "Author Full Names"}): "frontiers_researcher_id",
        frozenset({"Addresses", "Author Full Names"}): "frontiers_address",
        frozenset({"Reprint Addresses", "Author Full Names"}): "frontiers_reprint",
        frozenset({"Author Full Names", "Addresses", "Reprint Addresses"}): "frontiers_author_relation",
    }
    alias = alias_map.get(key_set)
    if alias:
        return [alias]
    if {"Article Title", "Source Title"} <= key_set:
        return ["frontiers_row"]
    if {"name", "countries", "current_affiliation"} <= key_set:
        return ["scholars_row"]
    if {"scholar_name", "doi"} <= key_set or {"scholar_name", "publication_id"} <= key_set:
        return ["matched_row"]
    if len(keys) <= 2:
        return list(keys)
    compact_tokens = [normalize_text(key) or str(key).lower() for key in keys[:3]]
    remainder = len(keys) - len(compact_tokens)
    suffix = f"+{remainder}" if remainder > 0 else ""
    return ["keys:" + "|".join(compact_tokens) + suffix]


def make_source_ref(source_system, source_file, sheet, row_index, raw_keys):
    return {
        "source_system": source_system,
        "source_file": source_file,
        "sheet": sheet,
        "row_index": row_index,
        "raw_keys": compact_raw_keys(raw_keys),
    }


def split_semicolon_ordered(value):
    if value is None:
        return []
    s = str(value).strip()
    if not s:
        return []
    return [part.strip() for part in s.split(";") if part and part.strip()]


def split_top_level_segments(value):
    if value is None:
        return []
    text = str(value).strip()
    if not text:
        return []
    parts = []
    current = []
    square_depth = 0
    paren_depth = 0
    for ch in text:
        if ch == "[":
            square_depth += 1
        elif ch == "]" and square_depth > 0:
            square_depth -= 1
        elif ch == "(":
            paren_depth += 1
        elif ch == ")" and paren_depth > 0:
            paren_depth -= 1
        if ch == ";" and square_depth == 0 and paren_depth == 0:
            segment = "".join(current).strip()
            if segment:
                parts.append(segment)
            current = []
            continue
        current.append(ch)
    tail = "".join(current).strip()
    if tail:
        parts.append(tail)
    return parts


def normalize_compact(value):
    if value is None:
        return None
    compact = re.sub(r"[^a-z0-9]+", "", normalize_text(value) or "")
    return compact or None


def tokenize_name_tokens(value):
    if value is None:
        return []
    normalized = normalize_text(value)
    if not normalized:
        return []
    return [token for token in normalized.split(" ") if token]


def parse_name_parts(name):
    raw = str(name or "").strip()
    if not raw:
        return {
            "raw": raw,
            "name_norm": None,
            "surname_raw": None,
            "surname_norm": None,
            "given_names_raw": None,
            "given_names_norm": None,
            "initials": None,
            "first_initial": None,
            "compact_full": None,
        }

    if "," in raw:
        surname_raw, given_names_raw = raw.split(",", 1)
        surname_raw = surname_raw.strip()
        given_names_raw = given_names_raw.strip()
    else:
        tokens = raw.split()
        surname_raw = tokens[-1] if tokens else raw
        given_names_raw = " ".join(tokens[:-1]) if len(tokens) > 1 else ""

    surname_norm = normalize_text(surname_raw)
    given_names_norm = normalize_text(given_names_raw)
    given_tokens = tokenize_name_tokens(given_names_raw)
    initials = "".join(token[0] for token in given_tokens if token)
    first_initial = initials[0] if initials else None
    compact_full = normalize_compact(raw)
    return {
        "raw": raw,
        "name_norm": normalize_text(raw),
        "surname_raw": surname_raw or None,
        "surname_norm": surname_norm,
        "given_names_raw": given_names_raw or None,
        "given_names_norm": given_names_norm,
        "initials": initials or None,
        "first_initial": first_initial,
        "compact_full": compact_full,
    }


def is_likely_chinese_surname(surname_norm):
    if not surname_norm:
        return False
    compact = surname_norm.replace(" ", "").upper()
    if compact in _LIKELY_KOREAN_SURNAMES and compact not in {"LIM"}:
        return False
    return compact in _COMMON_CHINESE_SURNAMES


def is_likely_chinese_name(name_parts):
    if not isinstance(name_parts, dict):
        return False
    return is_likely_chinese_surname(name_parts.get("surname_norm"))


def _is_non_chinese_asian_country(country_value):
    normalized = normalize_country(country_value)
    return normalized in _NON_CHINESE_ASIAN_COUNTRY_SET


def classify_chinese_identity(name_parts, countries):
    normalized_countries = []
    for country in countries or []:
        normalized = normalize_country(country)
        if normalized:
            normalized_countries.append(normalized)
    normalized_countries = unique_preserve(normalized_countries)
    if "China" in normalized_countries:
        return CHINESE_IDENTITY_DOMESTIC
    if not is_likely_chinese_name(name_parts):
        return CHINESE_IDENTITY_FOREIGN
    if not normalized_countries:
        return CHINESE_IDENTITY_FOREIGN
    if any(not _is_non_chinese_asian_country(country) for country in normalized_countries):
        return CHINESE_IDENTITY_OVERSEAS
    return CHINESE_IDENTITY_FOREIGN


def chinese_identity_counts(rows, key="chinese_identity"):
    return {
        "domestic_chinese_count": sum(1 for row in rows if row.get(key) == CHINESE_IDENTITY_DOMESTIC),
        "overseas_chinese_count": sum(1 for row in rows if row.get(key) == CHINESE_IDENTITY_OVERSEAS),
        "foreign_count": sum(1 for row in rows if row.get(key) == CHINESE_IDENTITY_FOREIGN),
    }


def name_aliases(name):
    parts = parse_name_parts(name)
    raw = parts["raw"]
    if not raw:
        return []
    aliases = {raw}
    surname = parts["surname_raw"]
    given_raw = parts["given_names_raw"]
    initials = parts["initials"]
    raw_given_tokens = re.findall(r"[A-Za-z]+", given_raw or "")
    first_given = raw_given_tokens[0] if raw_given_tokens else None

    if surname and given_raw:
        aliases.add(f"{surname}, {given_raw}")
        aliases.add(f"{given_raw} {surname}")
    if surname and first_given:
        aliases.add(f"{surname}, {first_given}")
        aliases.add(f"{first_given} {surname}")
    if surname and initials:
        aliases.add(f"{surname}, {initials.upper()}")
        aliases.add(f"{initials.upper()} {surname}")
    return sorted({alias.strip() for alias in aliases if alias and alias.strip()})


def author_occurrence_id(publication_id_value, author_order, author_name_norm):
    return sha1_text(f"{publication_id_value}|{author_order}|{author_name_norm or ''}")


def generic_claim_id(*parts):
    return sha1_text("|".join("" if part is None else str(part) for part in parts))


def merge_unique_values(existing_values, new_values):
    merged = []
    seen = set()
    for value in (existing_values or []) + (new_values or []):
        if isinstance(value, (dict, list)):
            marker = json.dumps(value, ensure_ascii=False, sort_keys=True)
        else:
            marker = str(value)
        if marker in seen:
            continue
        seen.add(marker)
        merged.append(value)
    return merged


def _max_timestamp(left, right):
    if left is None:
        return right
    if right is None:
        return left
    return max(left, right)


def upsert_author_occurrence(records, record, quality):
    occurrence_id_value = record["occurrence_id"]
    if occurrence_id_value not in records:
        records[occurrence_id_value] = record
        return

    existing = records[occurrence_id_value]
    conflict_keys = [
        "publication_id",
        "author_order",
        "author_name_norm",
        "surname_norm",
        "given_names_norm",
    ]
    if any(
        existing.get(key) is not None and record.get(key) is not None and existing.get(key) != record.get(key)
        for key in conflict_keys
    ):
        quality["author_occurrence"]["pk_conflicts"] += 1

    for key in ["author_name_raw", "author_name_norm", "surname_norm", "given_names_norm", "initials", "orcid"]:
        if existing.get(key) is None and record.get(key) is not None:
            existing[key] = record[key]

    existing["name_aliases"] = merge_unique_values(existing.get("name_aliases"), record.get("name_aliases"))
    existing["researcher_ids"] = sorted(set((existing.get("researcher_ids") or []) + (record.get("researcher_ids") or [])))
    existing["is_corresponding_author"] = bool(existing.get("is_corresponding_author")) or bool(
        record.get("is_corresponding_author")
    )
    existing["source_refs"] = merge_unique_values(existing.get("source_refs"), record.get("source_refs"))
    existing["updated_at"] = _max_timestamp(existing.get("updated_at"), record.get("updated_at"))


def upsert_claim_record(records, record, entity, quality, conflict_keys):
    claim_id_value = record["claim_id"]
    if claim_id_value not in records:
        records[claim_id_value] = record
        return

    existing = records[claim_id_value]
    if any(
        existing.get(key) is not None and record.get(key) is not None and existing.get(key) != record.get(key)
        for key in conflict_keys
    ):
        quality[entity]["pk_conflicts"] += 1

    for key, value in record.items():
        if key in {"claim_id", "source_refs", "created_at", "updated_at"}:
            continue
        if key == "score":
            existing_score = existing.get("score")
            if existing_score is None or (value is not None and value > existing_score):
                existing["score"] = value
            continue
        if key == "confidence":
            rank = {"high": 3, "medium": 2, "low": 1}
            existing_rank = rank.get(existing.get("confidence"), 0)
            incoming_rank = rank.get(value, 0)
            if incoming_rank > existing_rank:
                existing["confidence"] = value
            continue
        if existing.get(key) is None and value is not None:
            existing[key] = value
        elif isinstance(existing.get(key), list) or isinstance(value, list):
            existing[key] = merge_unique_values(existing.get(key), value)

    existing["source_refs"] = merge_unique_values(existing.get("source_refs"), record.get("source_refs"))
    existing["updated_at"] = _max_timestamp(existing.get("updated_at"), record.get("updated_at"))


def split_name_id_tokens(value):
    tokens = []
    if value is None:
        return tokens
    for token in str(value).split(";"):
        token = token.strip()
        if not token or "/" not in token:
            continue
        name_part, id_part = token.rsplit("/", 1)
        name_part = name_part.strip()
        id_part = id_part.strip()
        if not name_part or not id_part:
            continue
        tokens.append((name_part, id_part))
    return tokens


def build_author_index(authors):
    exact = defaultdict(list)
    surname_initials = defaultdict(list)
    surname_first_initial = defaultdict(list)
    for idx, author in enumerate(authors):
        alias_keys = {author.get("author_name_norm"), author.get("compact_full")}
        alias_keys.update(normalize_text(alias) for alias in author.get("name_aliases", []))
        alias_keys.update(normalize_compact(alias) for alias in author.get("name_aliases", []))
        for key in alias_keys:
            if key:
                exact[key].append(idx)
        surname = author.get("surname_norm")
        initials = author.get("initials")
        first_initial = author.get("first_initial")
        if surname and initials:
            surname_initials[f"{surname}|{initials}"].append(idx)
        if surname and first_initial:
            surname_first_initial[f"{surname}|{first_initial}"].append(idx)
    return {
        "exact": exact,
        "surname_initials": surname_initials,
        "surname_first_initial": surname_first_initial,
    }


def match_name_to_author(name, authors, author_index):
    parts = parse_name_parts(name)
    candidate_keys = [
        parts["name_norm"],
        parts["compact_full"],
    ]
    candidate_keys.extend(normalize_text(alias) for alias in name_aliases(name))
    candidate_keys.extend(normalize_compact(alias) for alias in name_aliases(name))
    for key in candidate_keys:
        if key and len(author_index["exact"].get(key, [])) == 1:
            idx = author_index["exact"][key][0]
            return idx, "exact_name", 1.0

    surname = parts["surname_norm"]
    initials = parts["initials"]
    first_initial = parts["first_initial"]
    if surname and initials:
        compound = f"{surname}|{initials}"
        if len(author_index["surname_initials"].get(compound, [])) == 1:
            idx = author_index["surname_initials"][compound][0]
            return idx, "surname_initials", 0.94
    if surname and first_initial:
        compound = f"{surname}|{first_initial}"
        if len(author_index["surname_first_initial"].get(compound, [])) == 1:
            idx = author_index["surname_first_initial"][compound][0]
            return idx, "surname_first_initial", 0.82
    return None, None, None


def normalize_country(country_raw):
    if country_raw is None:
        return None
    cleaned = re.sub(r"\b\d{4,}\b", " ", str(country_raw))
    cleaned = re.sub(r"[.;]+$", "", cleaned).strip(" ,")
    normalized = normalize_text(cleaned)
    if not normalized:
        return None
    if normalized in COUNTRY_NORMALIZATION:
        return COUNTRY_NORMALIZATION[normalized]
    return cleaned.strip() or None


def extract_country_from_address_text(address_text):
    if address_text is None:
        return None
    tokens = [token.strip(" .") for token in str(address_text).replace(";", ",").split(",")]
    for token in reversed(tokens):
        if not token:
            continue
        if re.fullmatch(r"[A-Z]?\d[\dA-Z -]*", token, re.IGNORECASE):
            continue
        normalized = normalize_country(token)
        if normalized:
            return normalized
    return None


def extract_institution_from_address_text(address_text):
    if address_text is None:
        return None
    token = str(address_text).split(",", 1)[0].strip(" .")
    return token or None


def parse_addresses_segments(addresses_value, authors, author_index):
    claims = []
    for segment in split_top_level_segments(addresses_value):
        match = re.match(r"^\s*\[([^\]]+)\]\s*(.*)$", segment)
        if not match:
            continue
        author_group = match.group(1)
        address_text = match.group(2).strip(" ,.")
        institution = extract_institution_from_address_text(address_text)
        country = extract_country_from_address_text(address_text)
        for raw_name in split_semicolon_ordered(author_group):
            idx, rule, score = match_name_to_author(raw_name, authors, author_index)
            if idx is None:
                continue
            claims.append(
                {
                    "author_idx": idx,
                    "matched_name_raw": raw_name,
                    "match_rule": rule,
                    "institution": institution,
                    "address_text": address_text,
                    "country_norm": country,
                    "source_field": "Addresses",
                    "is_reprint": False,
                    "score": score,
                    "confidence": "high" if (score or 0) >= 0.9 else "medium",
                }
            )
    return claims


def parse_reprint_segments(reprint_value, authors, author_index):
    claims = []
    corresponding_indices = set()
    for segment in split_top_level_segments(reprint_value):
        segment_clean = str(segment).strip()
        lowered = segment_clean.lower()
        if "(" in segment_clean and ")" in segment_clean and segment_clean.index("(") < segment_clean.index(")"):
            author_token = segment_clean[: segment_clean.index("(")].strip(" ,")
            after_text = segment_clean[segment_clean.index(")") + 1 :].strip(" ,")
        else:
            match = re.match(r"^\s*([^,]+,\s*[^,]+)\s*,\s*(.*)$", segment_clean)
            if match:
                author_token = match.group(1).strip(" ,")
                after_text = match.group(2).strip(" ,")
            else:
                author_token = None
                after_text = segment_clean
        if not author_token:
            continue
        idx, rule, score = match_name_to_author(author_token, authors, author_index)
        if idx is None:
            continue
        is_corresponding = "corresponding author" in lowered
        if is_corresponding:
            corresponding_indices.add(idx)
        claims.append(
            {
                "author_idx": idx,
                "matched_name_raw": author_token,
                "match_rule": rule,
                "institution": extract_institution_from_address_text(after_text),
                "address_text": after_text,
                "country_norm": extract_country_from_address_text(after_text),
                "source_field": "Reprint Addresses",
                "is_reprint": True,
                "score": score or 0.9,
                "confidence": "high" if (score or 0.9) >= 0.9 else "medium",
                "is_corresponding_author": is_corresponding,
            }
        )
    return claims, corresponding_indices


def parse_valid_emails(email_value):
    emails = []
    for token in split_semicolon_ordered(email_value):
        email = token.strip().strip(".")
        if not EMAIL_RE.fullmatch(email):
            continue
        local_part = email.split("@", 1)[0]
        if local_part.isdigit():
            continue
        emails.append(email)
    return unique_preserve(emails)


def email_local_part(email):
    return email.split("@", 1)[0].lower()


def email_aliases_for_author(author):
    aliases = set()
    surname = (author.get("surname_norm") or "").replace(" ", "")
    given = (author.get("given_names_norm") or "").replace(" ", "")
    initials = (author.get("initials") or "").lower()
    first_initial = (author.get("first_initial") or "").lower() if author.get("first_initial") else None
    compact_full = author.get("compact_full")
    if compact_full:
        aliases.add(compact_full)
    if surname and given:
        aliases.add(f"{surname}{given}")
        aliases.add(f"{given}{surname}")
    if surname and initials:
        aliases.add(f"{surname}{initials}")
        aliases.add(f"{initials}{surname}")
    if surname and first_initial:
        aliases.add(f"{surname}{first_initial}")
        aliases.add(f"{first_initial}{surname}")
    if is_likely_chinese_surname(author.get("surname_norm")) and surname and given:
        aliases.add(f"{surname}{given}")
        aliases.add(f"{given}{surname}")
        if initials:
            aliases.add(initials)
        if first_initial:
            aliases.add(f"{surname[:1]}{first_initial}")
            aliases.add(f"{first_initial}{surname[:1]}")
    return {alias for alias in aliases if alias}


def score_author_email(author, email):
    local = email_local_part(email)
    local_core = re.sub(r"\d+$", "", re.sub(r"[^a-z0-9]+", "", local))
    if not local_core:
        return 0.0, []
    best_score = 0.0
    best_rules = []
    aliases = email_aliases_for_author(author)
    for alias in aliases:
        if local == alias or local_core == alias:
            score = 1.0 if local == alias else 0.97
            rules = ["exact_alias"]
        else:
            similarity = difflib.SequenceMatcher(None, alias, local_core).ratio()
            score = similarity
            rules = ["sequence_match"]
            if local_core.startswith(alias) or alias.startswith(local_core):
                score = max(score, 0.9)
                rules.append("prefix_overlap")
        if is_likely_chinese_surname(author.get("surname_norm")) and alias in {local_core, local}:
            score = max(score, 0.99)
            rules.append("chinese_alias")
        if score > best_score:
            best_score = score
            best_rules = rules
    return best_score, sorted(set(best_rules))


def bind_emails_to_authors(authors, emails, corresponding_indices):
    claims = []
    if not emails:
        return claims

    if len(emails) == 1 and len(corresponding_indices) == 1:
        idx = next(iter(corresponding_indices))
        claims.append(
            {
                "author_idx": idx,
                "email": emails[0],
                "binding_rule": "unique_corresponding_author",
                "confidence": "high",
                "score": 0.99,
            }
        )
        return claims

    candidate_indices = sorted(corresponding_indices) if corresponding_indices else list(range(len(authors)))
    for email in emails:
        scored = []
        for idx in candidate_indices:
            score, rules = score_author_email(authors[idx], email)
            if score <= 0:
                continue
            scored.append((score, idx, rules))
        scored.sort(reverse=True)
        if not scored:
            continue
        top_score, top_idx, top_rules = scored[0]
        second_score = scored[1][0] if len(scored) > 1 else 0.0
        if top_score < 0.9:
            continue
        if (top_score - second_score) < 0.08 and top_score < 0.97:
            continue
        claims.append(
            {
                "author_idx": top_idx,
                "email": email,
                "binding_rule": "+".join(top_rules) if top_rules else "score_match",
                "confidence": "high" if top_score >= 0.97 else "medium",
                "score": round(top_score, 4),
            }
        )
    return claims


def parse_orcid(value):
    if value is None:
        return None
    m = ORCID_RE.search(str(value))
    return m.group(0).upper() if m else None


def parse_name_id_pairs(value):
    mapping = {}
    if value is None:
        return mapping
    for token in str(value).split(";"):
        token = token.strip()
        if not token or "/" not in token:
            continue
        name_part, id_part = token.rsplit("/", 1)
        name_norm = normalize_text(name_part)
        id_clean = id_part.strip()
        if not name_norm or not id_clean:
            continue
        mapping.setdefault(name_norm, []).append(id_clean)
    return mapping


def person_id(name_norm, orcid=None, country=None):
    return sha1_text(f"{name_norm or ''}|{orcid or ''}|{country or ''}")


def publication_id(doi, ut_wos_id, title, year, source_title):
    if doi:
        return str(doi)
    if ut_wos_id:
        return str(ut_wos_id)
    return sha1_text(f"{title or ''}|{year or ''}|{source_title or ''}")


def relation_id(person_id_value, publication_id_value, relation_type):
    return sha1_text(f"{person_id_value}|{publication_id_value}|{relation_type}")


def coerce_int(v):
    if v is None or v == "":
        return None
    try:
        return int(v)
    except Exception:
        return None


def coerce_float(v):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except Exception:
        return None


def type_ok(value, expected):
    if value is None:
        return True
    if expected == "str":
        return isinstance(value, str)
    if expected == "int":
        return isinstance(value, int) and not isinstance(value, bool)
    if expected == "float":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if expected == "list":
        return isinstance(value, list)
    if expected == "bool":
        return isinstance(value, bool)
    return True


def init_quality():
    data = {}
    for entity, fields in ENTITY_EXPECTED_TYPES.items():
        data[entity] = {
            "records": 0,
            "null_counts": {f: 0 for f in fields},
            "type_anomalies": {f: 0 for f in fields},
            "pk_conflicts": 0,
        }
    return data


def quality_track(quality, entity, row):
    q = quality[entity]
    q["records"] += 1
    for field, expected in ENTITY_EXPECTED_TYPES[entity].items():
        v = row.get(field)
        if v is None:
            q["null_counts"][field] += 1
            continue
        if not type_ok(v, expected):
            q["type_anomalies"][field] += 1


def finalize_quality(quality):
    out = {}
    for entity, q in quality.items():
        recs = q["records"] if q["records"] else 1
        out[entity] = {
            "records": q["records"],
            "pk_conflicts": q["pk_conflicts"],
            "null_rate": {k: v / recs for k, v in q["null_counts"].items()},
            "type_anomalies": {k: v for k, v in q["type_anomalies"].items() if v > 0},
        }
    return out


def upsert_person(persons, record, quality):
    pid = record["person_id"]
    if pid not in persons:
        persons[pid] = record
        return

    existing = persons[pid]
    conflict_keys = ["name_norm", "orcid", "country"]
    if any(
        existing.get(k) is not None and record.get(k) is not None and existing.get(k) != record.get(k)
        for k in conflict_keys
    ):
        quality["person"]["pk_conflicts"] += 1

    for key in [
        "canonical_person_id",
        "legacy_person_id",
        "name_original",
        "name_norm",
        "orcid",
        "country",
        "current_affiliation",
        "primary_email",
        "matched_by_orcid",
        "h_index",
        "i10_index",
        "total_citations",
        "average_citations",
        "articles",
        "google_scholar_url",
        "sci_profile_url",
    ]:
        if existing.get(key) is None and record.get(key) is not None:
            existing[key] = record[key]

    for rank_key in ["qs_top200_rank", "world_top500_rank"]:
        existing[rank_key] = merge_best_rank(existing.get(rank_key), record.get(rank_key))

    for arr_key in [
        "name_aliases",
        "researcher_ids",
        "countries_observed",
        "affiliations",
        "addresses",
        "emails",
        "subjects",
        "keywords",
        "interests",
        "match_methods",
        "source_systems",
    ]:
        merged = set(existing.get(arr_key, []) or [])
        merged.update(record.get(arr_key, []) or [])
        existing[arr_key] = sorted(merged)

    existing["source_refs"] = (existing.get("source_refs", []) or []) + (record.get("source_refs", []) or [])
    existing["updated_at"] = record.get("updated_at", existing.get("updated_at"))


def merge_person_records(target, source):
    for key in [
        "canonical_person_id",
        "legacy_person_id",
        "name_original",
        "name_norm",
        "orcid",
        "country",
        "current_affiliation",
        "primary_email",
        "h_index",
        "i10_index",
        "total_citations",
        "average_citations",
        "articles",
        "google_scholar_url",
        "sci_profile_url",
    ]:
        if target.get(key) is None and source.get(key) is not None:
            target[key] = source[key]

    for rank_key in ["qs_top200_rank", "world_top500_rank"]:
        target[rank_key] = merge_best_rank(target.get(rank_key), source.get(rank_key))

    for arr_key in [
        "name_aliases",
        "researcher_ids",
        "countries_observed",
        "affiliations",
        "addresses",
        "emails",
        "subjects",
        "keywords",
        "interests",
        "match_methods",
        "source_systems",
    ]:
        merged = set(target.get(arr_key, []) or [])
        merged.update(source.get(arr_key, []) or [])
        target[arr_key] = sorted(merged)

    target["matched_by_orcid"] = bool(target.get("matched_by_orcid")) or bool(source.get("matched_by_orcid"))
    target["cluster_size"] = max(
        [value for value in [target.get("cluster_size"), source.get("cluster_size")] if value is not None],
        default=target.get("cluster_size"),
    )
    target["source_refs"] = (target.get("source_refs", []) or []) + (source.get("source_refs", []) or [])
    target["created_at"] = min(
        [x for x in [target.get("created_at"), source.get("created_at")] if x is not None],
        default=target.get("created_at"),
    )
    target["updated_at"] = max(
        [x for x in [target.get("updated_at"), source.get("updated_at")] if x is not None],
        default=target.get("updated_at"),
    )


def choose_canonical_person_id(group_records):
    def score(item):
        pid, rec = item
        source_systems = set(rec.get("source_systems", []) or [])
        has_scholars = 1 if "scholars" in source_systems else 0
        field_density = sum(1 for value in rec.values() if value not in (None, [], ""))
        return (has_scholars, field_density, len(rec.get("source_refs", []) or []), str(pid))

    return max(group_records, key=score)[0]


def reconcile_persons_by_orcid(persons, relations):
    grouped = defaultdict(list)
    for pid, rec in persons.items():
        orcid = rec.get("orcid")
        if orcid:
            grouped[str(orcid)].append((pid, rec))

    canonical_by_pid = {}
    for _, group_records in grouped.items():
        if len(group_records) <= 1:
            continue
        canonical_pid = choose_canonical_person_id(group_records)
        canonical_by_pid[canonical_pid] = canonical_pid
        canonical_record = persons[canonical_pid]
        canonical_record["matched_by_orcid"] = True
        for pid, rec in group_records:
            canonical_by_pid[pid] = canonical_pid
            if pid == canonical_pid:
                continue
            merge_person_records(canonical_record, rec)
            canonical_record["matched_by_orcid"] = True

    if not canonical_by_pid:
        for rec in persons.values():
            rec["matched_by_orcid"] = bool(rec.get("matched_by_orcid"))
        return persons, relations

    merged_persons = {}
    for pid, rec in persons.items():
        if canonical_by_pid.get(pid, pid) != pid:
            continue
        rec["matched_by_orcid"] = bool(rec.get("matched_by_orcid"))
        merged_persons[pid] = rec

    merged_relations = {}
    for relation in relations.values():
        new_person_id = canonical_by_pid.get(relation["person_id"], relation["person_id"])
        new_relation = dict(relation)
        if new_person_id != relation["person_id"]:
            new_relation["person_id"] = new_person_id
            new_relation["relation_id"] = relation_id(
                new_person_id,
                relation["publication_id"],
                relation["relation_type"],
            )
        merged_relations[new_relation["relation_id"]] = new_relation

    return merged_persons, merged_relations


def upsert_publication(publications, record, quality):
    pub_id = record["publication_id"]
    if pub_id not in publications:
        publications[pub_id] = record
        return

    existing = publications[pub_id]
    conflict_keys = ["doi", "ut_wos_id", "title", "publication_year"]
    if any(
        existing.get(k) is not None and record.get(k) is not None and existing.get(k) != record.get(k)
        for k in conflict_keys
    ):
        quality["publication"]["pk_conflicts"] += 1

    existing["source_refs"] = (existing.get("source_refs", []) or []) + (record.get("source_refs", []) or [])
    existing["updated_at"] = record.get("updated_at", existing.get("updated_at"))


def upsert_relation(relations, record, quality):
    rid = record["relation_id"]
    if rid in relations:
        quality["person_publication"]["pk_conflicts"] += 1
        return
    relations[rid] = record


def upsert_seed_person(persons, record):
    pid = record["person_id"]
    if pid not in persons:
        persons[pid] = record
        return
    merge_person_records(persons[pid], record)


def upsert_seed_relation(relations, record):
    rid = record["relation_id"]
    if rid in relations:
        return
    relations[rid] = record


def write_jsonl(path, rows):
    storage_write_jsonl(path, rows)


def write_bulk(path, index_name, id_key, rows, id_safety_stats):
    storage_write_bulk(path, index_name, id_key, rows, id_safety_stats)


def remove_tree(path):
    if path.exists():
        shutil.rmtree(path)


def strip_internal_fields(row):
    return {key: value for key, value in row.items() if not str(key).startswith("_")}


def source_order_key(row):
    source_order = row.get("_source_order") or [0, 0]
    if not isinstance(source_order, list):
        source_order = [0, 0]
    if len(source_order) < 2:
        source_order = list(source_order) + [0] * (2 - len(source_order))
    return int(source_order[0]), int(source_order[1])


def stable_shard_index(source_file, sheet, row_index, shard_count):
    if shard_count <= 1:
        return 0
    digest = hashlib.sha1(
        f"{source_file}|{sheet}|{row_index}".encode("utf-8")
    ).hexdigest()
    return int(digest[:12], 16) % shard_count


def write_frontiers_shard_inputs(
    frontiers_json,
    frontiers_jsonl,
    frontiers_parquet,
    shard_count,
    tmp_dir,
):
    tmp_dir = Path(tmp_dir)
    remove_tree(tmp_dir)
    tmp_dir.mkdir(parents=True, exist_ok=True)
    shard_paths = [tmp_dir / f"frontiers_shard_{idx:04d}.jsonl" for idx in range(shard_count)]
    handles = [open(path, "w", encoding="utf-8") for path in shard_paths]
    frontiers_row_to_pubid = {}
    frontiers_doi_to_pubid = {}
    shard_row_counts = [0 for _ in range(shard_count)]
    global_frontiers_row = 0
    try:
        for source_file, sheet, row_idx, row in iter_input_rows(frontiers_json, frontiers_jsonl, frontiers_parquet):
            global_frontiers_row += 1
            doi = row.get("DOI")
            ut = row.get("UT (Unique WOS ID)")
            title = row.get("Article Title")
            pub_year = coerce_int(row.get("Publication Year"))
            source_title = row.get("Source Title")
            pub_id = publication_id(doi, ut, title, pub_year, source_title)
            frontiers_row_to_pubid[global_frontiers_row] = pub_id
            if doi:
                frontiers_doi_to_pubid[str(doi)] = pub_id
            shard_idx = stable_shard_index(source_file, sheet, row_idx, shard_count)
            shard_row_counts[shard_idx] += 1
            handles[shard_idx].write(
                json.dumps(
                    {
                        "global_frontiers_row": global_frontiers_row,
                        "source_file": source_file,
                        "sheet": sheet,
                        "row_index": row_idx,
                        "row": row,
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
    finally:
        for handle in handles:
            handle.close()
    return {
        "global_frontiers_row": global_frontiers_row,
        "frontiers_row_to_pubid": frontiers_row_to_pubid,
        "frontiers_doi_to_pubid": frontiers_doi_to_pubid,
        "shard_paths": shard_paths,
        "shard_row_counts": shard_row_counts,
    }


def process_frontiers_shard_worker(
    shard_input_text,
    output_dir_text,
    ts,
    write_compat_person,
    qs_top200_json,
    world_top500_json,
):
    shard_input = Path(shard_input_text)
    output_dir = Path(output_dir_text)
    output_dir.mkdir(parents=True, exist_ok=True)
    institution_rankings = InstitutionRankings.from_paths(
        Path(qs_top200_json) if qs_top200_json else None,
        Path(world_top500_json) if world_top500_json else None,
    )
    outputs = {
        "publication": output_dir / "publication.raw.jsonl",
        "author_occurrence": output_dir / "author_occurrence.raw.jsonl",
        "author_identifier_claim": output_dir / "author_identifier_claim.raw.jsonl",
        "author_affiliation_claim": output_dir / "author_affiliation_claim.raw.jsonl",
        "author_email_claim": output_dir / "author_email_claim.raw.jsonl",
        "person": output_dir / "person.raw.jsonl",
        "person_publication": output_dir / "person_publication.raw.jsonl",
    }
    counters = {
        "publication": 0,
        "author_occurrence": 0,
        "author_identifier_claim": 0,
        "author_affiliation_claim": 0,
        "author_email_claim": 0,
        "person": 0,
        "person_publication": 0,
    }
    source_anomalies = {
        "frontiers_orcid_invalid_count": 0,
        "frontiers_email_unassigned_count": 0,
    }
    handles = {
        name: open(path, "w", encoding="utf-8")
        for name, path in outputs.items()
    }

    def emit(entity, record):
        handles[entity].write(json.dumps(record, ensure_ascii=False) + "\n")
        counters[entity] += 1

    try:
        with open(shard_input, "r", encoding="utf-8") as handle:
            for line in handle:
                raw = line.strip()
                if not raw:
                    continue
                item = json.loads(raw)
                global_frontiers_row = int(item["global_frontiers_row"])
                source_file = str(item["source_file"])
                sheet = str(item["sheet"])
                row_idx = int(item["row_index"])
                row = item["row"]
                local_order = 0

                doi = row.get("DOI")
                ut = row.get("UT (Unique WOS ID)")
                title = row.get("Article Title")
                pub_year = coerce_int(row.get("Publication Year"))
                source_title = row.get("Source Title")
                pub_id = publication_id(doi, ut, title, pub_year, source_title)

                pub = {
                    "publication_id": pub_id,
                    "doi": doi,
                    "doi_link": str(row.get("DOI Link")) if row.get("DOI Link") is not None else None,
                    "ut_wos_id": ut,
                    "pubmed_id": str(row.get("Pubmed Id")) if row.get("Pubmed Id") is not None else None,
                    "title": title,
                    "abstract": row.get("Abstract"),
                    "document_type": row.get("Document Type"),
                    "publication_type": row.get("Publication Type"),
                    "publication_year": pub_year,
                    "publication_date_text": row.get("Publication Date"),
                    "volume": str(row.get("Volume")) if row.get("Volume") is not None else None,
                    "issue": str(row.get("Issue")) if row.get("Issue") is not None else None,
                    "article_number": str(row.get("Article Number")) if row.get("Article Number") is not None else None,
                    "number_of_pages": coerce_int(row.get("Number of Pages")),
                    "source_title": source_title,
                    "journal_abbr": row.get("Journal Abbreviation"),
                    "journal_iso_abbr": row.get("Journal ISO Abbreviation"),
                    "issn": row.get("ISSN"),
                    "eissn": row.get("eISSN"),
                    "language": row.get("Language"),
                    "publisher": row.get("Publisher"),
                    "publisher_city": row.get("Publisher City"),
                    "publisher_address": row.get("Publisher Address"),
                    "author_keywords": split_semicolon(row.get("Author Keywords")),
                    "keywords_plus": split_semicolon(row.get("Keywords Plus")),
                    "wos_categories": split_semicolon(row.get("WoS Categories")),
                    "research_areas": split_semicolon(row.get("Research Areas")),
                    "web_of_science_index": split_semicolon(row.get("Web of Science Index")),
                    "times_cited_wos_core": coerce_int(row.get("Times Cited, WoS Core")),
                    "times_cited_all_db": coerce_int(row.get("Times Cited, All Databases")),
                    "usage_180d": coerce_int(row.get("180 Day Usage Count")),
                    "usage_since_2013": coerce_int(row.get("Since 2013 Usage Count")),
                    "open_access_designation": row.get("Open Access Designations"),
                    "funding_orgs": split_semicolon(row.get("Funding Orgs")),
                    "funding_text": row.get("Funding Text"),
                    "source_refs": [make_source_ref("frontiers", source_file, sheet, row_idx, sorted(row.keys()))],
                    "created_at": ts,
                    "updated_at": ts,
                    "_source_order": [global_frontiers_row, local_order],
                }
                emit("publication", pub)
                local_order += 1

                authors = []
                for order, author_name in enumerate(split_semicolon_ordered(row.get("Author Full Names")), start=1):
                    parts = parse_name_parts(author_name)
                    if not parts["name_norm"]:
                        continue
                    occurrence_id = author_occurrence_id(pub_id, order, parts["name_norm"])
                    authors.append(
                        {
                            "occurrence_id": occurrence_id,
                            "publication_id": pub_id,
                            "author_order": order,
                            "author_name_raw": parts["raw"],
                            "author_name_norm": parts["name_norm"],
                            "surname_norm": parts["surname_norm"],
                            "given_names_norm": parts["given_names_norm"],
                            "initials": parts["initials"],
                            "first_initial": parts["first_initial"],
                            "compact_full": parts["compact_full"],
                            "name_aliases": name_aliases(author_name),
                            "orcid": None,
                            "researcher_ids": [],
                            "is_corresponding_author": False,
                            "_countries": [],
                            "_institutions": [],
                            "_addresses": [],
                            "_qs_top200_ranks": [],
                            "_world_top500_email_ranks": [],
                        }
                    )
                author_index = build_author_index(authors)

                for raw_name, raw_orcid in split_name_id_tokens(row.get("ORCIDs")):
                    parsed_orcid = parse_orcid(raw_orcid)
                    if raw_orcid and parsed_orcid is None:
                        source_anomalies["frontiers_orcid_invalid_count"] += 1
                        continue
                    idx, match_rule, score = match_name_to_author(raw_name, authors, author_index)
                    if idx is None or parsed_orcid is None:
                        continue
                    authors[idx]["orcid"] = parsed_orcid
                    emit(
                        "author_identifier_claim",
                        {
                            "claim_id": generic_claim_id(
                                pub_id,
                                authors[idx]["occurrence_id"],
                                "orcid",
                                parsed_orcid,
                                str(raw_name).strip(),
                                match_rule,
                            ),
                            "occurrence_id": authors[idx]["occurrence_id"],
                            "publication_id": pub_id,
                            "claim_type": "orcid",
                            "claim_value": parsed_orcid,
                            "matched_name_raw": raw_name,
                            "match_rule": match_rule,
                            "confidence": "high",
                            "score": score,
                            "source_refs": [make_source_ref("frontiers", source_file, sheet, row_idx, ["ORCIDs", "Author Full Names"])],
                            "created_at": ts,
                            "updated_at": ts,
                            "_source_order": [global_frontiers_row, local_order],
                        },
                    )
                    local_order += 1

                for raw_name, researcher_id in split_name_id_tokens(row.get("Researcher Ids")):
                    idx, match_rule, score = match_name_to_author(raw_name, authors, author_index)
                    if idx is None:
                        continue
                    authors[idx]["researcher_ids"] = sorted(set(authors[idx]["researcher_ids"] + [researcher_id]))
                    emit(
                        "author_identifier_claim",
                        {
                            "claim_id": generic_claim_id(
                                pub_id,
                                authors[idx]["occurrence_id"],
                                "researcher_id",
                                researcher_id,
                                str(raw_name).strip(),
                                match_rule,
                            ),
                            "occurrence_id": authors[idx]["occurrence_id"],
                            "publication_id": pub_id,
                            "claim_type": "researcher_id",
                            "claim_value": researcher_id,
                            "matched_name_raw": raw_name,
                            "match_rule": match_rule,
                            "confidence": "high" if (score or 0) >= 0.9 else "medium",
                            "score": score,
                            "source_refs": [make_source_ref("frontiers", source_file, sheet, row_idx, ["Researcher Ids", "Author Full Names"])],
                            "created_at": ts,
                            "updated_at": ts,
                            "_source_order": [global_frontiers_row, local_order],
                        },
                    )
                    local_order += 1

                reprint_claims, corresponding_indices = parse_reprint_segments(row.get("Reprint Addresses"), authors, author_index)
                address_claims = parse_addresses_segments(row.get("Addresses"), authors, author_index)
                for idx in corresponding_indices:
                    authors[idx]["is_corresponding_author"] = True

                for claim in reprint_claims + address_claims:
                    author = authors[claim["author_idx"]]
                    if claim.get("is_corresponding_author"):
                        author["is_corresponding_author"] = True
                    if claim.get("country_norm"):
                        author["_countries"].append(claim["country_norm"])
                    if claim.get("institution"):
                        author["_institutions"].append(claim["institution"])
                    if claim.get("address_text"):
                        author["_addresses"].append(claim["address_text"])
                    claim_rankings = institution_rankings.best_qs_ranks(
                        [claim.get("institution")],
                        countries=[claim.get("country_norm")] if claim.get("country_norm") else [],
                    )
                    if claim_rankings["qs_top200_rank"] is not None:
                        author["_qs_top200_ranks"].append(claim_rankings["qs_top200_rank"])
                    emit(
                        "author_affiliation_claim",
                        {
                            "claim_id": generic_claim_id(
                                pub_id,
                                author["occurrence_id"],
                                claim["source_field"],
                                claim.get("address_text"),
                                claim.get("institution"),
                            ),
                            "occurrence_id": author["occurrence_id"],
                            "publication_id": pub_id,
                            "source_field": claim["source_field"],
                            "institution": claim.get("institution"),
                            "address_text": claim.get("address_text"),
                            "country_raw": claim.get("country_norm"),
                            "country_norm": claim.get("country_norm"),
                            "qs_top200_rank": claim_rankings["qs_top200_rank"],
                            "world_top500_rank": None,
                            "is_reprint": bool(claim.get("is_reprint")),
                            "confidence": claim.get("confidence"),
                            "score": claim.get("score"),
                            "source_refs": [make_source_ref("frontiers", source_file, sheet, row_idx, [claim["source_field"], "Author Full Names"])],
                            "created_at": ts,
                            "updated_at": ts,
                            "_source_order": [global_frontiers_row, local_order],
                        },
                    )
                    local_order += 1

                valid_emails = parse_valid_emails(row.get("Email Addresses"))
                email_claims = bind_emails_to_authors(authors, valid_emails, corresponding_indices)
                source_anomalies["frontiers_email_unassigned_count"] += max(0, len(valid_emails) - len(email_claims))
                for claim in email_claims:
                    email = claim["email"]
                    local_part, domain = email.split("@", 1)
                    author = authors[claim["author_idx"]]
                    world_rankings = institution_rankings.best_world_top500_rank(
                        [domain],
                        countries=author["_countries"],
                    )
                    world_top500_rank = world_rankings["world_top500_rank"]
                    if world_top500_rank is not None:
                        author["_world_top500_email_ranks"].append(world_top500_rank)
                    emit(
                        "author_email_claim",
                        {
                            "claim_id": generic_claim_id(pub_id, author["occurrence_id"], "email", email),
                            "occurrence_id": author["occurrence_id"],
                            "publication_id": pub_id,
                            "email": email,
                            "email_domain": domain.lower(),
                            "world_top500_rank": world_top500_rank,
                            "local_part": local_part.lower(),
                            "binding_rule": claim["binding_rule"],
                            "confidence": claim["confidence"],
                            "score": claim["score"],
                            "source_refs": [make_source_ref("frontiers", source_file, sheet, row_idx, ["Email Addresses", "Reprint Addresses", "Author Full Names"])],
                            "created_at": ts,
                            "updated_at": ts,
                            "_source_order": [global_frontiers_row, local_order],
                        },
                    )
                    local_order += 1

                for author in authors:
                    chinese_identity = classify_chinese_identity(
                        parse_name_parts(author["author_name_raw"]),
                        author["_countries"],
                    )
                    emit(
                        "author_occurrence",
                        {
                            "occurrence_id": author["occurrence_id"],
                            "publication_id": author["publication_id"],
                            "author_order": author["author_order"],
                            "author_name_raw": author["author_name_raw"],
                            "author_name_norm": author["author_name_norm"],
                            "surname_norm": author["surname_norm"],
                            "given_names_norm": author["given_names_norm"],
                            "initials": author["initials"],
                            "name_aliases": author["name_aliases"],
                            "orcid": author["orcid"],
                            "researcher_ids": author["researcher_ids"],
                            "is_corresponding_author": author["is_corresponding_author"],
                            "chinese_identity": chinese_identity,
                            "source_refs": [make_source_ref("frontiers", source_file, sheet, row_idx, ["Author Full Names", "ORCIDs", "Researcher Ids", "Addresses", "Reprint Addresses"])],
                            "created_at": ts,
                            "updated_at": ts,
                            "_source_order": [global_frontiers_row, local_order],
                        },
                    )
                    local_order += 1

                    if write_compat_person:
                        country = Counter(author["_countries"]).most_common(1)[0][0] if author["_countries"] else None
                        affiliations = unique_preserve(author["_institutions"])
                        addresses = unique_preserve(author["_addresses"])
                        qs_top200_rank = merge_best_rank(*author["_qs_top200_ranks"])
                        world_top500_rank = merge_best_rank(*author["_world_top500_email_ranks"])
                        pid = person_id(author["author_name_norm"], author["orcid"], None)
                        emit(
                            "person",
                            {
                                "person_id": pid,
                                "canonical_person_id": pid,
                                "legacy_person_id": pid,
                                "name_original": author["author_name_raw"],
                                "name_norm": author["author_name_norm"],
                                "name_aliases": author["name_aliases"],
                                "matched_by_orcid": False,
                                "orcid": author["orcid"],
                                "researcher_ids": author["researcher_ids"],
                                "country": country,
                                "countries_observed": [country] if country else [],
                                "current_affiliation": affiliations[0] if affiliations else None,
                                "chinese_identity": chinese_identity,
                                "qs_top200_rank": qs_top200_rank,
                                "world_top500_rank": world_top500_rank,
                                "affiliations": affiliations,
                                "addresses": addresses,
                                "primary_email": None,
                                "emails": [],
                                "subjects": [],
                                "keywords": [],
                                "interests": [],
                                "h_index": None,
                                "i10_index": None,
                                "total_citations": None,
                                "average_citations": None,
                                "articles": None,
                                "google_scholar_url": None,
                                "sci_profile_url": None,
                                "match_methods": [],
                                "cluster_size": 1,
                                "source_systems": ["frontiers"],
                                "source_refs": [make_source_ref("frontiers", source_file, sheet, row_idx, ["Author Full Names", "ORCIDs", "Researcher Ids", "Addresses", "Reprint Addresses"])],
                                "created_at": ts,
                                "updated_at": ts,
                                "_source_order": [global_frontiers_row, local_order],
                            },
                        )
                        local_order += 1
                        emit(
                            "person_publication",
                            {
                                "relation_id": relation_id(pid, pub_id, "author"),
                                "person_id": pid,
                                "publication_id": pub_id,
                                "relation_type": "author",
                                "author_order": author["author_order"],
                                "author_name_in_paper": author["author_name_raw"],
                                "match_score": None,
                                "match_confidence": None,
                                "match_rules_hit": [],
                                "is_corresponding_author": author["is_corresponding_author"],
                                "source_refs": [make_source_ref("frontiers", source_file, sheet, row_idx, ["Author Full Names", "Addresses", "Reprint Addresses"])],
                                "created_at": ts,
                                "updated_at": ts,
                                "_source_order": [global_frontiers_row, local_order],
                            },
                        )
                        local_order += 1
    finally:
        for handle in handles.values():
            handle.close()

    return {
        "shard_input": str(shard_input.resolve()),
        "output_dir": str(output_dir.resolve()),
        "counts": counters,
        "source_anomalies": source_anomalies,
    }


def load_sorted_raw_records(paths):
    rows = []
    for path in paths:
        rows.extend(load_jsonl(Path(path)))
    rows.sort(key=source_order_key)
    return rows


def load_jsonl(path):
    rows = []
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            raw = line.strip()
            if not raw:
                continue
            item = json.loads(raw)
            if isinstance(item, dict):
                rows.append(item)
    return rows


def resolve_worker_count(value):
    if isinstance(value, int):
        return max(1, value)
    text = str(value).strip().lower()
    if text == "auto":
        cpu = os.cpu_count() or 1
        return max(1, min(8, cpu - 1))
    return max(1, int(text))


def resolve_shard_count(value, worker_count):
    if isinstance(value, int):
        return max(1, value)
    text = str(value).strip().lower()
    if text == "auto":
        return max(worker_count, min(32, worker_count * 2))
    return max(1, int(text))


def iter_rows_stream(json_path):
    if ijson is None:
        raise RuntimeError(
            "Streaming parser dependency missing: ijson. Install with `python -m pip install ijson`."
        )

    source_file = str(json_path)
    row_index_by_sheet = defaultdict(int)
    collecting = False
    target_prefix = None
    target_sheet = None
    depth = 0
    builder = None

    with open(json_path, "rb") as f:
        for prefix, event, value in ijson.parse(f):
            if prefix == "source_file" and event in ("string", "number", "boolean"):
                source_file = str(value)
                continue

            if (not collecting) and event == "start_map" and prefix.startswith("sheets.") and prefix.endswith(".item"):
                target_prefix = prefix
                target_sheet = prefix[len("sheets.") : -len(".item")]
                row_index_by_sheet[target_sheet] += 1
                builder = ObjectBuilder()
                builder.event(event, value)
                depth = 1
                collecting = True
                continue

            if not collecting:
                continue

            if not prefix.startswith(target_prefix):
                continue

            builder.event(event, value)
            if event in ("start_map", "start_array"):
                depth += 1
            elif event in ("end_map", "end_array"):
                depth -= 1

            if depth == 0:
                yield source_file, target_sheet, row_index_by_sheet[target_sheet], builder.value
                collecting = False
                target_prefix = None
                target_sheet = None
                builder = None


def iter_rows_stream_jsonl(jsonl_path):
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            raw = line.strip()
            if not raw:
                continue
            item = json.loads(raw)
            if not isinstance(item, dict):
                continue
            row = item.get("row")
            if not isinstance(row, dict):
                continue
            source_file = str(item.get("source_file") or jsonl_path)
            sheet = str(item.get("sheet") or "unknown")
            row_index = coerce_int(item.get("row_index"))
            yield source_file, sheet, row_index if row_index is not None else line_no, row


def iter_rows_stream_parquet(parquet_path):
    meta_columns = {"source_file", "source_group", "converted_at", "sheet", "row_index"}
    for item in iter_parquet_rows(Path(parquet_path)):
        source_file = str(item.get("source_file") or parquet_path)
        sheet = str(item.get("sheet") or "unknown")
        row_index = coerce_int(item.get("row_index"))
        row = {
            key: value
            for key, value in item.items()
            if key not in meta_columns and value not in (None, "")
        }
        yield source_file, sheet, row_index if row_index is not None else 0, row


def iter_input_rows(json_path, jsonl_path, parquet_path):
    if parquet_path:
        yield from iter_rows_stream_parquet(parquet_path)
        return
    if jsonl_path:
        yield from iter_rows_stream_jsonl(jsonl_path)
        return
    if json_path:
        yield from iter_rows_stream(json_path)
        return
    raise ValueError("either json_path, jsonl_path, or parquet_path must be provided")


def iter_matched_records(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                yield item
        return

    if isinstance(data, dict):
        if isinstance(data.get("matches_by_scholar"), dict):
            for scholar_name, rows in data["matches_by_scholar"].items():
                if not isinstance(rows, list):
                    continue
                for item in rows:
                    if not isinstance(item, dict):
                        continue
                    if "scholar_name" not in item:
                        item["scholar_name"] = scholar_name
                    yield item
            return
        if isinstance(data.get("matches"), list):
            for item in data["matches"]:
                if isinstance(item, dict):
                    yield item
            return


def infer_match_score(item):
    score = coerce_float(item.get("match_score"))
    rules = []
    if score is not None:
        rules.append("score_from_input")
    else:
        score = 0.55
        scholar_norm = normalize_text(item.get("scholar_name"))
        afn_norm = normalize_text(item.get("author_full_names"))
        if scholar_norm and afn_norm and scholar_norm in afn_norm:
            score += 0.2
            rules.append("name_contains")
        if item.get("doi"):
            score += 0.1
            rules.append("doi_present")
    ext_rules = item.get("match_rules_hit")
    if isinstance(ext_rules, list):
        for r in ext_rules:
            if isinstance(r, str):
                rules.append(r)
    rules = sorted(set(rules))
    score = min(1.0, max(0.0, score))
    if score >= 0.85:
        conf = "high"
    elif score >= 0.65:
        conf = "medium"
    else:
        conf = "low"
    return score, conf, rules


def main():
    parser = argparse.ArgumentParser(description="Transform scholars/frontiers JSON files into schema_v1 entities.")
    parser.add_argument("--scholars-json", default=None, help="Path to scholars json")
    parser.add_argument("--frontiers-json", default=None, help="Path to frontiers json")
    parser.add_argument("--scholars-jsonl", default=None, help="Path to scholars row-oriented jsonl")
    parser.add_argument("--frontiers-jsonl", default=None, help="Path to frontiers row-oriented jsonl")
    parser.add_argument("--scholars-parquet", default=None, help="Path to scholars row-oriented parquet")
    parser.add_argument("--frontiers-parquet", default=None, help="Path to frontiers row-oriented parquet")
    parser.add_argument("--output-dir", required=True, help="Output directory")
    parser.add_argument("--qs-top200-json", default=None, help="Optional QS Top 200 ranking json path")
    parser.add_argument("--world-top500-json", default=None, help="Optional World Top 500 ranking json path")
    parser.add_argument(
        "--matched-json",
        nargs="*",
        default=[],
        help="Optional matched relation files (flat list or matches_by_scholar structure)",
    )
    parser.add_argument(
        "--write-compat-person",
        action="store_true",
        help="Write compatibility person/person_publication outputs in silver stage. Disabled by default.",
    )
    parser.add_argument(
        "--write-debug-jsonl",
        action="store_true",
        help="Write JSONL debug copies alongside parquet outputs.",
    )
    parser.add_argument(
        "--silver-workers",
        default="auto",
        help="Worker count for frontiers shard processing. Default: auto.",
    )
    parser.add_argument(
        "--silver-shards",
        default="auto",
        help="Shard count for frontiers preprocessing. Default: auto.",
    )
    args = parser.parse_args()

    using_json = bool(args.scholars_json and args.frontiers_json)
    using_jsonl = bool(args.scholars_jsonl and args.frontiers_jsonl)
    using_parquet = bool(args.scholars_parquet and args.frontiers_parquet)
    if sum(1 for flag in (using_json, using_jsonl, using_parquet) if flag) != 1:
        raise ValueError(
            "provide exactly one input pair: json, jsonl, or parquet"
        )

    ts = now_iso()
    write_compat_person = bool(args.write_compat_person)
    write_debug_jsonl = bool(args.write_debug_jsonl)
    silver_workers = resolve_worker_count(args.silver_workers)
    silver_shards = resolve_shard_count(args.silver_shards, silver_workers)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    silver_tmp_dir = out_dir / "_tmp"
    institution_rankings = InstitutionRankings.from_paths(
        Path(args.qs_top200_json) if args.qs_top200_json else None,
        Path(args.world_top500_json) if args.world_top500_json else None,
    )

    quality = init_quality()
    source_anomalies = {
        "scholars_orcid_invalid_count": 0,
        "frontiers_orcid_invalid_count": 0,
        "frontiers_email_unassigned_count": 0,
    }
    ranking_hits = {
        "author_affiliation_claim_qs_hits": 0,
        "author_affiliation_claim_world_top500_hits": 0,
        "author_email_claim_world_top500_hits": 0,
        "person_seed_qs_hits": 0,
        "person_seed_world_top500_hits": 0,
        "person_qs_hits": 0,
        "person_world_top500_hits": 0,
    }

    persons = {}
    publications = {}
    relations = {}
    person_seed_records = {}
    matched_relation_seed_records = {}
    author_occurrence_records = {}
    identifier_claim_records = {}
    affiliation_claim_records = {}
    email_claim_records = {}

    scholars_name_map = defaultdict(list)
    frontiers_row_to_pubid = {}
    frontiers_doi_to_pubid = {}
    global_frontiers_row = 0
    silver_tmp_dir_path = None
    silver_worker_seconds = 0.0
    silver_merge_seconds = 0.0
    silver_preprocess_seconds = 0.0
    silver_shard_counts = []

    # 1) Scholars -> person (streaming)
    for source_file, sheet, row_idx, row in iter_input_rows(args.scholars_json, args.scholars_jsonl, args.scholars_parquet):
        name_original = row.get("name")
        name_norm = normalize_text(name_original)
        if not name_norm:
            continue

        raw_orcid = row.get("orcid")
        orcid = parse_orcid(raw_orcid)
        if raw_orcid not in (None, "") and orcid is None:
            source_anomalies["scholars_orcid_invalid_count"] += 1

        country = row.get("countries")
        scholar_countries = split_semicolon(country) if country else []
        scholar_rankings = institution_rankings.best_qs_ranks(
            [row.get("current_affiliation")] + split_semicolon(row.get("affiliations")),
            countries=scholar_countries,
        )
        pid = person_id(name_norm, orcid, country)
        rec = {
            "person_id": pid,
            "canonical_person_id": pid,
            "legacy_person_id": pid,
            "name_original": str(name_original).strip(),
            "name_norm": name_norm,
            "name_aliases": name_aliases(name_original),
            "matched_by_orcid": False,
            "orcid": orcid,
            "researcher_ids": [],
            "country": country,
            "countries_observed": [country] if country else [],
            "current_affiliation": row.get("current_affiliation"),
            "qs_top200_rank": scholar_rankings["qs_top200_rank"],
            "world_top500_rank": None,
            "affiliations": [],
            "addresses": [],
            "primary_email": None,
            "emails": [],
            "subjects": split_semicolon(row.get("subjects")),
            "keywords": split_semicolon(row.get("keywords")),
            "interests": split_semicolon(row.get("interests")),
            "h_index": coerce_int(row.get("h_index")),
            "i10_index": coerce_int(row.get("i10_index")),
            "total_citations": coerce_int(row.get("total_citations")),
            "average_citations": coerce_float(row.get("average_citations")),
            "articles": coerce_int(row.get("articles")),
            "google_scholar_url": row.get("google_scholar"),
            "sci_profile_url": row.get("sci_profile"),
            "match_methods": [],
            "cluster_size": 1,
            "source_systems": ["scholars"],
            "source_refs": [make_source_ref("scholars", source_file, sheet, row_idx, sorted(row.keys()))],
            "created_at": ts,
            "updated_at": ts,
        }
        upsert_seed_person(person_seed_records, dict(rec))
        if write_compat_person:
            compat_rec = dict(rec)
            compat_rec["chinese_identity"] = classify_chinese_identity(
                parse_name_parts(name_original),
                scholar_countries,
            )
            upsert_person(persons, compat_rec, quality)
            quality_track(quality, "person", compat_rec)
        scholars_name_map[name_norm].append(pid)

    # 2) Frontiers -> publication + person(author) + relation(author) via shard preprocessing + multiprocessing
    silver_tmp_dir_path = silver_tmp_dir / "frontiers_shards"
    preprocess_start = perf_counter()
    shard_inputs = write_frontiers_shard_inputs(
        args.frontiers_json,
        args.frontiers_jsonl,
        args.frontiers_parquet,
        silver_shards,
        silver_tmp_dir_path / "inputs",
    )
    silver_preprocess_seconds = round(perf_counter() - preprocess_start, 3)
    global_frontiers_row = shard_inputs["global_frontiers_row"]
    frontiers_row_to_pubid = shard_inputs["frontiers_row_to_pubid"]
    frontiers_doi_to_pubid = shard_inputs["frontiers_doi_to_pubid"]
    silver_shard_counts = list(shard_inputs["shard_row_counts"])

    worker_start = perf_counter()
    shard_results = []
    shard_output_root = silver_tmp_dir_path / "outputs"
    shard_output_root.mkdir(parents=True, exist_ok=True)
    with ProcessPoolExecutor(max_workers=silver_workers) as executor:
        future_map = {}
        for shard_idx, shard_input_path in enumerate(shard_inputs["shard_paths"]):
            shard_output_dir = shard_output_root / f"shard_{shard_idx:04d}"
            future = executor.submit(
                process_frontiers_shard_worker,
                str(shard_input_path.resolve()),
                str(shard_output_dir.resolve()),
                ts,
                write_compat_person,
                args.qs_top200_json,
                args.world_top500_json,
            )
            future_map[future] = shard_idx
        for future in as_completed(future_map):
            shard_result = future.result()
            shard_result["shard_idx"] = future_map[future]
            shard_results.append(shard_result)
    silver_worker_seconds = round(perf_counter() - worker_start, 3)
    shard_results.sort(key=lambda item: int(item["shard_idx"]))

    merge_start = perf_counter()
    source_anomalies["frontiers_orcid_invalid_count"] += sum(
        int(item["source_anomalies"]["frontiers_orcid_invalid_count"]) for item in shard_results
    )
    source_anomalies["frontiers_email_unassigned_count"] += sum(
        int(item["source_anomalies"]["frontiers_email_unassigned_count"]) for item in shard_results
    )

    publication_records = load_sorted_raw_records(
        [Path(item["output_dir"]) / "publication.raw.jsonl" for item in shard_results]
    )
    for record in publication_records:
        clean_record = strip_internal_fields(record)
        quality_track(quality, "publication", clean_record)
        upsert_publication(publications, clean_record, quality)

    author_occurrence_records_raw = load_sorted_raw_records(
        [Path(item["output_dir"]) / "author_occurrence.raw.jsonl" for item in shard_results]
    )
    for record in author_occurrence_records_raw:
        upsert_author_occurrence(author_occurrence_records, strip_internal_fields(record), quality)

    identifier_claim_records_raw = load_sorted_raw_records(
        [Path(item["output_dir"]) / "author_identifier_claim.raw.jsonl" for item in shard_results]
    )
    for record in identifier_claim_records_raw:
        upsert_claim_record(
            identifier_claim_records,
            strip_internal_fields(record),
            "author_identifier_claim",
            quality,
            ["occurrence_id", "publication_id", "claim_type", "claim_value", "matched_name_raw", "match_rule"],
        )

    affiliation_claim_records_raw = load_sorted_raw_records(
        [Path(item["output_dir"]) / "author_affiliation_claim.raw.jsonl" for item in shard_results]
    )
    for record in affiliation_claim_records_raw:
        upsert_claim_record(
            affiliation_claim_records,
            strip_internal_fields(record),
            "author_affiliation_claim",
            quality,
            ["occurrence_id", "publication_id", "source_field", "institution", "address_text", "country_norm", "is_reprint"],
        )

    email_claim_records_raw = load_sorted_raw_records(
        [Path(item["output_dir"]) / "author_email_claim.raw.jsonl" for item in shard_results]
    )
    for record in email_claim_records_raw:
        upsert_claim_record(
            email_claim_records,
            strip_internal_fields(record),
            "author_email_claim",
            quality,
            ["occurrence_id", "publication_id", "email", "email_domain", "local_part", "binding_rule"],
        )

    if write_compat_person:
        compat_person_records_raw = load_sorted_raw_records(
            [Path(item["output_dir"]) / "person.raw.jsonl" for item in shard_results]
        )
        for record in compat_person_records_raw:
            clean_record = strip_internal_fields(record)
            upsert_person(persons, clean_record, quality)
            quality_track(quality, "person", clean_record)

        compat_relation_records_raw = load_sorted_raw_records(
            [Path(item["output_dir"]) / "person_publication.raw.jsonl" for item in shard_results]
        )
        for record in compat_relation_records_raw:
            clean_record = strip_internal_fields(record)
            upsert_relation(relations, clean_record, quality)
            quality_track(quality, "person_publication", clean_record)

    silver_merge_seconds = round(perf_counter() - merge_start, 3)

    # 3) Optional matched relations -> person_publication(relation_type=matched)
    matched_records_count = 0
    for matched_path in args.matched_json:
        mpath = Path(matched_path)
        if not mpath.exists():
            continue
        for item in iter_matched_records(mpath):
            scholar_name = item.get("scholar_name") or item.get("name")
            scholar_norm = normalize_text(scholar_name)
            if not scholar_norm:
                continue

            candidate_pids = scholars_name_map.get(scholar_norm, [])
            if not candidate_pids:
                # fallback: create a minimal person entry
                pid = person_id(scholar_norm, None, None)
                matched_qs_rankings = institution_rankings.best_qs_ranks([item.get("scholar_affiliation")])
                minimal_person = {
                    "person_id": pid,
                    "canonical_person_id": pid,
                    "legacy_person_id": pid,
                    "name_original": str(scholar_name).strip(),
                    "name_norm": scholar_norm,
                    "name_aliases": name_aliases(scholar_name),
                    "matched_by_orcid": False,
                    "orcid": None,
                    "researcher_ids": [],
                    "country": None,
                    "countries_observed": [],
                    "current_affiliation": item.get("scholar_affiliation"),
                    "qs_top200_rank": matched_qs_rankings["qs_top200_rank"],
                    "world_top500_rank": None,
                    "affiliations": [],
                    "addresses": [],
                    "primary_email": None,
                    "emails": [],
                    "subjects": [],
                    "keywords": [],
                    "interests": [],
                    "h_index": None,
                    "i10_index": None,
                    "total_citations": None,
                    "average_citations": None,
                    "articles": None,
                    "google_scholar_url": None,
                    "sci_profile_url": None,
                    "match_methods": [],
                    "cluster_size": 1,
                    "source_systems": ["matched"],
                    "source_refs": [make_source_ref("matched", str(mpath), "N/A", None, sorted(item.keys()))],
                    "created_at": ts,
                    "updated_at": ts,
                }
                upsert_seed_person(person_seed_records, dict(minimal_person))
                if write_compat_person:
                    upsert_person(persons, minimal_person, quality)
                    quality_track(quality, "person", minimal_person)
                candidate_pids = [pid]

            pub_id = None
            if item.get("publication_id"):
                pub_id = str(item["publication_id"])
            elif item.get("doi") and str(item["doi"]) in frontiers_doi_to_pubid:
                pub_id = frontiers_doi_to_pubid[str(item["doi"])]
            elif item.get("frontiers_row_index") is not None:
                idx = coerce_int(item.get("frontiers_row_index"))
                if idx in frontiers_row_to_pubid:
                    pub_id = frontiers_row_to_pubid[idx]

            if not pub_id:
                continue

            score, confidence, rules = infer_match_score(item)
            for pid in candidate_pids:
                rid = relation_id(pid, pub_id, "matched")
                rec = {
                    "relation_id": rid,
                    "person_id": pid,
                    "publication_id": pub_id,
                    "relation_type": "matched",
                    "author_order": None,
                    "author_name_in_paper": item.get("author_full_names"),
                    "match_score": score,
                    "match_confidence": confidence,
                    "match_rules_hit": rules,
                    "is_corresponding_author": None,
                    "source_refs": [make_source_ref("matched", str(mpath), "N/A", item.get("frontiers_row_index"), sorted(item.keys()))],
                    "created_at": ts,
                    "updated_at": ts,
                }
                upsert_seed_relation(matched_relation_seed_records, dict(rec))
                if write_compat_person:
                    upsert_relation(relations, rec, quality)
                    quality_track(quality, "person_publication", rec)
                matched_records_count += 1

    if write_compat_person:
        persons, relations = reconcile_persons_by_orcid(persons, relations)
        person_rows = list(persons.values())
        relation_rows = list(relations.values())
    else:
        person_rows = []
        relation_rows = []
    person_seed_rows = list(person_seed_records.values())
    matched_relation_seed_rows = list(matched_relation_seed_records.values())
    publication_rows = list(publications.values())
    author_occurrence_rows = list(author_occurrence_records.values())
    identifier_claim_rows = list(identifier_claim_records.values())
    affiliation_claim_rows = list(affiliation_claim_records.values())
    email_claim_rows = list(email_claim_records.values())
    ranking_hits["author_affiliation_claim_qs_hits"] = sum(
        1 for row in affiliation_claim_rows if row.get("qs_top200_rank") is not None
    )
    ranking_hits["author_affiliation_claim_world_top500_hits"] = sum(
        1 for row in affiliation_claim_rows if row.get("world_top500_rank") is not None
    )
    ranking_hits["author_email_claim_world_top500_hits"] = sum(
        1 for row in email_claim_rows if row.get("world_top500_rank") is not None
    )
    ranking_hits["person_seed_qs_hits"] = sum(1 for row in person_seed_rows if row.get("qs_top200_rank") is not None)
    ranking_hits["person_seed_world_top500_hits"] = sum(
        1 for row in person_seed_rows if row.get("world_top500_rank") is not None
    )
    ranking_hits["person_qs_hits"] = sum(1 for row in person_rows if row.get("qs_top200_rank") is not None)
    ranking_hits["person_world_top500_hits"] = sum(
        1 for row in person_rows if row.get("world_top500_rank") is not None
    )
    author_occurrence_chinese_identity_counts = chinese_identity_counts(author_occurrence_rows)
    person_chinese_identity_counts = chinese_identity_counts(person_rows)

    for row in author_occurrence_rows:
        quality_track(quality, "author_occurrence", row)
    for row in identifier_claim_rows:
        quality_track(quality, "author_identifier_claim", row)
    for row in affiliation_claim_rows:
        quality_track(quality, "author_affiliation_claim", row)
    for row in email_claim_rows:
        quality_track(quality, "author_email_claim", row)

    parquet_outputs = {
        "publication": out_dir / "publication.parquet",
        "author_occurrence": out_dir / "author_occurrence.parquet",
        "author_identifier_claim": out_dir / "author_identifier_claim.parquet",
        "author_affiliation_claim": out_dir / "author_affiliation_claim.parquet",
        "author_email_claim": out_dir / "author_email_claim.parquet",
        "person_seed": out_dir / "person_seed.parquet",
        "matched_relation_seed": out_dir / "matched_relation_seed.parquet",
        "person": out_dir / "person.parquet" if write_compat_person else None,
        "person_publication": out_dir / "person_publication.parquet" if write_compat_person else None,
    }
    jsonl_outputs = {
        "publication": out_dir / "publication.jsonl" if write_debug_jsonl else None,
        "author_occurrence": out_dir / "author_occurrence.jsonl" if write_debug_jsonl else None,
        "author_identifier_claim": out_dir / "author_identifier_claim.jsonl" if write_debug_jsonl else None,
        "author_affiliation_claim": out_dir / "author_affiliation_claim.jsonl" if write_debug_jsonl else None,
        "author_email_claim": out_dir / "author_email_claim.jsonl" if write_debug_jsonl else None,
        "person_seed": out_dir / "person_seed.jsonl" if write_debug_jsonl else None,
        "matched_relation_seed": out_dir / "matched_relation_seed.jsonl" if write_debug_jsonl else None,
        "person": out_dir / "person.jsonl" if write_compat_person and write_debug_jsonl else None,
        "person_publication": out_dir / "person_publication.jsonl" if write_compat_person and write_debug_jsonl else None,
    }
    write_timings = {}

    def write_entity_outputs(name, rows):
        parquet_path = parquet_outputs[name]
        start = perf_counter()
        write_pylist_parquet(parquet_path, rows)
        write_timings[f"{name}_parquet_seconds"] = round(perf_counter() - start, 3)
        jsonl_path = jsonl_outputs[name]
        if jsonl_path is not None:
            start = perf_counter()
            write_jsonl(jsonl_path, rows)
            write_timings[f"{name}_jsonl_seconds"] = round(perf_counter() - start, 3)

    write_entity_outputs("publication", publication_rows)
    write_entity_outputs("author_occurrence", author_occurrence_rows)
    write_entity_outputs("author_identifier_claim", identifier_claim_rows)
    write_entity_outputs("author_affiliation_claim", affiliation_claim_rows)
    write_entity_outputs("author_email_claim", email_claim_rows)
    write_entity_outputs("person_seed", person_seed_rows)
    write_entity_outputs("matched_relation_seed", matched_relation_seed_rows)
    if write_compat_person:
        write_entity_outputs("person", person_rows)
        write_entity_outputs("person_publication", relation_rows)

    id_safety_stats = {
        "person_v1": {"total_docs": 0, "hashed_id_count": 0},
        "publication_v1": {"total_docs": 0, "hashed_id_count": 0},
        "person_publication_v1": {"total_docs": 0, "hashed_id_count": 0},
        "author_occurrence_v1": {"total_docs": 0, "hashed_id_count": 0},
        "author_identifier_claim_v1": {"total_docs": 0, "hashed_id_count": 0},
        "author_affiliation_claim_v1": {"total_docs": 0, "hashed_id_count": 0},
        "author_email_claim_v1": {"total_docs": 0, "hashed_id_count": 0},
    }
    write_bulk(
        out_dir / "publication.bulk.ndjson",
        "publication_v1",
        "publication_id",
        publication_rows,
        id_safety_stats,
    )
    write_bulk(
        out_dir / "author_occurrence.bulk.ndjson",
        "author_occurrence_v1",
        "occurrence_id",
        author_occurrence_rows,
        id_safety_stats,
    )
    write_bulk(
        out_dir / "author_identifier_claim.bulk.ndjson",
        "author_identifier_claim_v1",
        "claim_id",
        identifier_claim_rows,
        id_safety_stats,
    )
    write_bulk(
        out_dir / "author_affiliation_claim.bulk.ndjson",
        "author_affiliation_claim_v1",
        "claim_id",
        affiliation_claim_rows,
        id_safety_stats,
    )
    write_bulk(
        out_dir / "author_email_claim.bulk.ndjson",
        "author_email_claim_v1",
        "claim_id",
        email_claim_rows,
        id_safety_stats,
    )
    if write_compat_person:
        write_bulk(out_dir / "person.bulk.ndjson", "person_v1", "person_id", person_rows, id_safety_stats)
        write_bulk(
            out_dir / "person_publication.bulk.ndjson",
            "person_publication_v1",
            "relation_id",
            relation_rows,
            id_safety_stats,
        )

    quality_report = {
        "generated_at": ts,
        "source_anomalies": source_anomalies,
        "entity_quality": finalize_quality(quality),
        "id_safety_stats": id_safety_stats,
    }
    quality_report_path = out_dir / "quality_report.json"
    with open(quality_report_path, "w", encoding="utf-8") as f:
        json.dump(quality_report, f, ensure_ascii=False, indent=2)

    if silver_tmp_dir.exists():
        remove_tree(silver_tmp_dir)

    summary = {
        "generated_at": ts,
        "inputs": {
            "scholars_json": str(args.scholars_json) if args.scholars_json else None,
            "frontiers_json": str(args.frontiers_json) if args.frontiers_json else None,
            "scholars_jsonl": str(args.scholars_jsonl) if args.scholars_jsonl else None,
            "frontiers_jsonl": str(args.frontiers_jsonl) if args.frontiers_jsonl else None,
            "scholars_parquet": str(args.scholars_parquet) if args.scholars_parquet else None,
            "frontiers_parquet": str(args.frontiers_parquet) if args.frontiers_parquet else None,
            "qs_top200_json": str(args.qs_top200_json) if args.qs_top200_json else None,
            "world_top500_json": str(args.world_top500_json) if args.world_top500_json else None,
            "matched_json": [str(p) for p in args.matched_json],
            "write_compat_person": write_compat_person,
            "write_debug_jsonl": write_debug_jsonl,
            "silver_workers": silver_workers,
            "silver_shards": silver_shards,
            "input_format": "parquet" if using_parquet else ("jsonl" if using_jsonl else "json"),
        },
        "outputs": {
            "person_parquet": str(parquet_outputs["person"]) if parquet_outputs["person"] else None,
            "publication_parquet": str(parquet_outputs["publication"]),
            "person_publication_parquet": str(parquet_outputs["person_publication"]) if parquet_outputs["person_publication"] else None,
            "author_occurrence_parquet": str(parquet_outputs["author_occurrence"]),
            "author_identifier_claim_parquet": str(parquet_outputs["author_identifier_claim"]),
            "author_affiliation_claim_parquet": str(parquet_outputs["author_affiliation_claim"]),
            "author_email_claim_parquet": str(parquet_outputs["author_email_claim"]),
            "person_seed_parquet": str(parquet_outputs["person_seed"]),
            "matched_relation_seed_parquet": str(parquet_outputs["matched_relation_seed"]),
            "person_jsonl": str(jsonl_outputs["person"]) if jsonl_outputs["person"] else None,
            "publication_jsonl": str(jsonl_outputs["publication"]) if jsonl_outputs["publication"] else None,
            "person_publication_jsonl": str(jsonl_outputs["person_publication"]) if jsonl_outputs["person_publication"] else None,
            "author_occurrence_jsonl": str(jsonl_outputs["author_occurrence"]) if jsonl_outputs["author_occurrence"] else None,
            "author_identifier_claim_jsonl": str(jsonl_outputs["author_identifier_claim"]) if jsonl_outputs["author_identifier_claim"] else None,
            "author_affiliation_claim_jsonl": str(jsonl_outputs["author_affiliation_claim"]) if jsonl_outputs["author_affiliation_claim"] else None,
            "author_email_claim_jsonl": str(jsonl_outputs["author_email_claim"]) if jsonl_outputs["author_email_claim"] else None,
            "person_seed_jsonl": str(jsonl_outputs["person_seed"]) if jsonl_outputs["person_seed"] else None,
            "matched_relation_seed_jsonl": str(jsonl_outputs["matched_relation_seed"]) if jsonl_outputs["matched_relation_seed"] else None,
            "person_bulk": str(out_dir / "person.bulk.ndjson") if write_compat_person else None,
            "publication_bulk": str(out_dir / "publication.bulk.ndjson"),
            "person_publication_bulk": str(out_dir / "person_publication.bulk.ndjson") if write_compat_person else None,
            "author_occurrence_bulk": str(out_dir / "author_occurrence.bulk.ndjson"),
            "author_identifier_claim_bulk": str(out_dir / "author_identifier_claim.bulk.ndjson"),
            "author_affiliation_claim_bulk": str(out_dir / "author_affiliation_claim.bulk.ndjson"),
            "author_email_claim_bulk": str(out_dir / "author_email_claim.bulk.ndjson"),
            "quality_report": str(quality_report_path),
            "tmp_dir": str(silver_tmp_dir_path.resolve()) if silver_tmp_dir_path else None,
        },
        "counts": {
            "person_seed": len(person_seed_rows),
            "person": len(person_rows),
            "publication": len(publication_rows),
            "matched_relation_seed": len(matched_relation_seed_rows),
            "person_publication": len(relation_rows),
            "author_occurrence": len(author_occurrence_rows),
            "author_identifier_claim": len(identifier_claim_rows),
            "author_affiliation_claim": len(affiliation_claim_rows),
            "author_email_claim": len(email_claim_rows),
            "matched_relations_written": matched_records_count,
        },
        "ranking_hits": ranking_hits,
        "author_occurrence_chinese_identity_counts": author_occurrence_chinese_identity_counts,
        "person_chinese_identity_counts": person_chinese_identity_counts,
        "file_sizes": {
            "person_parquet_bytes": file_size_or_none(parquet_outputs["person"]),
            "publication_parquet_bytes": file_size_or_none(parquet_outputs["publication"]),
            "person_publication_parquet_bytes": file_size_or_none(parquet_outputs["person_publication"]),
            "author_occurrence_parquet_bytes": file_size_or_none(parquet_outputs["author_occurrence"]),
            "author_identifier_claim_parquet_bytes": file_size_or_none(parquet_outputs["author_identifier_claim"]),
            "author_affiliation_claim_parquet_bytes": file_size_or_none(parquet_outputs["author_affiliation_claim"]),
            "author_email_claim_parquet_bytes": file_size_or_none(parquet_outputs["author_email_claim"]),
            "person_seed_parquet_bytes": file_size_or_none(parquet_outputs["person_seed"]),
            "matched_relation_seed_parquet_bytes": file_size_or_none(parquet_outputs["matched_relation_seed"]),
            "person_jsonl_bytes": file_size_or_none(jsonl_outputs["person"]),
            "publication_jsonl_bytes": file_size_or_none(jsonl_outputs["publication"]),
            "person_publication_jsonl_bytes": file_size_or_none(jsonl_outputs["person_publication"]),
            "author_occurrence_jsonl_bytes": file_size_or_none(jsonl_outputs["author_occurrence"]),
            "author_identifier_claim_jsonl_bytes": file_size_or_none(jsonl_outputs["author_identifier_claim"]),
            "author_affiliation_claim_jsonl_bytes": file_size_or_none(jsonl_outputs["author_affiliation_claim"]),
            "author_email_claim_jsonl_bytes": file_size_or_none(jsonl_outputs["author_email_claim"]),
            "person_seed_jsonl_bytes": file_size_or_none(jsonl_outputs["person_seed"]),
            "matched_relation_seed_jsonl_bytes": file_size_or_none(jsonl_outputs["matched_relation_seed"]),
            "person_bulk_bytes": file_size_or_none(out_dir / "person.bulk.ndjson") if write_compat_person else None,
            "publication_bulk_bytes": file_size_or_none(out_dir / "publication.bulk.ndjson"),
            "person_publication_bulk_bytes": file_size_or_none(out_dir / "person_publication.bulk.ndjson") if write_compat_person else None,
            "author_occurrence_bulk_bytes": file_size_or_none(out_dir / "author_occurrence.bulk.ndjson"),
            "author_identifier_claim_bulk_bytes": file_size_or_none(out_dir / "author_identifier_claim.bulk.ndjson"),
            "author_affiliation_claim_bulk_bytes": file_size_or_none(out_dir / "author_affiliation_claim.bulk.ndjson"),
            "author_email_claim_bulk_bytes": file_size_or_none(out_dir / "author_email_claim.bulk.ndjson"),
        },
        "write_timings": write_timings,
        "parallelism": {
            "silver_workers": silver_workers,
            "silver_shards": silver_shards,
            "tmp_dir": str(silver_tmp_dir_path.resolve()) if silver_tmp_dir_path else None,
            "shard_count": len(silver_shard_counts),
            "shard_row_counts": silver_shard_counts,
            "preprocess_seconds": silver_preprocess_seconds,
            "worker_seconds": silver_worker_seconds,
            "merge_seconds": silver_merge_seconds,
        },
    }
    with open(out_dir / "transform_summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(json.dumps(summary["counts"], ensure_ascii=False))


if __name__ == "__main__":
    main()
