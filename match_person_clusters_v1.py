import argparse
import itertools
import json
import os
import shutil
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from time import perf_counter
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

from etl_transform_schema_v1 import (
    CHINESE_IDENTITY_DOMESTIC,
    CHINESE_IDENTITY_FOREIGN,
    CHINESE_IDENTITY_OVERSEAS,
    chinese_identity_counts,
    classify_chinese_identity,
    file_size_or_none,
    finalize_quality,
    init_quality,
    normalize_compact,
    normalize_text,
    now_iso,
    parse_name_parts,
    person_id,
    quality_track,
    relation_id,
    sha1_text,
    split_semicolon,
    unique_preserve,
)
from institution_rankings import merge_best_rank
from pipeline_storage import load_parquet_rows, write_bulk, write_jsonl, write_pylist_parquet


MAX_FUZZY_BLOCK_SIZE = 200
STRONG_NAME_THRESHOLD = 0.93
MATCH_WORKER_NODES: List[Dict[str, object]] = []


class UnionFind:
    def __init__(self, node_ids: Iterable[str]) -> None:
        self.parent = {node_id: node_id for node_id in node_ids}
        self.rank = {node_id: 0 for node_id in node_ids}

    def find(self, value: str) -> str:
        parent = self.parent[value]
        if parent != value:
            self.parent[value] = self.find(parent)
        return self.parent[value]

    def union(self, left: str, right: str) -> str:
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root == right_root:
            return left_root
        if self.rank[left_root] < self.rank[right_root]:
            left_root, right_root = right_root, left_root
        self.parent[right_root] = left_root
        if self.rank[left_root] == self.rank[right_root]:
            self.rank[left_root] += 1
        return left_root


def load_jsonl(path: Path) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            raw = line.strip()
            if not raw:
                continue
            item = json.loads(raw)
            if isinstance(item, dict):
                rows.append(item)
    return rows


def remove_tree(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)


def resolve_worker_count(value: object) -> int:
    text = str(value).strip().lower()
    if text == "auto":
        cpu = os.cpu_count() or 1
        return max(1, min(8, cpu - 1))
    return max(1, int(text))


def serialize_worker_node(node: Dict[str, object]) -> Dict[str, object]:
    return {
        "node_type": node["node_type"],
        "name_norm": node.get("name_norm"),
        "compact_full": node.get("compact_full"),
        "name_aliases_norm": sorted(normalized_set(node.get("name_aliases", set()) or [])),
        "surname_norm": node.get("surname_norm"),
        "given_names_norm": node.get("given_names_norm"),
        "initials": node.get("initials"),
        "first_initial": node.get("first_initial"),
        "orcid_set": sorted(str(value) for value in node.get("orcid_set", set()) if value),
        "researcher_id_set": sorted(str(value) for value in node.get("researcher_id_set", set()) if value),
        "institution_fps": sorted(str(value) for value in node.get("institution_fps", set()) if value),
        "country_fps": sorted(str(value) for value in node.get("country_fps", set()) if value),
        "publication_ids": sorted(str(value) for value in node.get("publication_ids", set()) if value),
    }


def init_match_worker(snapshot_path_text: str) -> None:
    global MATCH_WORKER_NODES
    with open(snapshot_path_text, "r", encoding="utf-8") as handle:
        MATCH_WORKER_NODES = json.load(handle)


def candidate_conflicts_compact(left: Dict[str, object], right: Dict[str, object]) -> List[str]:
    left_orcid = set(left.get("orcid_set", []))
    right_orcid = set(right.get("orcid_set", []))
    if left_orcid and right_orcid and not (left_orcid & right_orcid):
        conflicts = ["orcid_conflict"]
    else:
        conflicts = []
    left_rid = set(left.get("researcher_id_set", []))
    right_rid = set(right.get("researcher_id_set", []))
    if left_rid and right_rid and not (left_rid & right_rid):
        conflicts.append("researcher_id_conflict")
    return conflicts


def strong_name_score_compact(left: Dict[str, object], right: Dict[str, object]) -> Tuple[float, List[str]]:
    left_name = left.get("name_norm")
    right_name = right.get("name_norm")
    left_compact = left.get("compact_full")
    right_compact = right.get("compact_full")
    left_aliases = set(left.get("name_aliases_norm", []))
    right_aliases = set(right.get("name_aliases_norm", []))

    if left_name and left_name == right_name:
        return 1.0, ["exact_name_norm"]
    if left_compact and right_compact and left_compact == right_compact:
        return 0.99, ["exact_compact_full"]
    if left_aliases and right_aliases and left_aliases.intersection(right_aliases):
        return 0.98, ["alias_overlap"]
    if left.get("surname_norm") and left.get("surname_norm") == right.get("surname_norm"):
        if left.get("given_names_norm") and left.get("given_names_norm") == right.get("given_names_norm"):
            return 0.97, ["same_surname_given_names"]
        if left.get("initials") and left.get("initials") == right.get("initials"):
            return 0.95, ["same_surname_initials"]
        if left.get("first_initial") and left.get("first_initial") == right.get("first_initial"):
            return 0.9, ["same_surname_first_initial"]
    return 0.0, []


def publication_stability_support_compact(left: Dict[str, object], right: Dict[str, object]) -> bool:
    if left["node_type"] != "frontiers" or right["node_type"] != "frontiers":
        return False
    if left.get("compact_full") != right.get("compact_full"):
        return False
    if not (set(left.get("institution_fps", [])) & set(right.get("institution_fps", []))):
        return False
    left_publications = set(left.get("publication_ids", []))
    right_publications = set(right.get("publication_ids", []))
    return bool(left_publications and right_publications and left_publications != right_publications)


def evaluate_candidate_batch(
    batch_input_text: str,
    batch_output_text: str,
) -> Dict[str, object]:
    output_count = 0
    with open(batch_input_text, "r", encoding="utf-8") as source, open(batch_output_text, "w", encoding="utf-8") as target:
        for line in source:
            raw = line.strip()
            if not raw:
                continue
            item = json.loads(raw)
            left = MATCH_WORKER_NODES[int(item["left"])]
            right = MATCH_WORKER_NODES[int(item["right"])]
            blocking_keys = sorted(set(str(value) for value in item.get("blocking_keys", [])))

            conflicts = candidate_conflicts_compact(left, right)
            if conflicts:
                target.write(
                    json.dumps(
                        {
                            "left": item["left"],
                            "right": item["right"],
                            "decision": "blocked_conflict",
                            "score": 0.0,
                            "rules_hit": blocking_keys,
                            "blocking_key": ",".join(blocking_keys),
                            "conflict_flags": conflicts,
                        },
                        ensure_ascii=False,
                    )
                    + "\n"
                )
                output_count += 1
                continue

            name_score, name_rules = strong_name_score_compact(left, right)
            if name_score < STRONG_NAME_THRESHOLD:
                continue

            supports: List[str] = []
            if set(left.get("institution_fps", [])) & set(right.get("institution_fps", [])):
                supports.append("institution_overlap")
            if set(left.get("country_fps", [])) & set(right.get("country_fps", [])):
                supports.append("country_overlap")
            if "external_matched" in blocking_keys:
                supports.append("external_matched")
            if publication_stability_support_compact(left, right):
                supports.append("publication_stability")
            if not supports:
                continue
            decision = "merge" if len(supports) >= 2 else "manual_review"
            target.write(
                json.dumps(
                    {
                        "left": item["left"],
                        "right": item["right"],
                        "decision": decision,
                        "score": round(name_score, 4),
                        "rules_hit": list(name_rules) + supports,
                        "blocking_key": ",".join(blocking_keys),
                        "conflict_flags": [],
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
            output_count += 1
    return {
        "batch_input": batch_input_text,
        "batch_output": batch_output_text,
        "output_count": output_count,
    }


def load_rows(preferred_parquet: Path, fallback_jsonl: Path) -> tuple[List[Dict[str, object]], str]:
    if preferred_parquet.exists():
        return load_parquet_rows(preferred_parquet), "parquet"
    if fallback_jsonl.exists():
        return load_jsonl(fallback_jsonl), "jsonl"
    raise FileNotFoundError(f"missing input rows: {preferred_parquet} or {fallback_jsonl}")


def normalized_set(values: Iterable[Optional[str]]) -> Set[str]:
    output: Set[str] = set()
    for value in values:
        normalized = normalize_text(value)
        if normalized:
            output.add(normalized)
    return output


def normalized_email(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    cleaned = str(value).strip().lower()
    return cleaned or None


def institution_fingerprint(value: Optional[str]) -> Optional[str]:
    normalized = normalize_text(value)
    return normalized or None


def node_density(node: Dict[str, object]) -> int:
    return sum(1 for value in node.values() if value not in (None, [], "", set()))


def make_name_fields(name_value: Optional[str]) -> Dict[str, Optional[str]]:
    parts = parse_name_parts(name_value)
    return {
        "name_original": parts["raw"] or None,
        "name_norm": parts["name_norm"],
        "surname_norm": parts["surname_norm"],
        "given_names_norm": parts["given_names_norm"],
        "initials": parts["initials"],
        "first_initial": parts["first_initial"],
        "compact_full": parts["compact_full"],
    }


def choose_primary(counter_values: Counter, fallback: Optional[str] = None) -> Optional[str]:
    if counter_values:
        return counter_values.most_common(1)[0][0]
    return fallback


def build_exact_pair(left: str, right: str) -> Tuple[str, str]:
    return tuple(sorted((left, right)))  # type: ignore[return-value]


def build_scholar_node(person_row: Dict[str, object], matched_publications: Sequence[str]) -> Dict[str, object]:
    name_fields = make_name_fields(person_row.get("name_original"))
    affiliations = unique_preserve(
        [
            value
            for value in ([person_row.get("current_affiliation")] + list(person_row.get("affiliations", []) or []))
            if value
        ]
    )
    countries = unique_preserve(
        value
        for raw_value in ([person_row.get("country")] + list(person_row.get("countries_observed", []) or []))
        for value in split_semicolon(raw_value)
        if value
    )
    chinese_identity = classify_chinese_identity(
        parse_name_parts(person_row.get("name_original")),
        countries,
    )
    return {
        "node_id": f"legacy:{person_row['person_id']}",
        "node_type": "scholars_like",
        "legacy_person_id": person_row["person_id"],
        "source_person_id": person_row["person_id"],
        "occurrence_id": None,
        "publication_ids": set(str(v) for v in matched_publications if v),
        "matched_publications": set(str(v) for v in matched_publications if v),
        **name_fields,
        "name_aliases": set(person_row.get("name_aliases", []) or []),
        "orcid_set": set([person_row["orcid"]]) if person_row.get("orcid") else set(),
        "researcher_id_set": set(person_row.get("researcher_ids", []) or []),
        "email_set": set(normalized_email(v) for v in person_row.get("emails", []) or [] if normalized_email(v)),
        "email_score": {},
        "country_values": countries,
        "country_fps": normalized_set(countries),
        "institution_values": affiliations,
        "institution_fps": normalized_set(affiliations),
        "chinese_identity": chinese_identity,
        "qs_top200_rank": person_row.get("qs_top200_rank"),
        "world_top500_rank": person_row.get("world_top500_rank"),
        "address_values": unique_preserve(person_row.get("addresses", []) or []),
        "source_systems": set(person_row.get("source_systems", []) or []),
        "source_refs": list(person_row.get("source_refs", []) or []),
        "person_payload": person_row,
    }


def build_frontiers_node(
    occurrence_row: Dict[str, object],
    identifier_claims: Sequence[Dict[str, object]],
    affiliation_claims: Sequence[Dict[str, object]],
    email_claims: Sequence[Dict[str, object]],
) -> Dict[str, object]:
    name_fields = make_name_fields(occurrence_row.get("author_name_raw"))
    orcid_set = set()
    if occurrence_row.get("orcid"):
        orcid_set.add(str(occurrence_row["orcid"]))
    researcher_ids = set(str(v) for v in occurrence_row.get("researcher_ids", []) or [] if v)
    for claim in identifier_claims:
        claim_type = claim.get("claim_type")
        claim_value = claim.get("claim_value")
        if not claim_value:
            continue
        if claim_type == "orcid":
            orcid_set.add(str(claim_value))
        elif claim_type == "researcher_id":
            researcher_ids.add(str(claim_value))

    institution_values = unique_preserve(
        [claim.get("institution") for claim in affiliation_claims if claim.get("institution")]
    )
    address_values = unique_preserve(
        [claim.get("address_text") for claim in affiliation_claims if claim.get("address_text")]
    )
    country_values = unique_preserve(
        [claim.get("country_norm") for claim in affiliation_claims if claim.get("country_norm")]
    )
    qs_top200_rank = merge_best_rank(*(claim.get("qs_top200_rank") for claim in affiliation_claims))
    world_top500_rank = merge_best_rank(*(claim.get("world_top500_rank") for claim in email_claims))

    email_set = set()
    email_score: Dict[str, float] = {}
    for claim in email_claims:
        if str(claim.get("confidence") or "").lower() != "high":
            continue
        email = normalized_email(claim.get("email"))
        if not email:
            continue
        email_set.add(email)
        email_score[email] = float(claim.get("score") or 0.0)

    legacy_pid = person_id(name_fields["name_norm"], next(iter(sorted(orcid_set))) if orcid_set else None, None)
    return {
        "node_id": f"occurrence:{occurrence_row['occurrence_id']}",
        "node_type": "frontiers",
        "legacy_person_id": legacy_pid,
        "source_person_id": legacy_pid,
        "occurrence_id": occurrence_row["occurrence_id"],
        "publication_ids": {str(occurrence_row["publication_id"])},
        "matched_publications": set(),
        **name_fields,
        "name_aliases": set(occurrence_row.get("name_aliases", []) or []),
        "orcid_set": orcid_set,
        "researcher_id_set": researcher_ids,
        "email_set": email_set,
        "email_score": email_score,
        "country_values": country_values,
        "country_fps": normalized_set(country_values),
        "institution_values": institution_values,
        "institution_fps": normalized_set(institution_values),
        "chinese_identity": occurrence_row.get("chinese_identity") or CHINESE_IDENTITY_FOREIGN,
        "qs_top200_rank": qs_top200_rank,
        "world_top500_rank": world_top500_rank,
        "address_values": address_values,
        "source_systems": {"frontiers"},
        "source_refs": list(occurrence_row.get("source_refs", []) or []),
        "person_payload": None,
        "author_order": occurrence_row.get("author_order"),
        "is_corresponding_author": occurrence_row.get("is_corresponding_author"),
    }


def merge_chinese_identity(values: Iterable[object]) -> str:
    normalized = [str(value) for value in values if value]
    if CHINESE_IDENTITY_DOMESTIC in normalized:
        return CHINESE_IDENTITY_DOMESTIC
    if CHINESE_IDENTITY_OVERSEAS in normalized:
        return CHINESE_IDENTITY_OVERSEAS
    return CHINESE_IDENTITY_FOREIGN


def strong_name_score(left: Dict[str, object], right: Dict[str, object]) -> Tuple[float, List[str]]:
    left_name = left.get("name_norm")
    right_name = right.get("name_norm")
    left_compact = left.get("compact_full")
    right_compact = right.get("compact_full")
    left_aliases = normalized_set(left.get("name_aliases", []) or [])
    right_aliases = normalized_set(right.get("name_aliases", []) or [])

    if left_name and left_name == right_name:
        return 1.0, ["exact_name_norm"]
    if left_compact and right_compact and left_compact == right_compact:
        return 0.99, ["exact_compact_full"]
    if left_aliases and right_aliases and left_aliases.intersection(right_aliases):
        return 0.98, ["alias_overlap"]
    if left.get("surname_norm") and left.get("surname_norm") == right.get("surname_norm"):
        if left.get("given_names_norm") and left.get("given_names_norm") == right.get("given_names_norm"):
            return 0.97, ["same_surname_given_names"]
        if left.get("initials") and left.get("initials") == right.get("initials"):
            return 0.95, ["same_surname_initials"]
        if left.get("first_initial") and left.get("first_initial") == right.get("first_initial"):
            return 0.9, ["same_surname_first_initial"]
    return 0.0, []


def choose_representative_node(nodes: Sequence[Dict[str, object]]) -> Dict[str, object]:
    def score(node: Dict[str, object]) -> Tuple[int, int, int, str]:
        source_systems = set(node.get("source_systems", set()) or set())
        has_scholars = 1 if ("scholars" in source_systems or "matched" in source_systems) else 0
        return (
            has_scholars,
            node_density(node),
            len(node.get("source_refs", []) or []),
            str(node["node_id"]),
        )

    return max(nodes, key=score)


def canonical_person_identifier(
    orcid_values: Sequence[str],
    researcher_ids: Sequence[str],
    emails: Sequence[str],
    cluster_signature: str,
) -> str:
    if len(orcid_values) == 1:
        return f"orcid:{orcid_values[0]}"
    if len(researcher_ids) == 1:
        return f"rid:{researcher_ids[0]}"
    if len(emails) == 1:
        return f"email:{emails[0]}"
    return f"cluster:{sha1_text(cluster_signature)}"


def candidate_conflicts(left: Dict[str, object], right: Dict[str, object]) -> List[str]:
    conflicts: List[str] = []
    if left["orcid_set"] and right["orcid_set"] and not (left["orcid_set"] & right["orcid_set"]):
        conflicts.append("orcid_conflict")
    if left["researcher_id_set"] and right["researcher_id_set"] and not (
        left["researcher_id_set"] & right["researcher_id_set"]
    ):
        conflicts.append("researcher_id_conflict")
    return conflicts


def publication_stability_support(left: Dict[str, object], right: Dict[str, object]) -> bool:
    if left["node_type"] != "frontiers" or right["node_type"] != "frontiers":
        return False
    if left.get("compact_full") != right.get("compact_full"):
        return False
    if not (left["institution_fps"] & right["institution_fps"]):
        return False
    return bool(left["publication_ids"] and right["publication_ids"] and left["publication_ids"] != right["publication_ids"])


def append_audit(
    rows: Optional[List[Dict[str, object]]],
    left: Dict[str, object],
    right: Dict[str, object],
    decision: str,
    score: float,
    rules_hit: Sequence[str],
    blocking_key: str,
    conflict_flags: Sequence[str],
) -> None:
    if rows is None:
        return
    rows.append(
        {
            "candidate_a_id": left["node_id"],
            "candidate_b_id": right["node_id"],
            "decision": decision,
            "score": round(score, 4),
            "rules_hit": sorted(set(str(rule) for rule in rules_hit if rule)),
            "blocking_key": blocking_key,
            "conflict_flags": sorted(set(str(flag) for flag in conflict_flags if flag)),
        }
    )


def build_match_evidence_pairs(
    matched_relations: Sequence[Dict[str, object]],
    scholar_nodes_by_legacy_id: Dict[str, Dict[str, object]],
    occurrence_nodes_by_publication: Dict[str, List[Dict[str, object]]],
) -> Set[Tuple[str, str]]:
    evidence_pairs: Set[Tuple[str, str]] = set()
    for relation in matched_relations:
        scholar_node = scholar_nodes_by_legacy_id.get(str(relation["person_id"]))
        if not scholar_node:
            continue
        candidates = occurrence_nodes_by_publication.get(str(relation["publication_id"]), [])
        target_name = normalize_text(relation.get("author_name_in_paper"))
        for occurrence_node in candidates:
            if target_name and target_name != occurrence_node.get("name_norm"):
                score, _ = strong_name_score(
                    scholar_node,
                    {
                        **occurrence_node,
                        "name_norm": target_name,
                        "compact_full": normalize_compact(target_name),
                        "name_aliases": [relation.get("author_name_in_paper")],
                    },
                )
                if score < STRONG_NAME_THRESHOLD:
                    continue
            evidence_pairs.add(build_exact_pair(scholar_node["node_id"], occurrence_node["node_id"]))
    return evidence_pairs


def main() -> None:
    parser = argparse.ArgumentParser(description="Build authoritative person clusters from silver-stage occurrence and claim data.")
    parser.add_argument("--silver-dir", required=True, help="Silver output directory")
    parser.add_argument("--output-dir", required=True, help="Match output directory")
    parser.add_argument("--write-audit", action="store_true", help="Write person_match_audit.jsonl. Disabled by default.")
    parser.add_argument("--write-debug-jsonl", action="store_true", help="Write JSONL debug copies alongside parquet outputs.")
    parser.add_argument("--match-workers", default="auto", help="Worker count for candidate evaluation. Default: auto.")
    parser.add_argument("--match-batch-size", type=int, default=200000, help="Candidate pairs per batch file.")
    args = parser.parse_args()

    silver_dir = Path(args.silver_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = now_iso()
    write_audit = bool(args.write_audit)
    write_debug_jsonl = bool(args.write_debug_jsonl)
    match_workers = resolve_worker_count(args.match_workers)
    match_batch_size = max(1, int(args.match_batch_size))
    match_tmp_dir = output_dir / "_tmp"

    required_files = {
        "person_seed": (silver_dir / "person_seed.parquet", silver_dir / "person_seed.jsonl"),
        "matched_relation_seed": (silver_dir / "matched_relation_seed.parquet", silver_dir / "matched_relation_seed.jsonl"),
        "publication": (silver_dir / "publication.parquet", silver_dir / "publication.jsonl"),
        "author_occurrence": (silver_dir / "author_occurrence.parquet", silver_dir / "author_occurrence.jsonl"),
        "author_identifier_claim": (silver_dir / "author_identifier_claim.parquet", silver_dir / "author_identifier_claim.jsonl"),
        "author_affiliation_claim": (silver_dir / "author_affiliation_claim.parquet", silver_dir / "author_affiliation_claim.jsonl"),
        "author_email_claim": (silver_dir / "author_email_claim.parquet", silver_dir / "author_email_claim.jsonl"),
    }
    missing = [
        f"{parquet_path} or {jsonl_path}"
        for parquet_path, jsonl_path in required_files.values()
        if not parquet_path.exists() and not jsonl_path.exists()
    ]
    if missing:
        raise FileNotFoundError(f"match stage missing silver inputs: {missing}")

    silver_person_rows, person_seed_format = load_rows(*required_files["person_seed"])
    publication_rows, publication_format = load_rows(*required_files["publication"])
    matched_relation_rows, matched_relation_format = load_rows(*required_files["matched_relation_seed"])
    occurrence_rows, occurrence_format = load_rows(*required_files["author_occurrence"])
    identifier_claim_rows, identifier_format = load_rows(*required_files["author_identifier_claim"])
    affiliation_claim_rows, affiliation_format = load_rows(*required_files["author_affiliation_claim"])
    email_claim_rows, email_format = load_rows(*required_files["author_email_claim"])

    identifier_by_occurrence: Dict[str, List[Dict[str, object]]] = defaultdict(list)
    for row in identifier_claim_rows:
        identifier_by_occurrence[str(row["occurrence_id"])].append(row)

    affiliation_by_occurrence: Dict[str, List[Dict[str, object]]] = defaultdict(list)
    for row in affiliation_claim_rows:
        affiliation_by_occurrence[str(row["occurrence_id"])].append(row)

    email_by_occurrence: Dict[str, List[Dict[str, object]]] = defaultdict(list)
    for row in email_claim_rows:
        email_by_occurrence[str(row["occurrence_id"])].append(row)

    matched_publications_by_person: Dict[str, Set[str]] = defaultdict(set)
    for row in matched_relation_rows:
        matched_publications_by_person[str(row["person_id"])].add(str(row["publication_id"]))

    nodes: List[Dict[str, object]] = []
    scholar_nodes_by_legacy_id: Dict[str, Dict[str, object]] = {}
    for person_row in silver_person_rows:
        source_systems = set(person_row.get("source_systems", []) or [])
        if "scholars" not in source_systems and "matched" not in source_systems:
            continue
        node = build_scholar_node(
            person_row,
            sorted(matched_publications_by_person.get(str(person_row["person_id"]), set())),
        )
        nodes.append(node)
        scholar_nodes_by_legacy_id[str(person_row["person_id"])] = node

    occurrence_nodes_by_publication: Dict[str, List[Dict[str, object]]] = defaultdict(list)
    for occurrence_row in occurrence_rows:
        occurrence_id = str(occurrence_row["occurrence_id"])
        node = build_frontiers_node(
            occurrence_row,
            identifier_by_occurrence.get(occurrence_id, []),
            affiliation_by_occurrence.get(occurrence_id, []),
            email_by_occurrence.get(occurrence_id, []),
        )
        nodes.append(node)
        occurrence_nodes_by_publication[str(occurrence_row["publication_id"])].append(node)

    node_by_id = {node["node_id"]: node for node in nodes}
    union_find = UnionFind(node_by_id.keys())
    audit_rows: Optional[List[Dict[str, object]]] = [] if write_audit else None
    audit_row_count = [0]
    merge_events: List[Dict[str, object]] = []
    merge_counts = Counter()
    skipped_large_blocks = Counter()

    def union_pair(
        left_id: str,
        right_id: str,
        method: str,
        score: float,
        blocking_key: str,
        rules_hit: Sequence[str],
    ) -> None:
        left = node_by_id[left_id]
        right = node_by_id[right_id]
        if union_find.find(left_id) == union_find.find(right_id):
            return
        union_find.union(left_id, right_id)
        merge_counts[method] += 1
        merge_events.append(
            {
                "left_id": left_id,
                "right_id": right_id,
                "method": method,
                "rules_hit": sorted(set(rules_hit)),
            }
        )
        audit_row_count[0] += 1
        append_audit(audit_rows, left, right, "merge", score, rules_hit, blocking_key, [])

    strong_groups = [
        ("P0_orcid", defaultdict(list)),
        ("P1_researcher_id", defaultdict(list)),
        ("P2_email", defaultdict(list)),
    ]
    for node in nodes:
        for orcid in sorted(node["orcid_set"]):
            strong_groups[0][1][orcid].append(node["node_id"])
        for researcher_id in sorted(node["researcher_id_set"]):
            strong_groups[1][1][researcher_id].append(node["node_id"])
        for email in sorted(node["email_set"]):
            strong_groups[2][1][email].append(node["node_id"])

    for method, grouped in strong_groups:
        for value, member_ids in grouped.items():
            unique_ids = sorted(set(member_ids))
            if len(unique_ids) <= 1:
                continue
            anchor = unique_ids[0]
            for other_id in unique_ids[1:]:
                union_pair(anchor, other_id, method, 1.0, f"{method}:{value}", [method])

    matched_evidence_pairs = build_match_evidence_pairs(
        matched_relation_rows,
        scholar_nodes_by_legacy_id,
        occurrence_nodes_by_publication,
    )

    candidate_pairs: Dict[Tuple[str, str], Set[str]] = defaultdict(set)
    for left_id, right_id in matched_evidence_pairs:
        candidate_pairs[(left_id, right_id)].add("external_matched")

    fuzzy_blocks: Dict[str, List[str]] = defaultdict(list)
    for node in nodes:
        compact_full = node.get("compact_full")
        if compact_full:
            fuzzy_blocks[f"compact:{compact_full}"].append(node["node_id"])
        signature = None
        if node.get("surname_norm") and node.get("first_initial"):
            signature = f"{node['surname_norm']}|{node['first_initial']}"
        if not signature:
            continue
        for institution_fp in sorted(node["institution_fps"]):
            fuzzy_blocks[f"siginst:{signature}|{institution_fp}"].append(node["node_id"])
        for country_fp in sorted(node["country_fps"]):
            fuzzy_blocks[f"sigcountry:{signature}|{country_fp}"].append(node["node_id"])

    for blocking_key, grouped_ids in fuzzy_blocks.items():
        unique_ids = sorted(set(grouped_ids))
        if len(unique_ids) <= 1:
            continue
        if len(unique_ids) > MAX_FUZZY_BLOCK_SIZE:
            skipped_large_blocks[blocking_key.split(":", 1)[0]] += 1
            continue
        for left_id, right_id in itertools.combinations(unique_ids, 2):
            candidate_pairs[build_exact_pair(left_id, right_id)].add(blocking_key)

    manual_review_count = 0
    conflict_counts = Counter()
    batch_build_start = perf_counter()
    remove_tree(match_tmp_dir)
    candidate_batch_dir = match_tmp_dir / "candidate_batches"
    candidate_batch_dir.mkdir(parents=True, exist_ok=True)
    node_snapshot_path = match_tmp_dir / "nodes_snapshot.json"
    with open(node_snapshot_path, "w", encoding="utf-8") as handle:
        json.dump([serialize_worker_node(node) for node in nodes], handle, ensure_ascii=False)
    node_index_by_id = {str(node["node_id"]): idx for idx, node in enumerate(nodes)}

    candidate_batch_paths: List[Path] = []
    batch_handle = None
    batch_record_count = 0
    batch_index = 0
    sorted_candidate_items = sorted(candidate_pairs.items())
    try:
        for (left_id, right_id), blocking_keys in sorted_candidate_items:
            if batch_handle is None or batch_record_count >= match_batch_size:
                if batch_handle is not None:
                    batch_handle.close()
                batch_path = candidate_batch_dir / f"batch_{batch_index:05d}.jsonl"
                candidate_batch_paths.append(batch_path)
                batch_handle = open(batch_path, "w", encoding="utf-8")
                batch_record_count = 0
                batch_index += 1
            batch_handle.write(
                json.dumps(
                    {
                        "left": node_index_by_id[left_id],
                        "right": node_index_by_id[right_id],
                        "blocking_keys": sorted(blocking_keys),
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
            batch_record_count += 1
    finally:
        if batch_handle is not None:
            batch_handle.close()
    batch_build_seconds = round(perf_counter() - batch_build_start, 3)

    worker_eval_start = perf_counter()
    batch_result_paths: List[Path] = []
    with ProcessPoolExecutor(
        max_workers=match_workers,
        initializer=init_match_worker,
        initargs=(str(node_snapshot_path.resolve()),),
    ) as executor:
        future_map = {}
        for idx, batch_path in enumerate(candidate_batch_paths):
            batch_output_path = candidate_batch_dir / f"batch_{idx:05d}.result.jsonl"
            batch_result_paths.append(batch_output_path)
            future = executor.submit(
                evaluate_candidate_batch,
                str(batch_path.resolve()),
                str(batch_output_path.resolve()),
            )
            future_map[future] = batch_output_path
        for future in as_completed(future_map):
            future.result()
    worker_eval_seconds = round(perf_counter() - worker_eval_start, 3)

    replay_start = perf_counter()
    for batch_result_path in batch_result_paths:
        with open(batch_result_path, "r", encoding="utf-8") as handle:
            for line in handle:
                raw = line.strip()
                if not raw:
                    continue
                item = json.loads(raw)
                left_id = nodes[int(item["left"])]["node_id"]
                right_id = nodes[int(item["right"])]["node_id"]
                left = node_by_id[left_id]
                right = node_by_id[right_id]
                if union_find.find(left_id) == union_find.find(right_id):
                    continue
                decision = str(item["decision"])
                rules_hit = list(item.get("rules_hit", []))
                blocking_key = str(item.get("blocking_key") or "")
                score = float(item.get("score") or 0.0)
                conflict_flags = list(item.get("conflict_flags", []))
                if decision == "blocked_conflict":
                    for flag in conflict_flags:
                        conflict_counts[str(flag)] += 1
                    audit_row_count[0] += 1
                    append_audit(audit_rows, left, right, decision, score, rules_hit, blocking_key, conflict_flags)
                    continue
                if decision == "merge":
                    union_pair(left_id, right_id, "P3_fuzzy", score, blocking_key, rules_hit)
                    continue
                if decision == "manual_review":
                    manual_review_count += 1
                    audit_row_count[0] += 1
                    append_audit(audit_rows, left, right, decision, score, rules_hit, blocking_key, [])
    replay_seconds = round(perf_counter() - replay_start, 3)

    clusters: Dict[str, List[Dict[str, object]]] = defaultdict(list)
    for node in nodes:
        clusters[union_find.find(node["node_id"])].append(node)

    cluster_methods: Dict[str, Set[str]] = defaultdict(set)
    for event in merge_events:
        root = union_find.find(str(event["left_id"]))
        cluster_methods[root].add(str(event["method"]))

    quality = init_quality()
    person_rows: List[Dict[str, object]] = []
    occurrence_to_canonical: Dict[str, str] = {}
    legacy_to_canonical: Dict[str, str] = {}

    for root, members in clusters.items():
        representative = choose_representative_node(members)
        source_systems = sorted(set().union(*(member["source_systems"] for member in members)))
        name_aliases = sorted(
            set(alias for member in members for alias in member.get("name_aliases", set()) if alias)
        )
        orcid_values = sorted(set(value for member in members for value in member["orcid_set"]))
        researcher_ids = sorted(set(value for member in members for value in member["researcher_id_set"]))
        emails = sorted(set(value for member in members for value in member["email_set"]))
        institutions = unique_preserve(
            value for member in members for value in member.get("institution_values", []) if value
        )
        addresses = unique_preserve(
            value for member in members for value in member.get("address_values", []) if value
        )
        countries_observed = unique_preserve(
            value for member in members for value in member.get("country_values", []) if value
        )
        qs_top200_rank = merge_best_rank(*(member.get("qs_top200_rank") for member in members))
        world_top500_rank = merge_best_rank(*(member.get("world_top500_rank") for member in members))
        chinese_identity = merge_chinese_identity(member.get("chinese_identity") for member in members)
        country_counter = Counter(value for value in countries_observed if value)
        institution_counter = Counter(value for value in institutions if value)
        email_counter = Counter(value for value in emails if value)
        canonical_id = canonical_person_identifier(
            orcid_values,
            researcher_ids,
            emails,
            "|".join(sorted(member["node_id"] for member in members)),
        )
        legacy_id = str(representative["legacy_person_id"])
        primary_email = choose_primary(email_counter)
        scholars_payloads = [member["person_payload"] for member in members if member.get("person_payload")]
        primary_payload = scholars_payloads[0] if scholars_payloads else None
        match_methods = sorted(cluster_methods.get(root, set()))

        person_record = {
            "person_id": canonical_id,
            "canonical_person_id": canonical_id,
            "legacy_person_id": legacy_id,
            "name_original": representative.get("name_original"),
            "name_norm": normalize_text(representative.get("name_original")) or representative.get("name_norm"),
            "name_aliases": name_aliases,
            "matched_by_orcid": "P0_orcid" in match_methods,
            "orcid": orcid_values[0] if len(orcid_values) == 1 else None,
            "researcher_ids": researcher_ids,
            "country": choose_primary(
                country_counter,
                primary_payload.get("country") if primary_payload and primary_payload.get("country") else None,
            ),
            "countries_observed": countries_observed,
            "current_affiliation": (
                primary_payload.get("current_affiliation")
                if primary_payload and primary_payload.get("current_affiliation")
                else choose_primary(institution_counter)
            ),
            "chinese_identity": chinese_identity,
            "qs_top200_rank": qs_top200_rank,
            "world_top500_rank": world_top500_rank,
            "affiliations": institutions,
            "addresses": addresses,
            "primary_email": primary_email,
            "emails": emails,
            "subjects": primary_payload.get("subjects", []) if primary_payload else [],
            "keywords": primary_payload.get("keywords", []) if primary_payload else [],
            "interests": primary_payload.get("interests", []) if primary_payload else [],
            "h_index": primary_payload.get("h_index") if primary_payload else None,
            "i10_index": primary_payload.get("i10_index") if primary_payload else None,
            "total_citations": primary_payload.get("total_citations") if primary_payload else None,
            "average_citations": primary_payload.get("average_citations") if primary_payload else None,
            "articles": primary_payload.get("articles") if primary_payload else None,
            "google_scholar_url": primary_payload.get("google_scholar_url") if primary_payload else None,
            "sci_profile_url": primary_payload.get("sci_profile_url") if primary_payload else None,
            "match_methods": match_methods,
            "cluster_size": len(members),
            "source_systems": source_systems,
            "source_refs": [ref for member in members for ref in member.get("source_refs", [])],
            "created_at": ts,
            "updated_at": ts,
        }
        person_rows.append(person_record)
        quality_track(quality, "person", person_record)

        for member in members:
            legacy_to_canonical[str(member["legacy_person_id"])] = canonical_id
            if member.get("occurrence_id"):
                occurrence_to_canonical[str(member["occurrence_id"])] = canonical_id

    relation_rows_by_id: Dict[str, Dict[str, object]] = {}
    relation_conflicts = 0

    def add_relation(record: Dict[str, object]) -> None:
        nonlocal relation_conflicts
        rid = str(record["relation_id"])
        if rid in relation_rows_by_id:
            relation_conflicts += 1
            return
        relation_rows_by_id[rid] = record
        quality_track(quality, "person_publication", record)

    for occurrence_row in occurrence_rows:
        canonical_id = occurrence_to_canonical.get(str(occurrence_row["occurrence_id"]))
        if not canonical_id:
            continue
        add_relation(
            {
                "relation_id": relation_id(canonical_id, str(occurrence_row["publication_id"]), "author"),
                "person_id": canonical_id,
                "publication_id": str(occurrence_row["publication_id"]),
                "relation_type": "author",
                "author_order": occurrence_row.get("author_order"),
                "author_name_in_paper": occurrence_row.get("author_name_raw"),
                "match_score": None,
                "match_confidence": None,
                "match_rules_hit": [],
                "is_corresponding_author": occurrence_row.get("is_corresponding_author"),
                "source_refs": list(occurrence_row.get("source_refs", []) or []),
                "created_at": ts,
                "updated_at": ts,
            }
        )

    for relation_row in matched_relation_rows:
        canonical_id = legacy_to_canonical.get(str(relation_row["person_id"]))
        if not canonical_id:
            continue
        record = dict(relation_row)
        record["person_id"] = canonical_id
        record["relation_id"] = relation_id(canonical_id, str(relation_row["publication_id"]), "matched")
        record["created_at"] = ts
        record["updated_at"] = ts
        add_relation(record)

    quality["person_publication"]["pk_conflicts"] = relation_conflicts
    person_publication_rows = list(relation_rows_by_id.values())
    ranking_hits = {
        "person_qs_hits": sum(1 for row in person_rows if row.get("qs_top200_rank") is not None),
        "person_world_top500_hits": sum(1 for row in person_rows if row.get("world_top500_rank") is not None),
    }
    person_chinese_identity_counts = chinese_identity_counts(person_rows)

    parquet_outputs = {
        "person": output_dir / "person.parquet",
        "person_publication": output_dir / "person_publication.parquet",
    }
    jsonl_outputs = {
        "person": output_dir / "person.jsonl" if write_debug_jsonl else None,
        "person_publication": output_dir / "person_publication.jsonl" if write_debug_jsonl else None,
    }
    write_timings = {}

    start = perf_counter()
    write_pylist_parquet(parquet_outputs["person"], person_rows)
    write_timings["person_parquet_seconds"] = round(perf_counter() - start, 3)
    start = perf_counter()
    write_pylist_parquet(parquet_outputs["person_publication"], person_publication_rows)
    write_timings["person_publication_parquet_seconds"] = round(perf_counter() - start, 3)
    if write_debug_jsonl:
        start = perf_counter()
        write_jsonl(jsonl_outputs["person"], person_rows)
        write_timings["person_jsonl_seconds"] = round(perf_counter() - start, 3)
        start = perf_counter()
        write_jsonl(jsonl_outputs["person_publication"], person_publication_rows)
        write_timings["person_publication_jsonl_seconds"] = round(perf_counter() - start, 3)
    if write_audit and audit_rows is not None:
        write_jsonl(output_dir / "person_match_audit.jsonl", audit_rows)

    id_safety_stats = {
        "person_v1": {"total_docs": 0, "hashed_id_count": 0},
        "person_publication_v1": {"total_docs": 0, "hashed_id_count": 0},
    }
    write_bulk(output_dir / "person.bulk.ndjson", "person_v1", "person_id", person_rows, id_safety_stats)
    write_bulk(
        output_dir / "person_publication.bulk.ndjson",
        "person_publication_v1",
        "relation_id",
        person_publication_rows,
        id_safety_stats,
    )

    if match_tmp_dir.exists():
        remove_tree(match_tmp_dir)

    match_summary = {
        "generated_at": ts,
        "silver_dir": str(silver_dir.resolve()),
        "output_dir": str(output_dir.resolve()),
        "input_formats": {
            "person_seed": person_seed_format,
            "publication": publication_format,
            "matched_relation_seed": matched_relation_format,
            "author_occurrence": occurrence_format,
            "author_identifier_claim": identifier_format,
            "author_affiliation_claim": affiliation_format,
            "author_email_claim": email_format,
        },
        "input_counts": {
            "person_seed": len(silver_person_rows),
            "publication": len(publication_rows),
            "author_occurrence": len(occurrence_rows),
            "author_identifier_claim": len(identifier_claim_rows),
            "author_affiliation_claim": len(affiliation_claim_rows),
            "author_email_claim": len(email_claim_rows),
            "matched_relations": len(matched_relation_rows),
        },
        "output_counts": {
            "person": len(person_rows),
            "person_publication": len(person_publication_rows),
            "person_match_audit": len(audit_rows) if audit_rows is not None else 0,
        },
        "ranking_hits": ranking_hits,
        "person_chinese_identity_counts": person_chinese_identity_counts,
        "cluster_stats": {
            "cluster_total": len(clusters),
            "auto_merged_clusters": sum(1 for members in clusters.values() if len(members) > 1),
            "merge_counts": dict(merge_counts),
            "manual_review_count": manual_review_count,
            "skipped_large_blocks": dict(skipped_large_blocks),
        },
        "audit": {
            "enabled": write_audit,
            "rows_generated": audit_row_count[0],
            "rows_written": len(audit_rows) if audit_rows is not None else 0,
        },
        "parallelism": {
            "match_workers": match_workers,
            "match_batch_size": match_batch_size,
            "tmp_dir": str(match_tmp_dir.resolve()),
            "candidate_batch_count": len(candidate_batch_paths),
            "batch_build_seconds": batch_build_seconds,
            "worker_seconds": worker_eval_seconds,
            "merge_seconds": replay_seconds,
        },
        "outputs": {
            "person_parquet": str(parquet_outputs["person"]),
            "person_publication_parquet": str(parquet_outputs["person_publication"]),
            "person_jsonl": str(jsonl_outputs["person"]) if jsonl_outputs["person"] else None,
            "person_publication_jsonl": str(jsonl_outputs["person_publication"]) if jsonl_outputs["person_publication"] else None,
            "person_bulk": str(output_dir / "person.bulk.ndjson"),
            "person_publication_bulk": str(output_dir / "person_publication.bulk.ndjson"),
            "person_match_audit_jsonl": str(output_dir / "person_match_audit.jsonl") if write_audit else None,
            "tmp_dir": str(match_tmp_dir.resolve()),
        },
        "file_sizes": {
            "person_parquet_bytes": file_size_or_none(parquet_outputs["person"]),
            "person_publication_parquet_bytes": file_size_or_none(parquet_outputs["person_publication"]),
            "person_jsonl_bytes": file_size_or_none(jsonl_outputs["person"]),
            "person_publication_jsonl_bytes": file_size_or_none(jsonl_outputs["person_publication"]),
            "person_bulk_bytes": file_size_or_none(output_dir / "person.bulk.ndjson"),
            "person_publication_bulk_bytes": file_size_or_none(output_dir / "person_publication.bulk.ndjson"),
            "person_match_audit_jsonl_bytes": file_size_or_none(output_dir / "person_match_audit.jsonl"),
        },
        "write_timings": write_timings,
    }
    with open(output_dir / "match_summary.json", "w", encoding="utf-8") as handle:
        json.dump(match_summary, handle, ensure_ascii=False, indent=2)

    match_quality_report = {
        "generated_at": ts,
        "merge_stats": {
            "merge_counts": dict(merge_counts),
            "manual_review_count": manual_review_count,
            "conflict_counts": dict(conflict_counts),
            "relation_conflicts": relation_conflicts,
        },
        "entity_quality": finalize_quality(quality),
        "id_safety_stats": id_safety_stats,
    }
    with open(output_dir / "match_quality_report.json", "w", encoding="utf-8") as handle:
        json.dump(match_quality_report, handle, ensure_ascii=False, indent=2)

    print(
        json.dumps(
            {
                "person": len(person_rows),
                "person_publication": len(person_publication_rows),
                "clusters": len(clusters),
                "manual_review": manual_review_count,
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
