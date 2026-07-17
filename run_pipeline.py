import argparse
import json
import os
import sqlite3
import subprocess
import traceback
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from excel_to_json_batch import convert_files_to_jsonl, convert_files_to_parquet
from pipeline_storage import write_bulk_from_parquet


STAGE_ORDER = ["scan", "bronze", "silver", "vector", "match", "index"]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def resolve_auto_workers(requested: object, max_items: Optional[int] = None) -> int:
    text = str(requested).strip().lower()
    if text == "auto":
        cpu = os.cpu_count() or 1
        workers = max(1, min(8, cpu - 1))
    else:
        workers = max(1, int(text))
    if max_items is not None:
        workers = min(workers, max(1, max_items))
    return workers


def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def set_job_running(conn: sqlite3.Connection, job_id: int) -> None:
    conn.execute(
        """
        UPDATE ingest_jobs
        SET status = 'running', started_at = ?, updated_at = ?, error_message = NULL
        WHERE job_id = ?
        """,
        (now_iso(), now_iso(), job_id),
    )


def set_job_done(conn: sqlite3.Connection, job_id: int, success: bool, error_message: Optional[str] = None) -> None:
    status = "success" if success else "failed"
    conn.execute(
        """
        UPDATE ingest_jobs
        SET status = ?, ended_at = ?, updated_at = ?, error_message = ?
        WHERE job_id = ?
        """,
        (status, now_iso(), now_iso(), error_message, job_id),
    )


def fetch_pending_jobs(
    conn: sqlite3.Connection,
    stage: str,
    limit: int,
    source_group: Optional[str],
) -> List[sqlite3.Row]:
    params: List[object] = [stage]
    where = ["j.status = 'pending'", "j.stage = ?"]
    if source_group:
        where.append("m.source_group = ?")
        params.append(source_group)
    sql = f"""
        SELECT j.*, m.source_group
        FROM ingest_jobs j
        LEFT JOIN file_manifest m ON m.file_path = j.file_path
        WHERE {' AND '.join(where)}
        ORDER BY j.job_id
        LIMIT ?
    """
    params.append(limit)
    return conn.execute(sql, params).fetchall()


def stage_scan(conn: sqlite3.Connection, jobs: Sequence[sqlite3.Row]) -> Dict[str, int]:
    done = 0
    for job in jobs:
        jid = int(job["job_id"])
        set_job_running(conn, jid)
        set_job_done(conn, jid, True, None)
        done += 1
    conn.commit()
    return {"processed": done}


def bronze_group_output_path(output_root: Path, source_group: str, storage_format: str) -> Path:
    extension = ".rows.parquet" if storage_format == "parquet" else ".rows.jsonl"
    return output_root / "bronze" / f"{source_group}{extension}"


def fetch_active_source_files(conn: sqlite3.Connection, source_group: str) -> List[Path]:
    rows = conn.execute(
        """
        SELECT file_path
        FROM file_manifest
        WHERE source_group = ? AND status != 'deleted'
        ORDER BY file_path
        """,
        (source_group,),
    ).fetchall()
    return [Path(str(row["file_path"])) for row in rows]


def stage_bronze(
    conn: sqlite3.Connection,
    jobs: Sequence[sqlite3.Row],
    output_root: Path,
    storage_format: str,
    write_debug_jsonl: bool,
    bronze_workers: object,
) -> Dict[str, object]:
    if not jobs:
        return {"processed": 0, "success": 0, "failed": 0}

    ok = 0
    fail = 0
    groups: Dict[str, List[sqlite3.Row]] = defaultdict(list)
    for job in jobs:
        groups[job["source_group"] or "unknown"].append(job)

    group_results: Dict[str, object] = {}
    for source_group, group_jobs in groups.items():
        for job in group_jobs:
            set_job_running(conn, int(job["job_id"]))
        conn.commit()

        active_files = fetch_active_source_files(conn, source_group)
        out = bronze_group_output_path(output_root, source_group, storage_format)
        effective_workers = resolve_auto_workers(bronze_workers, len(active_files)) if active_files else 1
        tmp_dir = output_root / "bronze" / "_tmp" / source_group
        try:
            if storage_format == "parquet":
                convert_summary = convert_files_to_parquet(
                    active_files,
                    out,
                    source_group,
                    write_debug_jsonl=write_debug_jsonl,
                    workers=effective_workers,
                    tmp_dir=tmp_dir,
                )
            else:
                convert_summary = convert_files_to_jsonl(
                    active_files,
                    out,
                    source_group,
                    workers=effective_workers,
                    tmp_dir=tmp_dir,
                )
            if int(convert_summary["error_count"]) > 0:
                failed_inputs = [item["input_file"] for item in convert_summary["errors"]]
                raise RuntimeError(f"bronze rebuild failed for {source_group}: {failed_inputs}")

            for job in group_jobs:
                set_job_done(conn, int(job["job_id"]), True, None)
                ok += 1
            group_results[source_group] = {
                "job_count": len(group_jobs),
                "output_file": convert_summary["output_file"],
                "storage_format": storage_format,
                "debug_jsonl": convert_summary.get("debug_jsonl"),
                "tmp_dir": convert_summary.get("tmp_dir"),
                "workers": convert_summary.get("workers"),
                "source_file_count": convert_summary["converted_count"],
                "row_total": convert_summary["row_total"],
            }
        except Exception as exc:
            error_message = f"{exc}\n{traceback.format_exc()}"
            for job in group_jobs:
                set_job_done(conn, int(job["job_id"]), False, error_message)
                fail += 1
            group_results[source_group] = {
                "job_count": len(group_jobs),
                "error": str(exc),
            }
    conn.commit()
    return {"processed": len(jobs), "success": ok, "failed": fail, "groups": group_results}


def run_etl(
    base_dir: Path,
    scholars_json: Optional[Path],
    frontiers_json: Optional[Path],
    scholars_jsonl: Optional[Path],
    frontiers_jsonl: Optional[Path],
    scholars_parquet: Optional[Path],
    frontiers_parquet: Optional[Path],
    output_dir: Path,
    matched_json: Optional[Path],
    qs_top200_json: Optional[Path],
    world_top500_json: Optional[Path],
    write_compat_person: bool,
    write_debug_jsonl: bool,
    silver_workers: object,
    silver_shards: object,
) -> None:
    cmd = [
        "python",
        str(base_dir / "etl_transform_schema_v1.py"),
        "--output-dir",
        str(output_dir),
    ]
    if scholars_json and frontiers_json:
        cmd += ["--scholars-json", str(scholars_json), "--frontiers-json", str(frontiers_json)]
    elif scholars_parquet and frontiers_parquet:
        cmd += ["--scholars-parquet", str(scholars_parquet), "--frontiers-parquet", str(frontiers_parquet)]
    elif scholars_jsonl and frontiers_jsonl:
        cmd += ["--scholars-jsonl", str(scholars_jsonl), "--frontiers-jsonl", str(frontiers_jsonl)]
    else:
        raise ValueError("run_etl requires either json, jsonl, or parquet inputs")
    if matched_json and matched_json.exists():
        cmd += ["--matched-json", str(matched_json)]
    if qs_top200_json:
        cmd += ["--qs-top200-json", str(qs_top200_json)]
    if world_top500_json:
        cmd += ["--world-top500-json", str(world_top500_json)]
    if write_compat_person:
        cmd.append("--write-compat-person")
    if write_debug_jsonl:
        cmd.append("--write-debug-jsonl")
    cmd += ["--silver-workers", str(silver_workers), "--silver-shards", str(silver_shards)]
    subprocess.run(cmd, check=True)


def stage_silver(
    conn: sqlite3.Connection,
    jobs: Sequence[sqlite3.Row],
    base_dir: Path,
    output_root: Path,
    matched_json: Optional[Path],
    qs_top200_json: Optional[Path],
    world_top500_json: Optional[Path],
    write_compat_person: bool,
    storage_format: str,
    write_debug_jsonl: bool,
    silver_workers: object,
    silver_shards: object,
) -> Dict[str, object]:
    if not jobs:
        return {"processed": 0, "note": "no pending silver jobs"}

    bronze_input = {
        "scholars": bronze_group_output_path(output_root, "scholars", storage_format),
        "frontiers": bronze_group_output_path(output_root, "frontiers", storage_format),
    }
    if not bronze_input["scholars"].exists() or not bronze_input["frontiers"].exists():
        return {"processed": 0, "note": "bronze inputs missing"}

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    silver_out = output_root / "silver" / run_id

    for job in jobs:
        set_job_running(conn, int(job["job_id"]))
    conn.commit()

    try:
        run_etl(
            base_dir=base_dir,
            scholars_json=None,
            frontiers_json=None,
            scholars_jsonl=bronze_input["scholars"] if storage_format == "jsonl" else None,
            frontiers_jsonl=bronze_input["frontiers"] if storage_format == "jsonl" else None,
            scholars_parquet=bronze_input["scholars"] if storage_format == "parquet" else None,
            frontiers_parquet=bronze_input["frontiers"] if storage_format == "parquet" else None,
            output_dir=silver_out,
            matched_json=matched_json,
            qs_top200_json=qs_top200_json,
            world_top500_json=world_top500_json,
            write_compat_person=write_compat_person,
            write_debug_jsonl=write_debug_jsonl,
            silver_workers=silver_workers,
            silver_shards=silver_shards,
        )
        for job in jobs:
            set_job_done(conn, int(job["job_id"]), True, None)
        conn.commit()
        return {
            "processed": len(jobs),
            "silver_output": str(silver_out.resolve()),
            "storage_format": storage_format,
            "silver_workers": str(silver_workers),
            "silver_shards": str(silver_shards),
            "tmp_dir": str((silver_out / "_tmp").resolve()),
            "bronze_inputs": [str(bronze_input["scholars"].resolve()), str(bronze_input["frontiers"].resolve())],
        }
    except Exception as exc:
        err = f"{exc}\n{traceback.format_exc()}"
        for job in jobs:
            set_job_done(conn, int(job["job_id"]), False, err)
        conn.commit()
        return {"processed": len(jobs), "error": str(exc)}


def stage_vector(
    conn: sqlite3.Connection,
    jobs: Sequence[sqlite3.Row],
    base_dir: Path,
    output_root: Path,
    force_reembed: bool,
    strict_vector: bool,
    write_debug_jsonl: bool,
    shard_size: int = 1000,
) -> Dict[str, object]:
    if not jobs:
        return {"processed": 0}

    silver_dir = latest_silver_dir(output_root)
    if not silver_dir:
        return {"processed": 0, "note": "vector stage skipped: no silver output found"}

    vector_dir = output_root / "vector" / silver_dir.name
    for job in jobs:
        set_job_running(conn, int(job["job_id"]))
    conn.commit()

    cmd = [
        "python",
        str(base_dir / "vector_embed_publications_v1.py"),
        "--silver-dir",
        str(silver_dir),
        "--output-dir",
        str(vector_dir),
        "--env-file",
        str(base_dir / ".env"),
        "--shard-size",
        str(shard_size),
    ]
    if force_reembed:
        cmd.append("--force-reembed")
    if strict_vector:
        cmd.append("--strict-vector")
    if write_debug_jsonl:
        cmd.append("--write-debug-jsonl")

    try:
        subprocess.run(cmd, check=True)
        for job in jobs:
            set_job_done(conn, int(job["job_id"]), True, None)
        conn.commit()
        return {
            "processed": len(jobs),
            "silver_source": str(silver_dir.resolve()),
            "vector_output": str(vector_dir.resolve()),
            "force_reembed": force_reembed,
            "strict_vector": strict_vector,
            "write_debug_jsonl": write_debug_jsonl,
            "shard_size": shard_size,
        }
    except Exception as exc:
        err = f"{exc}\n{traceback.format_exc()}"
        for job in jobs:
            set_job_done(conn, int(job["job_id"]), False, err)
        conn.commit()
        return {"processed": len(jobs), "error": str(exc)}


def stage_match(
    conn: sqlite3.Connection,
    jobs: Sequence[sqlite3.Row],
    base_dir: Path,
    output_root: Path,
    write_match_audit: bool,
    write_debug_jsonl: bool,
    match_workers: object,
    match_batch_size: int,
) -> Dict[str, object]:
    if not jobs:
        return {"processed": 0}

    silver_dir = latest_silver_dir(output_root)
    if not silver_dir:
        return {"processed": 0, "note": "match stage skipped: no silver output found"}

    match_dir = output_root / "match" / silver_dir.name
    for job in jobs:
        set_job_running(conn, int(job["job_id"]))
    conn.commit()

    cmd = [
        "python",
        str(base_dir / "match_person_clusters_v1.py"),
        "--silver-dir",
        str(silver_dir),
        "--output-dir",
        str(match_dir),
    ]
    if write_match_audit:
        cmd.append("--write-audit")
    if write_debug_jsonl:
        cmd.append("--write-debug-jsonl")
    cmd += ["--match-workers", str(match_workers), "--match-batch-size", str(match_batch_size)]
    try:
        subprocess.run(cmd, check=True)
        for job in jobs:
            set_job_done(conn, int(job["job_id"]), True, None)
        conn.commit()
        return {
            "processed": len(jobs),
            "silver_source": str(silver_dir.resolve()),
            "match_output": str(match_dir.resolve()),
            "write_debug_jsonl": write_debug_jsonl,
            "match_workers": str(match_workers),
            "match_batch_size": match_batch_size,
            "tmp_dir": str((match_dir / "_tmp").resolve()),
        }
    except Exception as exc:
        err = f"{exc}\n{traceback.format_exc()}"
        for job in jobs:
            set_job_done(conn, int(job["job_id"]), False, err)
        conn.commit()
        return {"processed": len(jobs), "error": str(exc)}


def latest_silver_dir(output_root: Path) -> Optional[Path]:
    silver_root = output_root / "silver"
    if not silver_root.exists():
        return None
    dirs = sorted([p for p in silver_root.iterdir() if p.is_dir()])
    return dirs[-1] if dirs else None


def latest_vector_dir(output_root: Path) -> Optional[Path]:
    vector_root = output_root / "vector"
    if not vector_root.exists():
        return None
    dirs = sorted([p for p in vector_root.iterdir() if p.is_dir()])
    return dirs[-1] if dirs else None


def latest_match_dir(output_root: Path) -> Optional[Path]:
    match_root = output_root / "match"
    if not match_root.exists():
        return None
    dirs = sorted([p for p in match_root.iterdir() if p.is_dir()])
    return dirs[-1] if dirs else None


def stage_index(
    conn: sqlite3.Connection,
    jobs: Sequence[sqlite3.Row],
    base_dir: Path,
    output_root: Path,
    endpoint: Optional[str],
    username: Optional[str],
    password: Optional[str],
    insecure: bool,
) -> Dict[str, object]:
    if not jobs:
        return {"processed": 0}
    if not endpoint:
        return {"processed": 0, "note": "index stage skipped: --opensearch-endpoint not provided"}

    match_dir = latest_match_dir(output_root)
    silver_dir = latest_silver_dir(output_root)
    vector_dir = latest_vector_dir(output_root)
    if not silver_dir and not match_dir and not vector_dir:
        return {"processed": 0, "note": "index stage skipped: no silver, vector, or match output found"}

    publication_dir: Optional[Path] = None
    if vector_dir and (
        (vector_dir / "publication.parquet").exists()
        or (vector_dir / "publication.bulk.ndjson").exists()
    ):
        publication_dir = vector_dir
    elif silver_dir and (
        (silver_dir / "publication.parquet").exists()
        or (silver_dir / "publication.bulk.ndjson").exists()
    ):
        publication_dir = silver_dir

    for job in jobs:
        set_job_running(conn, int(job["job_id"]))
    conn.commit()

    create_cmd = [
        "powershell",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(base_dir / "create_indices.ps1"),
        "-Endpoint",
        endpoint,
        "-MappingFile",
        str(base_dir / "opensearch_mapping_v1.json"),
    ]
    if username and password:
        create_cmd += ["-Username", username, "-Password", password]
    if insecure:
        create_cmd += ["-Insecure"]

    try:
        bulk_specs = {
            "publication": ("publication_v1", "publication_id"),
            "author_occurrence": ("author_occurrence_v1", "occurrence_id"),
            "author_identifier_claim": ("author_identifier_claim_v1", "claim_id"),
            "author_affiliation_claim": ("author_affiliation_claim_v1", "claim_id"),
            "author_email_claim": ("author_email_claim_v1", "claim_id"),
            "person": ("person_v1", "person_id"),
            "person_publication": ("person_publication_v1", "relation_id"),
        }

        def ensure_bulk_file(base_dir_path: Path, stem: str) -> None:
            bulk_path = base_dir_path / f"{stem}.bulk.ndjson"
            parquet_path = base_dir_path / f"{stem}.parquet"
            if bulk_path.exists() or not parquet_path.exists():
                return
            index_name, id_key = bulk_specs[stem]
            id_stats = {index_name: {"total_docs": 0, "hashed_id_count": 0}}
            write_bulk_from_parquet(parquet_path, bulk_path, index_name, id_key, id_stats)

        subprocess.run(create_cmd, check=True)

        bulk_script = str(base_dir / "bulk_import.ps1")

        if publication_dir:
            ensure_bulk_file(publication_dir, "publication")
            publication_import_cmd = [
                "powershell",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                bulk_script,
                "-Endpoint",
                endpoint,
                "-BulkDir",
                str(publication_dir),
                "-IncludeFiles",
                "publication.bulk.ndjson",
            ]
            if username and password:
                publication_import_cmd += ["-Username", username, "-Password", password]
            if insecure:
                publication_import_cmd += ["-Insecure"]
            subprocess.run(publication_import_cmd, check=True)

        if silver_dir:
            silver_include = [
                "author_occurrence.bulk.ndjson",
                "author_identifier_claim.bulk.ndjson",
                "author_affiliation_claim.bulk.ndjson",
                "author_email_claim.bulk.ndjson",
            ]
            for stem in (
                "author_occurrence",
                "author_identifier_claim",
                "author_affiliation_claim",
                "author_email_claim",
            ):
                ensure_bulk_file(silver_dir, stem)
            silver_import_cmd = [
                "powershell",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                bulk_script,
                "-Endpoint",
                endpoint,
                "-BulkDir",
                str(silver_dir),
                "-IncludeFiles",
                ",".join(silver_include),
            ]
            if username and password:
                silver_import_cmd += ["-Username", username, "-Password", password]
            if insecure:
                silver_import_cmd += ["-Insecure"]
            subprocess.run(silver_import_cmd, check=True)

        if match_dir:
            match_include = [
                "person.bulk.ndjson",
                "person_publication.bulk.ndjson",
            ]
            for stem in ("person", "person_publication"):
                ensure_bulk_file(match_dir, stem)
            match_import_cmd = [
                "powershell",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                bulk_script,
                "-Endpoint",
                endpoint,
                "-BulkDir",
                str(match_dir),
                "-IncludeFiles",
                ",".join(match_include),
            ]
            if username and password:
                match_import_cmd += ["-Username", username, "-Password", password]
            if insecure:
                match_import_cmd += ["-Insecure"]
            subprocess.run(match_import_cmd, check=True)
        elif silver_dir:
            compat_person = silver_dir / "person.bulk.ndjson"
            compat_relation = silver_dir / "person_publication.bulk.ndjson"
            if compat_person.exists() and compat_relation.exists():
                compat_import_cmd = [
                    "powershell",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-File",
                    bulk_script,
                    "-Endpoint",
                    endpoint,
                    "-BulkDir",
                    str(silver_dir),
                    "-IncludeFiles",
                    "person.bulk.ndjson,person_publication.bulk.ndjson",
                ]
                if username and password:
                    compat_import_cmd += ["-Username", username, "-Password", password]
                if insecure:
                    compat_import_cmd += ["-Insecure"]
                subprocess.run(compat_import_cmd, check=True)
            else:
                raise FileNotFoundError(
                    "index stage requires match output or silver compatibility person/person_publication bulks"
                )

        for job in jobs:
            set_job_done(conn, int(job["job_id"]), True, None)
        conn.commit()
        return {
            "processed": len(jobs),
            "silver_source": str(silver_dir.resolve()) if silver_dir else None,
            "vector_source": str(vector_dir.resolve()) if vector_dir else None,
            "publication_source": str(publication_dir.resolve()) if publication_dir else None,
            "match_source": str(match_dir.resolve()) if match_dir else None,
            "source_stage": (
                "vector+match+silver"
                if publication_dir == vector_dir and match_dir and silver_dir
                else "match+silver"
                if match_dir and silver_dir
                else ("match" if match_dir else ("vector" if publication_dir == vector_dir else "silver"))
            ),
        }
    except Exception as exc:
        err = f"{exc}\n{traceback.format_exc()}"
        for job in jobs:
            set_job_done(conn, int(job["job_id"]), False, err)
        conn.commit()
        return {"processed": len(jobs), "error": str(exc)}


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Run staged pipeline based on ingest_jobs in manifest.db.")
    p.add_argument("--db-path", default="manifest.db", help="Path to manifest sqlite db")
    p.add_argument("--base-dir", default=".", help="Project base dir containing scripts")
    p.add_argument("--output-root", default="data_pipeline", help="Pipeline output root")
    p.add_argument("--max-jobs-per-stage", type=int, default=200, help="Max pending jobs consumed per stage")
    p.add_argument("--stages", default="scan,bronze,silver,vector,match,index", help="Comma-separated stages to run")
    p.add_argument("--source-group", default=None, choices=[None, "scholars", "frontiers"], help="Optional source group filter for scan/bronze")
    p.add_argument("--matched-json", default=None, help="Optional matched json path for silver ETL")
    p.add_argument("--qs-top200-json", default=None, help="Optional QS Top 200 ranking json path for silver ETL")
    p.add_argument("--world-top500-json", default=None, help="Optional World Top 500 ranking json path for silver ETL")
    p.add_argument("--storage-format", default="parquet", choices=["parquet", "jsonl"], help="Primary storage format for bronze/silver/match")
    p.add_argument("--write-debug-jsonl", action="store_true", help="Write JSONL debug copies alongside parquet outputs")
    p.add_argument("--write-silver-compat-person", action="store_true", help="Write compatibility person/person_publication outputs in silver stage")
    p.add_argument("--write-match-audit", action="store_true", help="Write person_match_audit.jsonl in match stage")
    p.add_argument("--force-reembed", action="store_true", help="Force re-embedding all publications in vector stage")
    p.add_argument("--strict-vector", action="store_true", help="Fail vector stage when embed failures exceed threshold")
    p.add_argument("--vector-shard-size", type=int, default=1000, help="Publication rows per vector parquet shard")
    p.add_argument("--bronze-workers", default="auto", help="Worker count for bronze conversion. Default: auto")
    p.add_argument("--silver-workers", default="auto", help="Worker count for silver frontiers shard processing. Default: auto")
    p.add_argument("--silver-shards", default="auto", help="Shard count for silver frontiers processing. Default: auto")
    p.add_argument("--match-workers", default="auto", help="Worker count for match candidate evaluation. Default: auto")
    p.add_argument("--match-batch-size", type=int, default=200000, help="Candidate pairs per batch in match stage")
    p.add_argument("--opensearch-endpoint", default=None, help="OpenSearch endpoint for index stage")
    p.add_argument("--opensearch-username", default=None, help="OpenSearch username")
    p.add_argument("--opensearch-password", default=None, help="OpenSearch password")
    p.add_argument("--opensearch-insecure", action="store_true", help="Skip TLS cert verification in index stage")
    return p


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    db_path = Path(args.db_path)
    base_dir = Path(args.base_dir).resolve()
    output_root = Path(args.output_root).resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    stages = [s.strip() for s in args.stages.split(",") if s.strip()]
    for s in stages:
        if s not in STAGE_ORDER:
            raise ValueError(f"Invalid stage: {s}")

    with connect(db_path) as conn:
        report: Dict[str, object] = {
            "started_at": now_iso(),
            "db_path": str(db_path.resolve()),
            "stages": stages,
            "max_jobs_per_stage": args.max_jobs_per_stage,
            "storage_format": args.storage_format,
            "write_debug_jsonl": bool(args.write_debug_jsonl),
            "bronze_workers": str(args.bronze_workers),
            "silver_workers": str(args.silver_workers),
            "silver_shards": str(args.silver_shards),
            "match_workers": str(args.match_workers),
            "match_batch_size": int(args.match_batch_size),
            "results": {},
        }

        for stage in stages:
            jobs = fetch_pending_jobs(conn, stage, args.max_jobs_per_stage, args.source_group if stage in ("scan", "bronze") else None)
            if stage == "scan":
                report["results"][stage] = stage_scan(conn, jobs)
            elif stage == "bronze":
                report["results"][stage] = stage_bronze(
                    conn,
                    jobs,
                    output_root,
                    args.storage_format,
                    args.write_debug_jsonl,
                    args.bronze_workers,
                )
            elif stage == "silver":
                report["results"][stage] = stage_silver(
                    conn=conn,
                    jobs=jobs,
                    base_dir=base_dir,
                    output_root=output_root,
                    matched_json=Path(args.matched_json) if args.matched_json else None,
                    qs_top200_json=Path(args.qs_top200_json) if args.qs_top200_json else None,
                    world_top500_json=Path(args.world_top500_json) if args.world_top500_json else None,
                    write_compat_person=args.write_silver_compat_person,
                    storage_format=args.storage_format,
                    write_debug_jsonl=args.write_debug_jsonl,
                    silver_workers=args.silver_workers,
                    silver_shards=args.silver_shards,
                )
            elif stage == "vector":
                report["results"][stage] = stage_vector(
                    conn=conn,
                    jobs=jobs,
                    base_dir=base_dir,
                    output_root=output_root,
                    force_reembed=args.force_reembed,
                    strict_vector=args.strict_vector,
                    write_debug_jsonl=args.write_debug_jsonl,
                    shard_size=args.vector_shard_size,
                )
            elif stage == "match":
                report["results"][stage] = stage_match(
                    conn=conn,
                    jobs=jobs,
                    base_dir=base_dir,
                    output_root=output_root,
                    write_match_audit=args.write_match_audit,
                    write_debug_jsonl=args.write_debug_jsonl,
                    match_workers=args.match_workers,
                    match_batch_size=args.match_batch_size,
                )
            elif stage == "index":
                report["results"][stage] = stage_index(
                    conn=conn,
                    jobs=jobs,
                    base_dir=base_dir,
                    output_root=output_root,
                    endpoint=args.opensearch_endpoint,
                    username=args.opensearch_username,
                    password=args.opensearch_password,
                    insecure=args.opensearch_insecure,
                )

        report["finished_at"] = now_iso()

    summary_path = output_root / "pipeline_run_summary.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print(json.dumps({"summary": str(summary_path), "results": report["results"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
