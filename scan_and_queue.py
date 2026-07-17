import argparse
import hashlib
import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


DEFAULT_STAGES = ("scan", "bronze", "silver", "vector", "match", "index")
ALLOWED_STAGES = set(DEFAULT_STAGES)
ALLOWED_STATUS = {"pending", "running", "success", "failed"}
ALLOWED_FILE_STATUS = {"new", "changed", "unchanged", "deleted"}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha1_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha1()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def ensure_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS file_manifest (
            file_path TEXT PRIMARY KEY,
            source_group TEXT NOT NULL CHECK (source_group IN ('scholars', 'frontiers')),
            file_size INTEGER NOT NULL,
            mtime_utc TEXT NOT NULL,
            sha1 TEXT NOT NULL,
            discovered_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            status TEXT NOT NULL CHECK (status IN ('new', 'changed', 'unchanged', 'deleted'))
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ingest_jobs (
            job_id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_path TEXT NOT NULL,
            sha1 TEXT NOT NULL,
            stage TEXT NOT NULL CHECK (stage IN ('scan', 'bronze', 'silver', 'vector', 'match', 'index')),
            status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'success', 'failed')),
            error_message TEXT,
            started_at TEXT,
            ended_at TEXT,
            retry_count INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(file_path, sha1, stage)
        )
        """
    )
    _migrate_ingest_jobs_stage_check(conn)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_manifest_source_group ON file_manifest(source_group)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_manifest_status ON file_manifest(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status_stage ON ingest_jobs(status, stage)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_ended_at ON ingest_jobs(ended_at)")
    conn.commit()


def _migrate_ingest_jobs_stage_check(conn: sqlite3.Connection) -> None:
    """Recreate ingest_jobs when legacy CHECK omits newer stages such as vector."""
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'ingest_jobs'"
    ).fetchone()
    if row is None or row[0] is None:
        return
    create_sql = str(row[0])
    required = ("'scan'", "'bronze'", "'silver'", "'vector'", "'match'", "'index'")
    if all(token in create_sql for token in required):
        return

    conn.execute("ALTER TABLE ingest_jobs RENAME TO ingest_jobs_legacy")
    conn.execute(
        """
        CREATE TABLE ingest_jobs (
            job_id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_path TEXT NOT NULL,
            sha1 TEXT NOT NULL,
            stage TEXT NOT NULL CHECK (stage IN ('scan', 'bronze', 'silver', 'vector', 'match', 'index')),
            status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'success', 'failed')),
            error_message TEXT,
            started_at TEXT,
            ended_at TEXT,
            retry_count INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(file_path, sha1, stage)
        )
        """
    )
    conn.execute(
        """
        INSERT INTO ingest_jobs (
            job_id, file_path, sha1, stage, status, error_message,
            started_at, ended_at, retry_count, created_at, updated_at
        )
        SELECT
            job_id, file_path, sha1, stage, status, error_message,
            started_at, ended_at, retry_count, created_at, updated_at
        FROM ingest_jobs_legacy
        """
    )
    conn.execute("DROP TABLE ingest_jobs_legacy")


def normalize_path(path: Path) -> str:
    return str(path.resolve())


def scan_files(root: Path, extensions: Tuple[str, ...]) -> Iterable[Path]:
    lower_ext = tuple(x.lower() for x in extensions)
    for dirpath, _, filenames in os.walk(root):
        for name in filenames:
            if name.startswith("~$"):
                continue
            if name.lower().endswith(lower_ext):
                yield Path(dirpath) / name


def load_manifest_by_group(conn: sqlite3.Connection, source_group: str) -> Dict[str, sqlite3.Row]:
    cur = conn.execute("SELECT * FROM file_manifest WHERE source_group = ?", (source_group,))
    return {row["file_path"]: row for row in cur.fetchall()}


def enqueue_jobs(
    conn: sqlite3.Connection,
    file_path: str,
    sha1: str,
    stages: Tuple[str, ...],
    now: str,
) -> int:
    created = 0
    for stage in stages:
        if stage not in ALLOWED_STAGES:
            continue
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO ingest_jobs
            (file_path, sha1, stage, status, error_message, started_at, ended_at, retry_count, created_at, updated_at)
            VALUES (?, ?, ?, 'pending', NULL, NULL, NULL, 0, ?, ?)
            """,
            (file_path, sha1, stage, now, now),
        )
        created += cur.rowcount
    return created


def upsert_manifest_row(
    conn: sqlite3.Connection,
    file_path: str,
    source_group: str,
    file_size: int,
    mtime_utc: str,
    sha1: str,
    discovered_at: Optional[str],
    last_seen_at: str,
    status: str,
) -> None:
    if status not in ALLOWED_FILE_STATUS:
        raise ValueError(f"invalid file status: {status}")
    conn.execute(
        """
        INSERT INTO file_manifest
        (file_path, source_group, file_size, mtime_utc, sha1, discovered_at, last_seen_at, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(file_path) DO UPDATE SET
            source_group=excluded.source_group,
            file_size=excluded.file_size,
            mtime_utc=excluded.mtime_utc,
            sha1=excluded.sha1,
            last_seen_at=excluded.last_seen_at,
            status=excluded.status
        """,
        (
            file_path,
            source_group,
            file_size,
            mtime_utc,
            sha1,
            discovered_at or last_seen_at,
            last_seen_at,
            status,
        ),
    )


def scan_one_group(
    conn: sqlite3.Connection,
    source_group: str,
    root: Path,
    extensions: Tuple[str, ...],
    stages: Tuple[str, ...],
    now: str,
) -> Dict[str, int]:
    counters = {
        "seen": 0,
        "new": 0,
        "changed": 0,
        "unchanged": 0,
        "deleted": 0,
        "jobs_created": 0,
    }
    existing = load_manifest_by_group(conn, source_group)
    seen_paths = set()

    for file_path_obj in scan_files(root, extensions):
        counters["seen"] += 1
        file_path = normalize_path(file_path_obj)
        seen_paths.add(file_path)

        stat = file_path_obj.stat()
        file_size = int(stat.st_size)
        mtime_utc = datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat()
        digest = sha1_file(file_path_obj)

        old = existing.get(file_path)
        if old is None:
            status = "new"
            counters["new"] += 1
            discovered_at = now
            counters["jobs_created"] += enqueue_jobs(conn, file_path, digest, stages, now)
        else:
            changed = (
                old["sha1"] != digest
                or int(old["file_size"]) != file_size
                or old["mtime_utc"] != mtime_utc
            )
            if changed:
                status = "changed"
                counters["changed"] += 1
                discovered_at = old["discovered_at"]
                counters["jobs_created"] += enqueue_jobs(conn, file_path, digest, stages, now)
            else:
                status = "unchanged"
                counters["unchanged"] += 1
                discovered_at = old["discovered_at"]

        upsert_manifest_row(
            conn=conn,
            file_path=file_path,
            source_group=source_group,
            file_size=file_size,
            mtime_utc=mtime_utc,
            sha1=digest,
            discovered_at=discovered_at,
            last_seen_at=now,
            status=status,
        )

    for old_path in existing.keys():
        if old_path in seen_paths:
            continue
        counters["deleted"] += 1
        conn.execute(
            """
            UPDATE file_manifest
            SET status = 'deleted', last_seen_at = ?
            WHERE file_path = ?
            """,
            (now, old_path),
        )

    return counters


def parse_since(since_text: str) -> str:
    try:
        dt = datetime.fromisoformat(since_text.replace("Z", "+00:00"))
    except Exception as exc:
        raise ValueError(f"invalid --since format: {since_text}") from exc
    return dt.isoformat()


def rerun_jobs(
    conn: sqlite3.Connection,
    job_id: Optional[int],
    since: Optional[str],
    max_retries: int,
    now: str,
) -> Dict[str, int]:
    if job_id is None and since is None:
        raise ValueError("rerun mode requires --job-id or --since")

    params: List[object] = []
    where = ["status = 'failed'", "retry_count < ?"]
    params.append(max_retries)

    if job_id is not None:
        where.append("job_id = ?")
        params.append(job_id)
    if since is not None:
        where.append("COALESCE(ended_at, created_at) >= ?")
        params.append(parse_since(since))

    sql = f"""
        UPDATE ingest_jobs
        SET
            status = 'pending',
            error_message = NULL,
            started_at = NULL,
            ended_at = NULL,
            retry_count = retry_count + 1,
            updated_at = ?
        WHERE {' AND '.join(where)}
    """
    params = [now] + params
    cur = conn.execute(sql, params)
    return {"rerun_queued": cur.rowcount}


def run_scan(args: argparse.Namespace) -> Dict[str, object]:
    db_path = Path(args.db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    now = now_iso()
    stages = tuple(s.strip() for s in args.enqueue_stages.split(",") if s.strip())
    if any(s not in ALLOWED_STAGES for s in stages):
        bad = [s for s in stages if s not in ALLOWED_STAGES]
        raise ValueError(f"invalid stage(s): {bad}")

    scholars_root = Path(args.scholars_root)
    frontiers_root = Path(args.frontiers_root)
    if not scholars_root.exists():
        raise FileNotFoundError(f"scholars root not found: {scholars_root}")
    if not frontiers_root.exists():
        raise FileNotFoundError(f"frontiers root not found: {frontiers_root}")

    exts = tuple(x.strip() for x in args.extensions.split(",") if x.strip())
    if not exts:
        raise ValueError("no extensions configured")

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        ensure_db(conn)
        conn.execute("BEGIN")
        scholars_stats = scan_one_group(conn, "scholars", scholars_root, exts, stages, now)
        frontiers_stats = scan_one_group(conn, "frontiers", frontiers_root, exts, stages, now)
        conn.commit()

        pending_cur = conn.execute("SELECT COUNT(1) AS c FROM ingest_jobs WHERE status = 'pending'")
        pending_total = int(pending_cur.fetchone()["c"])

    return {
        "mode": "scan",
        "db_path": str(db_path.resolve()),
        "scanned_at": now,
        "extensions": exts,
        "enqueue_stages": stages,
        "groups": {
            "scholars": scholars_stats,
            "frontiers": frontiers_stats,
        },
        "new_jobs_created": scholars_stats["jobs_created"] + frontiers_stats["jobs_created"],
        "pending_total": pending_total,
    }


def run_rerun(args: argparse.Namespace) -> Dict[str, object]:
    db_path = Path(args.db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"db file not found: {db_path}")

    now = now_iso()
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        ensure_db(conn)
        conn.execute("BEGIN")
        stats = rerun_jobs(
            conn=conn,
            job_id=args.job_id,
            since=args.since,
            max_retries=args.max_retries,
            now=now,
        )
        conn.commit()
    return {
        "mode": "rerun",
        "db_path": str(db_path.resolve()),
        "rerun_at": now,
        "job_id": args.job_id,
        "since": args.since,
        "max_retries": args.max_retries,
        **stats,
    }


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Scan source folders and maintain manifest + ingest_jobs.")
    sub = p.add_subparsers(dest="command", required=True)

    s = sub.add_parser("scan", help="Scan roots and enqueue jobs for new/changed files.")
    s.add_argument("--db-path", default="manifest.db", help="SQLite db path")
    s.add_argument("--scholars-root", required=True, help="scholars root directory")
    s.add_argument("--frontiers-root", required=True, help="frontiers root directory")
    s.add_argument(
        "--extensions",
        default=".xlsx,.xlsm,.xls,.csv",
        help="comma-separated file extensions, e.g. .xlsx,.xlsm,.csv",
    )
    s.add_argument(
        "--enqueue-stages",
        default="scan",
        help="comma-separated stages to enqueue for changed files. default: scan",
    )
    s.set_defaults(handler=run_scan)

    r = sub.add_parser("rerun", help="Reset failed jobs to pending with retry +1.")
    r.add_argument("--db-path", default="manifest.db", help="SQLite db path")
    r.add_argument("--job-id", type=int, default=None, help="specific failed job_id to rerun")
    r.add_argument("--since", default=None, help="ISO time window lower bound, e.g. 2026-04-03T00:00:00+00:00")
    r.add_argument("--max-retries", type=int, default=3, help="max retry count (exclusive upper bound)")
    r.set_defaults(handler=run_rerun)

    return p


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    result = args.handler(args)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
