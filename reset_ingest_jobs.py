"""Reset selected ingest_jobs stages back to pending."""

from __future__ import annotations

import argparse
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", required=True)
    parser.add_argument(
        "--stages",
        required=True,
        help="Comma-separated stages to reset, e.g. vector,index",
    )
    args = parser.parse_args()
    stages = [s.strip() for s in args.stages.split(",") if s.strip()]
    now = datetime.now(timezone.utc).isoformat()
    db_path = Path(args.db_path)
    with sqlite3.connect(db_path) as conn:
        placeholders = ",".join("?" for _ in stages)
        cur = conn.execute(
            f"""
            UPDATE ingest_jobs
            SET status = 'pending',
                error_message = NULL,
                started_at = NULL,
                ended_at = NULL,
                updated_at = ?
            WHERE stage IN ({placeholders})
            """,
            [now, *stages],
        )
        conn.commit()
        print({"reset_rows": cur.rowcount, "stages": stages, "db_path": str(db_path)})


if __name__ == "__main__":
    main()
