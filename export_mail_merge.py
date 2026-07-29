"""Export a MailMergeProjection from a pre-merge SQLite records database."""

from __future__ import annotations

import argparse
from pathlib import Path

from mail_merge_adapter import export_mail_merge_projection, load_candidates_from_records_db
from mail_merge_projection import build_mail_merge_projection


def main() -> None:
    parser = argparse.ArgumentParser(description="Export AuthorContact MailMergeProjection rows.")
    parser.add_argument("--db-path", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--similarity-threshold", type=float, default=0.6)
    args = parser.parse_args()
    candidates = load_candidates_from_records_db(args.db_path)
    rows = build_mail_merge_projection(candidates, similarity_threshold=args.similarity_threshold)
    export_mail_merge_projection(rows, args.output)
    print(f"Exported {len(rows)} MailMergeProjection rows to {args.output}")


if __name__ == "__main__":
    main()
