"""SQLite adapter and file exporter for MailMergeProjection."""

from __future__ import annotations

import csv
from contextlib import closing
import sqlite3
from pathlib import Path
from typing import Any, Iterable

from mail_merge_projection import PROJECTION_COLUMNS

CONTACT_COLUMNS = (
    "id", "short_name", "full_name", "email", "country", "institution", "institution_norm",
    "research_areas", "wos_categories", "similarity", "email_validity",
)
OPTIONAL_EVIDENCE_COLUMNS = (
    "title", "source_title", "publication_year", "author_order",
    "is_corresponding_author", "times_cited",
)
TRUTHY_STRINGS = {"1", "true", "yes", "y", "t"}


def _split_terms(value: Any) -> list[str]:
    if value is None:
        return []
    return [term.strip() for term in str(value).replace("|", ";").split(";") if term.strip()]


def _parse_truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in TRUTHY_STRINGS
    return bool(value)


def load_candidates_from_records_db(db_path: str | Path) -> list[dict[str, Any]]:
    """Load and group pre-merge ``records`` rows by case-insensitive email."""
    with closing(sqlite3.connect(str(db_path))) as connection:
        connection.row_factory = sqlite3.Row
        available = {row[1] for row in connection.execute("PRAGMA table_info(records)")}
        if not available:
            raise ValueError("SQLite database has no records table")
        selected = [column for column in CONTACT_COLUMNS + OPTIONAL_EVIDENCE_COLUMNS if column in available]
        rows = connection.execute(
            "SELECT " + ", ".join('"' + column + '"' for column in selected) + " FROM records"
        ).fetchall()

    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        data = dict(row)
        email = data.get("email")
        email_key = str(email).strip().lower() if email is not None else ""
        name = data.get("full_name") or data.get("short_name") or ""
        name_key = str(name).strip().lower()
        key = email_key + "|" + name_key
        if key not in grouped:
            grouped[key] = {
                column: data.get(column) for column in CONTACT_COLUMNS if column in data
            }
            grouped[key]["research_areas"] = _split_terms(data.get("research_areas"))
            grouped[key]["wos_categories"] = _split_terms(data.get("wos_categories"))
            grouped[key]["paper_evidences"] = []
        evidence = {column: data.get(column) for column in OPTIONAL_EVIDENCE_COLUMNS if column in data}
        evidence["similarity"] = data.get("similarity")
        evidence["is_corresponding_author"] = _parse_truthy(evidence.get("is_corresponding_author"))
        grouped[key]["paper_evidences"].append(evidence)
    return list(grouped.values())


def export_mail_merge_projection(rows: Iterable[dict[str, Any]], path: str | Path) -> None:
    """Write the documented projection columns as CSV or, when available, XLSX."""
    destination = Path(path)
    normalized = [{column: row.get(column) for column in PROJECTION_COLUMNS} for row in rows]
    if destination.suffix.lower() == ".xlsx":
        try:
            import pandas as pd
        except ImportError as exc:
            raise RuntimeError("XLSX export requires pandas") from exc
        pd.DataFrame(normalized, columns=PROJECTION_COLUMNS).to_excel(destination, index=False)
        return
    with destination.open("w", newline="", encoding="utf-8-sig") as output:
        writer = csv.DictWriter(output, fieldnames=PROJECTION_COLUMNS)
        writer.writeheader()
        writer.writerows(normalized)

