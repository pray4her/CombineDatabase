from __future__ import annotations

import csv
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from openpyxl import Workbook

from search_app.catalog import SearchCatalog
from search_app.opensearch_client import OpenSearchClient
from search_app.query_builder import QueryBuildResult, build_search_request


EXPORT_DIR = Path(__file__).resolve().parent / "exports"
EXPORT_DIR.mkdir(parents=True, exist_ok=True)


@dataclass
class ExportJob:
    job_id: str
    entity: str
    format: str
    status: str = "pending"
    created_at: str = field(default_factory=lambda: datetime.now().isoformat(timespec="seconds"))
    updated_at: str = field(default_factory=lambda: datetime.now().isoformat(timespec="seconds"))
    file_name: Optional[str] = None
    download_path: Optional[str] = None
    warnings: List[str] = field(default_factory=list)
    error: Optional[str] = None
    row_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "job_id": self.job_id,
            "entity": self.entity,
            "format": self.format,
            "status": self.status,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "file_name": self.file_name,
            "download_path": self.download_path,
            "warnings": self.warnings,
            "error": self.error,
            "row_count": self.row_count,
        }


class ExportJobRegistry:
    def __init__(self) -> None:
        self._jobs: Dict[str, ExportJob] = {}
        self._lock = threading.Lock()

    def create(self, entity: str, format_name: str) -> ExportJob:
        job = ExportJob(job_id=str(uuid.uuid4()), entity=entity, format=format_name)
        with self._lock:
            self._jobs[job.job_id] = job
        return job

    def get(self, job_id: str) -> ExportJob:
        with self._lock:
            if job_id not in self._jobs:
                raise KeyError(f"Unknown export job: {job_id}")
            return self._jobs[job_id]

    def update(self, job_id: str, **changes: Any) -> ExportJob:
        with self._lock:
            if job_id not in self._jobs:
                raise KeyError(f"Unknown export job: {job_id}")
            job = self._jobs[job_id]
            for key, value in changes.items():
                setattr(job, key, value)
            job.updated_at = datetime.now().isoformat(timespec="seconds")
            return job


def flatten_value(value: Any) -> Any:
    if isinstance(value, list):
        flattened = []
        for item in value:
            if isinstance(item, dict):
                flattened.append(str(item))
            else:
                flattened.append(str(item))
        return "；".join(flattened)
    if isinstance(value, bool):
        return "是" if value else "否"
    if isinstance(value, dict):
        return str(value)
    return value


def extract_field_value(payload: Any, field_parts: List[str]) -> Any:
    if not field_parts:
        return payload
    if payload is None:
        return None
    head, *tail = field_parts
    if isinstance(payload, list):
        return [extract_field_value(item, field_parts) for item in payload]
    if isinstance(payload, dict):
        return extract_field_value(payload.get(head), tail)
    return None


def fetch_all_rows(client: OpenSearchClient, build_result: QueryBuildResult) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    search_after: Optional[List[Any]] = None
    while True:
        body = dict(build_result.body)
        body["size"] = min(build_result.page_size, 1000)
        body.pop("from", None)
        if search_after:
            body["search_after"] = search_after
        else:
            body.pop("search_after", None)

        response = client.search(build_result.index_name, body)
        hits = response.get("hits", {}).get("hits", [])
        if not hits:
            break

        for hit in hits:
            source = hit.get("_source", {})
            rows.append(source)

        search_after = hits[-1].get("sort")
        if not search_after or len(hits) < body["size"]:
            break
    return rows


def write_csv(file_path: Path, rows: List[Dict[str, Any]], columns: List[str]) -> None:
    with file_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    column: flatten_value(extract_field_value(row, column.split(".")))
                    for column in columns
                }
            )


def write_xlsx(file_path: Path, rows: List[Dict[str, Any]], columns: List[str]) -> None:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "results"
    sheet.append(columns)
    for row in rows:
        sheet.append(
            [flatten_value(extract_field_value(row, column.split("."))) for column in columns]
        )
    workbook.save(file_path)


def run_export_job(
    *,
    registry: ExportJobRegistry,
    job_id: str,
    export_request: Dict[str, Any],
    catalog: SearchCatalog,
    client: OpenSearchClient,
) -> None:
    registry.update(job_id, status="running")
    try:
        build_result = build_search_request(
            entity_key=export_request["entity"],
            query_tree=export_request["query_tree"],
            select_fields=export_request["select_fields"],
            sort=export_request["sort"],
            page=1,
            page_size=500,
            search_after=None,
            catalog=catalog,
        )
        rows = fetch_all_rows(client, build_result)
        extension = export_request["format"]
        file_name = f"{job_id}.{extension}"
        file_path = EXPORT_DIR / file_name

        if extension == "csv":
            write_csv(file_path, rows, build_result.select_fields)
        else:
            write_xlsx(file_path, rows, build_result.select_fields)

        registry.update(
            job_id,
            status="completed",
            file_name=file_name,
            download_path=f"/api/search/export/jobs/{job_id}/download",
            warnings=build_result.warnings,
            row_count=len(rows),
        )
    except Exception as exc:  # pragma: no cover - defensive path
        registry.update(job_id, status="failed", error=str(exc))
