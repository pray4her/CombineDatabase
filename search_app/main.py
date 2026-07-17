from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from search_app.catalog import SearchCatalog
from search_app.exporter import EXPORT_DIR, ExportJobRegistry, run_export_job
from search_app.models import CountRequest, ExportRequest, QueryRequest
from search_app.opensearch_client import OpenSearchClient, OpenSearchRequestError
from search_app.query_builder import build_search_request, pretty_json


APP_DIR = Path(__file__).resolve().parent
STATIC_DIR = APP_DIR / "static"

app = FastAPI(title="OpenSearch Structured Search API", version="1.0.0")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

catalog = SearchCatalog()
client = OpenSearchClient()
export_registry = ExportJobRegistry()


def get_nested_value(payload: Any, field_parts: List[str]) -> Any:
    if not field_parts:
        return payload
    if payload is None:
        return None
    head, *tail = field_parts
    if isinstance(payload, list):
        return [get_nested_value(item, field_parts) for item in payload]
    if isinstance(payload, dict):
        return get_nested_value(payload.get(head), tail)
    return None


def flatten_selected_row(source: Dict[str, Any], select_fields: List[str]) -> Dict[str, Any]:
    row: Dict[str, Any] = {}
    for field_name in select_fields:
        row[field_name] = get_nested_value(source, field_name.split("."))
    return row


@app.get("/")
def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/search/entities")
def list_entities() -> Dict[str, Any]:
    return {"entities": catalog.list_entities()}


@app.get("/api/search/entities/{entity_key}/fields")
def list_entity_fields(entity_key: str) -> Dict[str, Any]:
    try:
        entity_meta = catalog.get_entity(entity_key)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return {
        "entity": entity_meta.entity_key,
        "cn_label": entity_meta.cn_label,
        "index_name": entity_meta.index_name,
        "default_visible_fields": entity_meta.default_visible_fields,
        "default_sort": entity_meta.default_sort,
        "fields": [field_meta.to_dict() for field_meta in entity_meta.fields],
    }


@app.post("/api/search/query")
def query_index(request: QueryRequest) -> Dict[str, Any]:
    try:
        build_result = build_search_request(
            entity_key=request.entity,
            query_tree=request.query_tree,
            select_fields=request.select_fields,
            sort=[item.model_dump() for item in request.sort],
            page=request.page,
            page_size=request.page_size,
            search_after=request.search_after,
            catalog=catalog,
        )
        response = client.search(build_result.index_name, build_result.body)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except OpenSearchRequestError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc

    hits = response.get("hits", {}).get("hits", [])
    total_payload = response.get("hits", {}).get("total", {})
    total = total_payload.get("value", 0) if isinstance(total_payload, dict) else total_payload

    columns = [catalog.get_field(request.entity, field_name).to_dict() for field_name in build_result.select_fields]
    rows = []
    for hit in hits:
        source = hit.get("_source", {})
        row = flatten_selected_row(source, build_result.select_fields)
        row["_search_after"] = hit.get("sort")
        rows.append(row)

    return {
        "entity": request.entity,
        "rows": rows,
        "total": total,
        "page": request.page,
        "page_size": request.page_size,
        "search_after": rows[-1]["_search_after"] if rows else None,
        "columns": columns,
        "generated_dsl": build_result.body,
        "generated_dsl_pretty": pretty_json(build_result.body),
        "generated_sql": build_result.sql,
        "generated_sql_request": build_result.sql_request_body,
        "generated_sql_request_pretty": pretty_json(build_result.sql_request_body),
        "sql_mode": build_result.sql_mode,
        "warnings": build_result.warnings,
    }


@app.post("/api/search/count")
def count_index(request: CountRequest) -> Dict[str, Any]:
    try:
        build_result = build_search_request(
            entity_key=request.entity,
            query_tree=request.query_tree,
            select_fields=[],
            sort=[],
            page=1,
            page_size=1,
            search_after=None,
            catalog=catalog,
        )
        response = client.count(build_result.index_name, build_result.query)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except OpenSearchRequestError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc

    return {
        "entity": request.entity,
        "total": response.get("count", 0),
        "generated_dsl": {"query": build_result.query},
        "warnings": build_result.warnings,
    }


@app.post("/api/search/export")
def create_export(request: ExportRequest, background_tasks: BackgroundTasks) -> Dict[str, Any]:
    try:
        build_search_request(
            entity_key=request.entity,
            query_tree=request.query_tree,
            select_fields=request.select_fields,
            sort=[item.model_dump() for item in request.sort],
            page=1,
            page_size=100,
            search_after=None,
            catalog=catalog,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    job = export_registry.create(entity=request.entity, format_name=request.format)
    background_tasks.add_task(
        run_export_job,
        registry=export_registry,
        job_id=job.job_id,
        export_request={
            "entity": request.entity,
            "query_tree": request.query_tree,
            "select_fields": request.select_fields,
            "sort": [item.model_dump() for item in request.sort],
            "format": request.format,
        },
        catalog=catalog,
        client=client,
    )
    return {"job": job.to_dict()}


@app.get("/api/search/export/jobs/{job_id}")
def get_export_job(job_id: str) -> Dict[str, Any]:
    try:
        job = export_registry.get(job_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"job": job.to_dict()}


@app.get("/api/search/export/jobs/{job_id}/download")
def download_export(job_id: str) -> FileResponse:
    try:
        job = export_registry.get(job_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    if job.status != "completed" or not job.file_name:
        raise HTTPException(status_code=409, detail="Export job is not ready for download.")

    file_path = EXPORT_DIR / job.file_name
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="Export file not found.")
    return FileResponse(file_path, filename=job.file_name)
