import hashlib
import json
import math
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional


try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:  # pragma: no cover - exercised in environments without parquet deps
    pa = None
    pq = None


MAX_OPENSEARCH_ID_BYTES = 512


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def require_pyarrow() -> None:
    if pa is None or pq is None:
        raise RuntimeError(
            "Parquet support requires pyarrow. Install with `python -m pip install pyarrow`."
        )


def sha1_text(value: object) -> str:
    return hashlib.sha1(str(value).encode("utf-8")).hexdigest()


def ensure_safe_bulk_id(raw_id: object) -> tuple[str, Optional[str]]:
    raw_id_str = str(raw_id)
    if len(raw_id_str.encode("utf-8")) <= MAX_OPENSEARCH_ID_BYTES:
        return raw_id_str, None
    return sha1_text(raw_id_str), raw_id_str


def write_jsonl(path: Path, rows: Iterable[Dict[str, object]]) -> None:
    ensure_parent(path)
    with open(path, "w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def sanitize_string(value: str) -> Optional[str]:
    cleaned_chars = []
    for ch in value:
        if ch in ("\t", "\r", "\n"):
            cleaned_chars.append(ch)
            continue
        category = ord(ch)
        if category < 32 or category == 127:
            continue
        cleaned_chars.append(ch)
    cleaned = "".join(cleaned_chars).strip()
    return cleaned or None


def sanitize_document_value(value: object) -> object:
    if value is None:
        return None
    if isinstance(value, str):
        return sanitize_string(value)
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, list):
        output = []
        for item in value:
            cleaned = sanitize_document_value(item)
            if cleaned in (None, "", [], {}):
                continue
            output.append(cleaned)
        return output
    if isinstance(value, dict):
        output = {}
        for key, item in value.items():
            cleaned = sanitize_document_value(item)
            if cleaned in (None, "", [], {}):
                continue
            output[str(key)] = cleaned
        return output
    return sanitize_string(str(value))


def sanitize_document(row: Dict[str, object]) -> Dict[str, object]:
    output: Dict[str, object] = {}
    for key, value in row.items():
        cleaned = sanitize_document_value(value)
        if cleaned is None and key != "original_id":
            continue
        output[key] = cleaned
    return output


def _empty_table() -> "pa.Table":
    require_pyarrow()
    return pa.table({})


def write_pylist_parquet(path: Path, rows: Iterable[Dict[str, object]], compression: str = "zstd") -> None:
    require_pyarrow()
    ensure_parent(path)
    materialized = list(rows)
    if materialized:
        table = pa.Table.from_pylist(materialized)
    else:
        table = _empty_table()
    pq.write_table(table, path, compression=compression, use_dictionary=True)


def iter_parquet_rows(path: Path, batch_size: int = 2048) -> Iterator[Dict[str, object]]:
    require_pyarrow()
    parquet_file = pq.ParquetFile(path)
    for batch in parquet_file.iter_batches(batch_size=batch_size):
        table = pa.Table.from_batches([batch])
        for row in table.to_pylist():
            if isinstance(row, dict):
                yield row


def load_parquet_rows(path: Path, batch_size: int = 2048) -> List[Dict[str, object]]:
    return list(iter_parquet_rows(path, batch_size=batch_size))


def write_bulk(
    path: Path,
    index_name: str,
    id_key: str,
    rows: Iterable[Dict[str, object]],
    id_safety_stats: Dict[str, Dict[str, int]],
) -> None:
    ensure_parent(path)
    with open(path, "w", encoding="utf-8") as handle:
        for row in rows:
            safe_id, original_id = ensure_safe_bulk_id(row[id_key])
            payload = sanitize_document(row)
            if original_id is not None:
                payload = dict(row)
                payload["original_id"] = original_id
                payload = sanitize_document(payload)
                id_safety_stats[index_name]["hashed_id_count"] += 1
            action = {"index": {"_index": index_name, "_id": safe_id}}
            handle.write(json.dumps(action, ensure_ascii=False) + "\n")
            handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
            id_safety_stats[index_name]["total_docs"] += 1


def write_bulk_from_parquet(
    parquet_path: Path,
    bulk_path: Path,
    index_name: str,
    id_key: str,
    id_safety_stats: Dict[str, Dict[str, int]],
) -> None:
    write_bulk(bulk_path, index_name, id_key, iter_parquet_rows(parquet_path), id_safety_stats)


def file_size_or_none(path: Optional[Path]) -> Optional[int]:
    if path is None or not path.exists():
        return None
    return int(path.stat().st_size)
