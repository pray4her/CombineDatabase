"""Embed silver-stage publications for OpenSearch knn_vector indexing.

Streams silver rows, checkpoints embeddings to SQLite after every API batch,
writes parquet shards, then merges final publication outputs. Crashes resume
from the SQLite checkpoint without re-calling the API for completed IDs.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

from pipeline_storage import (
    ensure_parent,
    file_size_or_none,
    iter_parquet_rows,
    load_parquet_rows,
    require_pyarrow,
    write_bulk,
    write_jsonl,
    write_pylist_parquet,
)

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:  # pragma: no cover
    pa = None
    pq = None

EMBEDDING_MODEL = "qwen3.7-text-embedding"
EMBEDDING_DIMENSION = 1024
EMBEDDING_BATCH_SIZE = 20
# Legacy global host; prefer DASHSCOPE_EMBEDDING_BASE_URL =
# https://{WorkspaceId}.cn-beijing.maas.aliyuncs.com/compatible-mode/v1
DEFAULT_BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1"
# Conservative char budget (~8k tokens for mixed CN/EN prose).
MAX_EMBED_CHARS = 24_000
MAX_RETRIES = 5
RETRY_BASE_SECONDS = 1.5
DEFAULT_SHARD_SIZE = 1000


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_dotenv_file(env_path: Path) -> None:
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key or key in os.environ:
            continue
        os.environ[key] = value.strip().strip('"').strip("'")


def parse_embedding(value: object) -> Optional[List[float]]:
    if not isinstance(value, list) or len(value) != EMBEDDING_DIMENSION:
        return None
    try:
        return [float(item) for item in value]
    except (TypeError, ValueError):
        return None


def format_keyword_list(value: object) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, list):
        parts = [str(item).strip() for item in value if str(item).strip()]
        return "; ".join(parts) if parts else None
    text = str(value).strip()
    return text or None


def format_section(label: str, body: Optional[str]) -> Optional[str]:
    if not body:
        return None
    return f"{label}:\n{body}"


def build_embedding_text(row: Dict[str, object], max_chars: int = MAX_EMBED_CHARS) -> Optional[str]:
    """Assemble template text; omit empty sections; require title.

    Truncation keeps Title + keyword/area sections and shrinks Abstract.
    """
    title = str(row.get("title") or "").strip()
    if not title:
        return None

    abstract = str(row.get("abstract") or "").strip() or None
    author_keywords = format_keyword_list(row.get("author_keywords"))
    keywords_plus = format_keyword_list(row.get("keywords_plus"))
    research_areas = format_keyword_list(row.get("research_areas"))

    priority_sections = [
        format_section("Title", title),
        format_section("Author Keywords", author_keywords),
        format_section("Keywords Plus", keywords_plus),
        format_section("Research Areas", research_areas),
    ]
    fixed_parts = [part for part in priority_sections if part]
    fixed_text = "\n\n".join(fixed_parts)
    remaining = max_chars - len(fixed_text) - 2

    abstract_section: Optional[str] = None
    if abstract and remaining > len("Abstract:\n"):
        abstract_budget = remaining - len("Abstract:\n")
        trimmed = abstract if len(abstract) <= abstract_budget else abstract[: max(0, abstract_budget)].rstrip()
        if trimmed:
            abstract_section = format_section("Abstract", trimmed)

    ordered: List[str] = [format_section("Title", title) or ""]
    if abstract_section:
        ordered.append(abstract_section)
    for label, body in (
        ("Author Keywords", author_keywords),
        ("Keywords Plus", keywords_plus),
        ("Research Areas", research_areas),
    ):
        section = format_section(label, body)
        if section:
            ordered.append(section)

    text = "\n\n".join(part for part in ordered if part)
    if len(text) > max_chars:
        text = text[:max_chars].rstrip()
    return text or None


class DashScopeEmbeddingClient:
    def __init__(
        self,
        api_key: str,
        base_url: str = DEFAULT_BASE_URL,
        model: str = EMBEDDING_MODEL,
        dimensions: int = EMBEDDING_DIMENSION,
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.dimensions = dimensions

    def embed_batch(self, texts: Sequence[str]) -> List[List[float]]:
        if not texts:
            return []
        if len(texts) > EMBEDDING_BATCH_SIZE:
            raise ValueError(f"batch size exceeds {EMBEDDING_BATCH_SIZE}")

        payload = {
            "model": self.model,
            "input": list(texts),
            "dimensions": self.dimensions,
            "encoding_format": "float",
        }
        body = json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            f"{self.base_url}/embeddings",
            data=body,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            method="POST",
        )

        last_error: Optional[Exception] = None
        for attempt in range(MAX_RETRIES):
            try:
                with urllib.request.urlopen(request, timeout=120) as response:
                    raw = response.read().decode("utf-8")
                data = json.loads(raw)
                items = data.get("data") or []
                if len(items) != len(texts):
                    raise RuntimeError(
                        f"embedding response size mismatch: got {len(items)}, expected {len(texts)}"
                    )
                ordered = sorted(items, key=lambda item: int(item.get("index", 0)))
                vectors: List[List[float]] = []
                for item in ordered:
                    vector = parse_embedding(item.get("embedding"))
                    if vector is None:
                        raise RuntimeError("invalid embedding vector in API response")
                    vectors.append(vector)
                return vectors
            except urllib.error.HTTPError as exc:
                detail = exc.read().decode("utf-8", errors="replace")
                last_error = RuntimeError(f"HTTP {exc.code}: {detail}")
                if exc.code in {429, 500, 502, 503, 504} and attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_BASE_SECONDS * (2**attempt))
                    continue
                raise last_error from exc
            except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, RuntimeError) as exc:
                last_error = exc
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_BASE_SECONDS * (2**attempt))
                    continue
                raise
        raise RuntimeError(f"embedding failed after retries: {last_error}")


class EmbeddingCheckpoint:
    """SQLite-backed embedding checkpoint for crash-safe resume."""

    def __init__(self, db_path: Path) -> None:
        ensure_parent(db_path)
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS embeddings (
                publication_id TEXT PRIMARY KEY,
                embedding_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    def clear(self) -> None:
        self.conn.execute("DELETE FROM embeddings")
        self.conn.commit()

    def count(self) -> int:
        row = self.conn.execute("SELECT COUNT(*) FROM embeddings").fetchone()
        return int(row[0] if row else 0)

    def has(self, publication_id: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM embeddings WHERE publication_id = ? LIMIT 1",
            (publication_id,),
        ).fetchone()
        return row is not None

    def get(self, publication_id: str) -> Optional[List[float]]:
        row = self.conn.execute(
            "SELECT embedding_json FROM embeddings WHERE publication_id = ?",
            (publication_id,),
        ).fetchone()
        if row is None:
            return None
        try:
            return parse_embedding(json.loads(row[0]))
        except json.JSONDecodeError:
            return None

    def put_many(self, items: Sequence[Tuple[str, List[float]]]) -> None:
        if not items:
            return
        ts = now_iso()
        self.conn.executemany(
            """
            INSERT INTO embeddings (publication_id, embedding_json, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(publication_id) DO UPDATE SET
                embedding_json = excluded.embedding_json,
                updated_at = excluded.updated_at
            """,
            [(pub_id, json.dumps(vector), ts) for pub_id, vector in items],
        )
        self.conn.commit()

    def import_from_publication_file(self, path: Path) -> int:
        if not path.exists():
            return 0
        imported = 0
        rows = load_parquet_rows(path) if path.suffix.lower() == ".parquet" else []
        if path.suffix.lower() == ".jsonl":
            with open(path, "r", encoding="utf-8") as handle:
                for line in handle:
                    raw = line.strip()
                    if not raw:
                        continue
                    item = json.loads(raw)
                    if isinstance(item, dict):
                        rows.append(item)
        batch: List[Tuple[str, List[float]]] = []
        for row in rows:
            pub_id = str(row.get("publication_id") or "").strip()
            vector = parse_embedding(row.get("embedding"))
            if not pub_id or vector is None:
                continue
            batch.append((pub_id, vector))
            if len(batch) >= 500:
                self.put_many(batch)
                imported += len(batch)
                batch = []
        if batch:
            self.put_many(batch)
            imported += len(batch)
        return imported


def resolve_silver_publication(silver_dir: Path) -> tuple[Path, str]:
    parquet_path = silver_dir / "publication.parquet"
    jsonl_path = silver_dir / "publication.jsonl"
    if parquet_path.exists():
        return parquet_path, "parquet"
    if jsonl_path.exists():
        return jsonl_path, "jsonl"
    raise FileNotFoundError(f"missing publication rows under {silver_dir}")


def iter_silver_publications(path: Path, input_format: str) -> Iterator[Dict[str, object]]:
    if input_format == "parquet":
        yield from iter_parquet_rows(path)
        return
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            raw = line.strip()
            if not raw:
                continue
            item = json.loads(raw)
            if isinstance(item, dict):
                yield item


def append_jsonl(path: Path, rows: Iterable[Dict[str, object]]) -> None:
    ensure_parent(path)
    with open(path, "a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def write_progress(path: Path, payload: Dict[str, object]) -> None:
    ensure_parent(path)
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
    tmp.replace(path)


def merge_parquet_shards(shard_paths: Sequence[Path], output_path: Path) -> int:
    require_pyarrow()
    ensure_parent(output_path)
    if not shard_paths:
        write_pylist_parquet(output_path, [])
        return 0

    writer: Optional["pq.ParquetWriter"] = None
    total = 0
    try:
        for shard_path in shard_paths:
            parquet_file = pq.ParquetFile(shard_path)
            for batch in parquet_file.iter_batches(batch_size=1024):
                table = pa.Table.from_batches([batch])
                if writer is None:
                    writer = pq.ParquetWriter(output_path, table.schema, compression="zstd")
                writer.write_table(table)
                total += table.num_rows
    finally:
        if writer is not None:
            writer.close()
    return total


def iter_all_shard_rows(shard_paths: Sequence[Path]) -> Iterator[Dict[str, object]]:
    for shard_path in shard_paths:
        yield from iter_parquet_rows(shard_path)


def flush_pending_batch(
    client: DashScopeEmbeddingClient,
    checkpoint: EmbeddingCheckpoint,
    pending: List[Tuple[str, str]],
    failures_path: Path,
    stats: Dict[str, int],
) -> None:
    if not pending:
        return
    texts = [text for _pub_id, text in pending]
    pub_ids = [pub_id for pub_id, _text in pending]
    try:
        vectors = client.embed_batch(texts)
        checkpoint.put_many(list(zip(pub_ids, vectors)))
        stats["embedded"] += len(vectors)
        stats["checkpoint_flushes"] += 1
    except Exception as exc:  # noqa: BLE001 - soft-fail one API batch
        message = str(exc)
        stats["failed_embed"] += len(pending)
        append_jsonl(
            failures_path,
            [
                {
                    "publication_id": pub_id,
                    "reason": "api_error",
                    "error": message,
                    "at": now_iso(),
                }
                for pub_id in pub_ids
            ],
        )
    pending.clear()


def run_embed_phase(
    silver_path: Path,
    input_format: str,
    client: DashScopeEmbeddingClient,
    checkpoint: EmbeddingCheckpoint,
    failures_path: Path,
    progress_path: Path,
    force_reembed: bool,
) -> Dict[str, int]:
    stats = {
        "input_rows": 0,
        "skipped_missing_title": 0,
        "reused_embeddings": 0,
        "embedded": 0,
        "failed_embed": 0,
        "checkpoint_flushes": 0,
    }
    pending: List[Tuple[str, str]] = []

    for row in iter_silver_publications(silver_path, input_format):
        stats["input_rows"] += 1
        pub_id = str(row.get("publication_id") or "").strip()
        text = build_embedding_text(row)

        if text is None:
            stats["skipped_missing_title"] += 1
            append_jsonl(
                failures_path,
                [{"publication_id": pub_id or None, "reason": "missing_title", "at": now_iso()}],
            )
            continue

        if not pub_id:
            stats["failed_embed"] += 1
            append_jsonl(
                failures_path,
                [{"publication_id": None, "reason": "missing_publication_id", "at": now_iso()}],
            )
            continue

        if not force_reembed and checkpoint.has(pub_id):
            stats["reused_embeddings"] += 1
            continue

        pending.append((pub_id, text))
        if len(pending) >= EMBEDDING_BATCH_SIZE:
            flush_pending_batch(client, checkpoint, pending, failures_path, stats)
            write_progress(
                progress_path,
                {
                    "phase": "embed",
                    "updated_at": now_iso(),
                    "checkpoint_count": checkpoint.count(),
                    "stats": stats,
                },
            )

    flush_pending_batch(client, checkpoint, pending, failures_path, stats)
    write_progress(
        progress_path,
        {
            "phase": "embed_done",
            "updated_at": now_iso(),
            "checkpoint_count": checkpoint.count(),
            "stats": stats,
        },
    )
    return stats


def run_materialize_phase(
    silver_path: Path,
    input_format: str,
    checkpoint: EmbeddingCheckpoint,
    shard_dir: Path,
    shard_size: int,
    output_dir: Path,
    write_debug_jsonl: bool,
    progress_path: Path,
    embed_stats: Dict[str, int],
) -> Dict[str, object]:
    if shard_dir.exists():
        for old in shard_dir.glob("shard_*.parquet"):
            old.unlink()
    shard_dir.mkdir(parents=True, exist_ok=True)

    buffer: List[Dict[str, object]] = []
    shard_index = 0
    shard_paths: List[Path] = []
    with_embedding = 0
    without_embedding = 0

    def flush_shard() -> None:
        nonlocal shard_index, buffer
        if not buffer:
            return
        shard_path = shard_dir / f"shard_{shard_index:05d}.parquet"
        write_pylist_parquet(shard_path, buffer)
        shard_paths.append(shard_path)
        shard_index += 1
        buffer = []
        write_progress(
            progress_path,
            {
                "phase": "materialize",
                "updated_at": now_iso(),
                "shards_written": len(shard_paths),
                "with_embedding": with_embedding,
                "without_embedding": without_embedding,
                "stats": embed_stats,
            },
        )

    for row in iter_silver_publications(silver_path, input_format):
        record = dict(row)
        pub_id = str(record.get("publication_id") or "").strip()
        vector = checkpoint.get(pub_id) if pub_id else None
        if vector is None:
            record.pop("embedding", None)
            without_embedding += 1
        else:
            record["embedding"] = vector
            with_embedding += 1
        buffer.append(record)
        if len(buffer) >= shard_size:
            flush_shard()

    flush_shard()

    parquet_path = output_dir / "publication.parquet"
    merge_parquet_shards(shard_paths, parquet_path)

    jsonl_path = output_dir / "publication.jsonl" if write_debug_jsonl else None
    if jsonl_path is not None:
        write_jsonl(jsonl_path, iter_all_shard_rows(shard_paths))

    id_stats = {"publication_v1": {"total_docs": 0, "hashed_id_count": 0}}
    bulk_path = output_dir / "publication.bulk.ndjson"
    write_bulk(bulk_path, "publication_v1", "publication_id", iter_all_shard_rows(shard_paths), id_stats)

    return {
        "shard_count": len(shard_paths),
        "with_embedding": with_embedding,
        "without_embedding": without_embedding,
        "parquet_path": parquet_path,
        "jsonl_path": jsonl_path,
        "bulk_path": bulk_path,
        "id_stats": id_stats,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Embed silver publications with checkpointed streaming writes."
    )
    parser.add_argument("--silver-dir", required=True, help="Silver output directory")
    parser.add_argument("--output-dir", required=True, help="Vector output directory")
    parser.add_argument(
        "--env-file",
        default=str(Path(__file__).resolve().parent / ".env"),
        help="Path to .env containing DASHSCOPE_API_KEY",
    )
    parser.add_argument(
        "--base-url",
        default=None,
        help="DashScope OpenAI-compatible embeddings base URL "
        "(default: DASHSCOPE_EMBEDDING_BASE_URL or legacy dashscope.aliyuncs.com/compatible-mode/v1)",
    )
    parser.add_argument("--force-reembed", action="store_true", help="Ignore checkpointed embeddings")
    parser.add_argument(
        "--strict-vector",
        action="store_true",
        help="Fail the stage when embed failures exceed threshold or no embeddings exist",
    )
    parser.add_argument(
        "--write-debug-jsonl",
        action="store_true",
        help="Also write publication.jsonl alongside parquet",
    )
    parser.add_argument(
        "--fail-rate-threshold",
        type=float,
        default=0.05,
        help="With --strict-vector, fail when failed_embed/input_rows exceeds this ratio",
    )
    parser.add_argument(
        "--shard-size",
        type=int,
        default=DEFAULT_SHARD_SIZE,
        help="Publication rows per parquet shard during materialize",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    silver_dir = Path(args.silver_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    checkpoint_dir = output_dir / "_checkpoint"
    shard_dir = output_dir / "shards"
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    progress_path = checkpoint_dir / "progress.json"
    failures_path = output_dir / "vector_failures.jsonl"
    checkpoint_db = checkpoint_dir / "embeddings.sqlite3"

    load_dotenv_file(Path(args.env_file))
    api_key = os.getenv("DASHSCOPE_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("DASHSCOPE_API_KEY is missing; set it in .env or the process environment")

    base_url = (
        (args.base_url or "").strip()
        or os.getenv("DASHSCOPE_EMBEDDING_BASE_URL", "").strip()
        or DEFAULT_BASE_URL
    )

    silver_path, input_format = resolve_silver_publication(silver_dir)
    client = DashScopeEmbeddingClient(api_key=api_key, base_url=base_url)
    checkpoint = EmbeddingCheckpoint(checkpoint_db)

    try:
        if args.force_reembed:
            checkpoint.clear()
            if failures_path.exists():
                failures_path.unlink()
            if shard_dir.exists():
                for old in shard_dir.glob("shard_*.parquet"):
                    old.unlink()
        elif checkpoint.count() == 0:
            # Bootstrap from a previous successful publication.parquet if present.
            bootstrapped = checkpoint.import_from_publication_file(output_dir / "publication.parquet")
            if bootstrapped:
                write_progress(
                    progress_path,
                    {
                        "phase": "bootstrap",
                        "updated_at": now_iso(),
                        "bootstrapped_embeddings": bootstrapped,
                    },
                )

        # Fresh failures file each full run unless resuming mid-embed with existing file.
        if args.force_reembed or not failures_path.exists():
            ensure_parent(failures_path)
            failures_path.write_text("", encoding="utf-8")

        embed_stats = run_embed_phase(
            silver_path=silver_path,
            input_format=input_format,
            client=client,
            checkpoint=checkpoint,
            failures_path=failures_path,
            progress_path=progress_path,
            force_reembed=bool(args.force_reembed),
        )

        materialize = run_materialize_phase(
            silver_path=silver_path,
            input_format=input_format,
            checkpoint=checkpoint,
            shard_dir=shard_dir,
            shard_size=max(1, int(args.shard_size)),
            output_dir=output_dir,
            write_debug_jsonl=bool(args.write_debug_jsonl),
            progress_path=progress_path,
            embed_stats=embed_stats,
        )
    finally:
        checkpoint.close()

    stats = dict(embed_stats)
    stats["with_embedding"] = int(materialize["with_embedding"])
    stats["without_embedding"] = int(materialize["without_embedding"])
    fail_rate = (stats["failed_embed"] / stats["input_rows"]) if stats["input_rows"] else 0.0

    parquet_path = Path(str(materialize["parquet_path"]))
    jsonl_path = materialize["jsonl_path"]
    bulk_path = Path(str(materialize["bulk_path"]))
    id_stats = materialize["id_stats"]

    summary = {
        "finished_at": now_iso(),
        "silver_dir": str(silver_dir.resolve()),
        "output_dir": str(output_dir.resolve()),
        "input_format": input_format,
        "model": EMBEDDING_MODEL,
        "dimension": EMBEDDING_DIMENSION,
        "force_reembed": bool(args.force_reembed),
        "strict_vector": bool(args.strict_vector),
        "shard_size": int(args.shard_size),
        "checkpoint_db": str(checkpoint_db.resolve()),
        "stats": stats,
        "fail_rate": round(fail_rate, 6),
        "outputs": {
            "publication_parquet": str(parquet_path),
            "publication_jsonl": str(jsonl_path) if jsonl_path else None,
            "publication_bulk": str(bulk_path),
            "vector_failures_jsonl": str(failures_path),
            "shard_dir": str(shard_dir.resolve()),
            "shard_count": materialize["shard_count"],
        },
        "output_bytes": {
            "publication_parquet": file_size_or_none(parquet_path),
            "publication_jsonl": file_size_or_none(Path(str(jsonl_path)) if jsonl_path else None),
            "publication_bulk": file_size_or_none(bulk_path),
            "vector_failures_jsonl": file_size_or_none(failures_path),
            "checkpoint_db": file_size_or_none(checkpoint_db),
        },
        "id_safety": id_stats,
    }
    with open(output_dir / "vector_summary.json", "w", encoding="utf-8") as handle:
        json.dump(summary, handle, ensure_ascii=False, indent=2)
    write_progress(
        progress_path,
        {"phase": "done", "updated_at": now_iso(), "summary": summary},
    )

    if args.strict_vector:
        if stats["input_rows"] > 0 and stats["with_embedding"] == 0:
            raise RuntimeError("strict-vector: no publications received embeddings")
        if fail_rate > float(args.fail_rate_threshold):
            raise RuntimeError(
                f"strict-vector: embed fail_rate {fail_rate:.4f} exceeds threshold {args.fail_rate_threshold}"
            )

    print(
        json.dumps(
            {
                "ok": True,
                "stats": stats,
                "output_dir": str(output_dir),
                "checkpoint_count": stats.get("reused_embeddings", 0) + stats.get("embedded", 0),
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
