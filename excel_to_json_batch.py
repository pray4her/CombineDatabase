import argparse
import json
import os
import shutil
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, TextIO, Tuple

import pandas as pd

from pipeline_storage import iter_parquet_rows, require_pyarrow, write_jsonl


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def remove_tree(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)


def resolve_worker_count(value: object, max_items: Optional[int] = None) -> int:
    text = str(value).strip().lower()
    if text == "auto":
        cpu = os.cpu_count() or 1
        workers = max(1, min(8, cpu - 1))
    else:
        workers = max(1, int(text))
    if max_items is not None:
        workers = min(workers, max(1, max_items))
    return workers


def is_excel_file(path: Path) -> bool:
    return path.suffix.lower() in {".xlsx", ".xlsm", ".xls"}


def is_csv_file(path: Path) -> bool:
    return path.suffix.lower() == ".csv"


def read_csv_frame(input_file: Path) -> pd.DataFrame:
    # Try the common UTF-8 variants first and fall back to permissive decoding for legacy exports.
    for encoding in ("utf-8-sig", "utf-8", "gb18030", "latin1"):
        try:
            return pd.read_csv(str(input_file), dtype=object, encoding=encoding, low_memory=False)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(str(input_file), dtype=object, encoding_errors="replace", low_memory=False)


def read_tabular_sheets(input_file: Path) -> Dict[str, pd.DataFrame]:
    if is_excel_file(input_file):
        return pd.read_excel(str(input_file), sheet_name=None, dtype=object)
    if is_csv_file(input_file):
        return {"Sheet1": read_csv_frame(input_file)}
    raise ValueError(f"unsupported input format: {input_file}")


def read_csv_columns(input_file: Path) -> List[str]:
    for encoding in ("utf-8-sig", "utf-8", "gb18030", "latin1"):
        try:
            return [str(col) for col in pd.read_csv(str(input_file), dtype=object, encoding=encoding, nrows=0).columns]
        except UnicodeDecodeError:
            continue
    return [str(col) for col in pd.read_csv(str(input_file), dtype=object, encoding_errors="replace", nrows=0).columns]


def read_tabular_sheet_columns(input_file: Path) -> Dict[str, List[str]]:
    if is_excel_file(input_file):
        excel = pd.ExcelFile(str(input_file))
        return {
            str(sheet_name): [str(col) for col in pd.read_excel(str(input_file), sheet_name=sheet_name, dtype=object, nrows=0).columns]
            for sheet_name in excel.sheet_names
        }
    if is_csv_file(input_file):
        return {"Sheet1": read_csv_columns(input_file)}
    raise ValueError(f"unsupported input format: {input_file}")


def iter_tabular_sheet_rows(input_file: Path, source_group: str) -> Iterator[Dict[str, object]]:
    converted_at = now_iso()
    source_file = str(input_file.resolve())
    sheets = read_tabular_sheets(input_file)
    for sheet_name, df in sheets.items():
        cleaned = df.where(pd.notna(df), None)
        rows = cleaned.to_dict(orient="records")
        for row_index, row in enumerate(rows, start=1):
            yield {
                "source_file": source_file,
                "source_group": source_group,
                "converted_at": converted_at,
                "sheet": sheet_name,
                "row_index": row_index,
                "row": row,
            }


def normalize_bronze_value(value: object) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        cleaned = value.strip()
        return cleaned if cleaned != "" else None
    return str(value)


def discover_group_columns(input_files: Iterable[Path]) -> List[str]:
    ordered_columns: List[str] = []
    seen = set()
    for input_file in input_files:
        for columns in read_tabular_sheet_columns(input_file).values():
            for column in columns:
                if column not in seen:
                    seen.add(column)
                    ordered_columns.append(column)
    return ordered_columns


def iter_tabular_sheet_rows_flat(
    input_file: Path,
    source_group: str,
    ordered_columns: List[str],
) -> Iterator[Dict[str, object]]:
    for record in iter_tabular_sheet_rows(input_file, source_group):
        row = record.get("row", {})
        output = {
            "source_file": record["source_file"],
            "source_group": record["source_group"],
            "converted_at": record["converted_at"],
            "sheet": record["sheet"],
            "row_index": int(record["row_index"]),
        }
        for column in ordered_columns:
            output[column] = normalize_bronze_value(row.get(column))
        yield output


def tabular_to_json(input_file: Path, output_json: Path, source_group: str) -> Dict[str, object]:
    sheets = read_tabular_sheets(input_file)
    payload = {
        "source_file": str(input_file.resolve()),
        "source_group": source_group,
        "converted_at": now_iso(),
        "sheets": {},
    }

    total_rows = 0
    for sheet_name, df in sheets.items():
        cleaned = df.where(pd.notna(df), None)
        rows = cleaned.to_dict(orient="records")
        payload["sheets"][sheet_name] = rows
        total_rows += len(rows)

    ensure_parent(output_json)
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, default=str)

    return {
        "input_file": str(input_file.resolve()),
        "output_file": str(output_json.resolve()),
        "source_group": source_group,
        "sheet_count": len(payload["sheets"]),
        "total_rows": total_rows,
    }


def write_tabular_rows_jsonl(input_file: Path, writer: TextIO, source_group: str) -> Dict[str, object]:
    sheet_names = set()
    row_count = 0
    for record in iter_tabular_sheet_rows(input_file, source_group):
        writer.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
        row_count += 1
        sheet_names.add(str(record["sheet"]))

    return {
        "input_file": str(input_file.resolve()),
        "source_group": source_group,
        "sheet_count": len(sheet_names),
        "total_rows": row_count,
    }


def write_tabular_rows_jsonl_debug(input_file: Path, writer: TextIO, source_group: str) -> Dict[str, object]:
    summary = write_tabular_rows_jsonl(input_file, writer, source_group)
    summary["debug_jsonl_written"] = True
    return summary


def write_tabular_rows_jsonl_file(input_file: Path, output_file: Path, source_group: str) -> Dict[str, object]:
    ensure_parent(output_file)
    with open(output_file, "w", encoding="utf-8") as writer:
        summary = write_tabular_rows_jsonl(input_file, writer, source_group)
    summary["output_file"] = str(output_file.resolve())
    return summary


def write_tabular_rows_parquet(
    input_file: Path,
    writer: "pq.ParquetWriter",
    source_group: str,
    ordered_columns: List[str],
    schema: "pa.Schema",
    debug_writer: Optional[TextIO] = None,
) -> Dict[str, object]:
    require_pyarrow()
    import pyarrow as pa

    sheet_names = set()
    row_count = 0
    buffer: List[Dict[str, object]] = []
    for record in iter_tabular_sheet_rows(input_file, source_group):
        if debug_writer is not None:
            debug_writer.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
        flat = {
            "source_file": record["source_file"],
            "source_group": record["source_group"],
            "converted_at": record["converted_at"],
            "sheet": record["sheet"],
            "row_index": int(record["row_index"]),
        }
        row = record.get("row", {})
        for column in ordered_columns:
            flat[column] = normalize_bronze_value(row.get(column))
        buffer.append(flat)
        row_count += 1
        sheet_names.add(str(record["sheet"]))
        if len(buffer) >= 2048:
            writer.write_table(pa.Table.from_pylist(buffer, schema=schema))
            buffer = []

    if buffer:
        writer.write_table(pa.Table.from_pylist(buffer, schema=schema))

    return {
        "input_file": str(input_file.resolve()),
        "source_group": source_group,
        "sheet_count": len(sheet_names),
        "total_rows": row_count,
    }


def write_tabular_rows_parquet_file(
    input_file: Path,
    output_file: Path,
    source_group: str,
    ordered_columns: List[str],
    write_debug_jsonl: bool = False,
) -> Dict[str, object]:
    require_pyarrow()
    import pyarrow as pa
    import pyarrow.parquet as pq

    ensure_parent(output_file)
    schema = pa.schema(
        [
            pa.field("source_file", pa.string()),
            pa.field("source_group", pa.string()),
            pa.field("converted_at", pa.string()),
            pa.field("sheet", pa.string()),
            pa.field("row_index", pa.int64()),
        ]
        + [pa.field(column, pa.string()) for column in ordered_columns]
    )
    debug_output = output_file.with_suffix(".jsonl") if write_debug_jsonl else None
    debug_handle: Optional[TextIO] = None
    try:
        if debug_output is not None:
            ensure_parent(debug_output)
            debug_handle = open(debug_output, "w", encoding="utf-8")
        with pq.ParquetWriter(output_file, schema=schema, compression="zstd", use_dictionary=True) as writer:
            summary = write_tabular_rows_parquet(
                input_file,
                writer,
                source_group,
                ordered_columns,
                schema,
                debug_handle,
            )
    finally:
        if debug_handle is not None:
            debug_handle.close()
    summary["output_file"] = str(output_file.resolve())
    summary["debug_jsonl"] = str(debug_output.resolve()) if debug_output else None
    return summary


def _jsonl_shard_worker(input_file_text: str, output_file_text: str, source_group: str) -> Dict[str, object]:
    return write_tabular_rows_jsonl_file(Path(input_file_text), Path(output_file_text), source_group)


def _parquet_shard_worker(
    input_file_text: str,
    output_file_text: str,
    source_group: str,
    ordered_columns: List[str],
    write_debug_jsonl: bool,
) -> Dict[str, object]:
    return write_tabular_rows_parquet_file(
        Path(input_file_text),
        Path(output_file_text),
        source_group,
        ordered_columns,
        write_debug_jsonl=write_debug_jsonl,
    )


def _merge_jsonl_shards(shard_files: List[Path], output_file: Path) -> None:
    tmp_output = output_file.with_suffix(output_file.suffix + ".tmp")
    ensure_parent(output_file)
    with open(tmp_output, "w", encoding="utf-8") as writer:
        for shard_file in shard_files:
            with open(shard_file, "r", encoding="utf-8") as handle:
                for line in handle:
                    writer.write(line)
    tmp_output.replace(output_file)


def _merge_parquet_shards(
    shard_files: List[Path],
    output_file: Path,
    ordered_columns: List[str],
) -> None:
    require_pyarrow()
    import pyarrow as pa
    import pyarrow.parquet as pq

    ensure_parent(output_file)
    tmp_output = output_file.with_suffix(output_file.suffix + ".tmp")
    schema = pa.schema(
        [
            pa.field("source_file", pa.string()),
            pa.field("source_group", pa.string()),
            pa.field("converted_at", pa.string()),
            pa.field("sheet", pa.string()),
            pa.field("row_index", pa.int64()),
        ]
        + [pa.field(column, pa.string()) for column in ordered_columns]
    )
    buffer: List[Dict[str, object]] = []
    with pq.ParquetWriter(tmp_output, schema=schema, compression="zstd", use_dictionary=True) as writer:
        for shard_file in shard_files:
            for row in iter_parquet_rows(shard_file):
                buffer.append(row)
                if len(buffer) >= 2048:
                    writer.write_table(pa.Table.from_pylist(buffer, schema=schema))
                    buffer = []
        if buffer:
            writer.write_table(pa.Table.from_pylist(buffer, schema=schema))
    tmp_output.replace(output_file)


def _parallel_map(
    max_workers: int,
    jobs: List[Tuple],
    worker_fn,
) -> Tuple[List[Dict[str, object]], List[Dict[str, object]]]:
    results_by_index: Dict[int, Dict[str, object]] = {}
    errors: List[Dict[str, object]] = []
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(worker_fn, *job_args): idx
            for idx, job_args in enumerate(jobs)
        }
        for future in as_completed(future_map):
            idx = future_map[future]
            try:
                results_by_index[idx] = future.result()
            except Exception as exc:
                job_args = jobs[idx]
                input_file = str(job_args[0]) if job_args else "unknown"
                errors.append(
                    {
                        "input_file": input_file,
                        "error": str(exc),
                        "traceback": traceback.format_exc(),
                    }
                )
    ordered_results = [results_by_index[idx] for idx in sorted(results_by_index)]
    return ordered_results, errors


def iter_tabular_files(root: Path, extensions: Tuple[str, ...]) -> Iterable[Path]:
    ext_lower = tuple(x.lower() for x in extensions)
    for p in root.rglob("*"):
        if p.is_file() and p.name.startswith("~$"):
            continue
        if p.is_file() and p.suffix.lower() in ext_lower:
            yield p


def convert_tree(root: Path, output_root: Path, source_group: str, extensions: Tuple[str, ...]) -> Dict[str, object]:
    results: List[Dict[str, object]] = []
    errors: List[Dict[str, object]] = []
    for input_file in iter_tabular_files(root, extensions):
        rel = input_file.relative_to(root)
        out = output_root / source_group / rel.parent / f"{input_file.stem}.json"
        try:
            results.append(tabular_to_json(input_file, out, source_group))
        except Exception as exc:
            errors.append(
                {
                    "input_file": str(input_file.resolve()),
                    "error": str(exc),
                    "traceback": traceback.format_exc(),
                }
            )
    return {
        "source_group": source_group,
        "root": str(root.resolve()),
        "converted_count": len(results),
        "error_count": len(errors),
        "results": results,
        "errors": errors,
    }


def convert_tree_to_jsonl(
    root: Path,
    output_file: Path,
    source_group: str,
    extensions: Tuple[str, ...],
) -> Dict[str, object]:
    results: List[Dict[str, object]] = []
    errors: List[Dict[str, object]] = []
    tmp_output = output_file.with_suffix(output_file.suffix + ".tmp")
    ensure_parent(output_file)

    with open(tmp_output, "w", encoding="utf-8") as writer:
        for input_file in iter_tabular_files(root, extensions):
            try:
                results.append(write_tabular_rows_jsonl(input_file, writer, source_group))
            except Exception as exc:
                errors.append(
                    {
                        "input_file": str(input_file.resolve()),
                        "error": str(exc),
                        "traceback": traceback.format_exc(),
                    }
                )

    tmp_output.replace(output_file)

    return {
        "source_group": source_group,
        "root": str(root.resolve()),
        "output_file": str(output_file.resolve()),
        "converted_count": len(results),
        "error_count": len(errors),
        "row_total": sum(int(item["total_rows"]) for item in results),
        "results": results,
        "errors": errors,
    }


def convert_files_to_jsonl(
    input_files: Iterable[Path],
    output_file: Path,
    source_group: str,
    workers: int = 1,
    tmp_dir: Optional[Path] = None,
) -> Dict[str, object]:
    files = sorted(Path(path) for path in input_files)
    results: List[Dict[str, object]] = []
    errors: List[Dict[str, object]] = []
    effective_tmp_dir = tmp_dir or (output_file.parent / "_tmp" / source_group)
    if workers <= 1 or len(files) <= 1:
        tmp_output = output_file.with_suffix(output_file.suffix + ".tmp")
        ensure_parent(output_file)
        with open(tmp_output, "w", encoding="utf-8") as writer:
            for input_file in files:
                try:
                    results.append(write_tabular_rows_jsonl(input_file, writer, source_group))
                except Exception as exc:
                    errors.append(
                        {
                            "input_file": str(input_file.resolve()),
                            "error": str(exc),
                            "traceback": traceback.format_exc(),
                        }
                    )
        tmp_output.replace(output_file)
    else:
        remove_tree(effective_tmp_dir)
        effective_tmp_dir.mkdir(parents=True, exist_ok=True)
        jobs = []
        shard_files: List[Path] = []
        for idx, input_file in enumerate(files):
            shard_file = effective_tmp_dir / f"part-{idx:04d}.rows.jsonl"
            shard_files.append(shard_file)
            jobs.append((str(input_file.resolve()), str(shard_file.resolve()), source_group))
        results, errors = _parallel_map(workers, jobs, _jsonl_shard_worker)
        if not errors:
            _merge_jsonl_shards(shard_files, output_file)
            remove_tree(effective_tmp_dir)

    roots = sorted({str(path.parent.resolve()) for path in files})
    return {
        "source_group": source_group,
        "roots": roots,
        "output_file": str(output_file.resolve()),
        "tmp_dir": str(effective_tmp_dir.resolve()) if workers > 1 and len(files) > 1 else None,
        "workers": workers,
        "converted_count": len(results),
        "error_count": len(errors),
        "row_total": sum(int(item["total_rows"]) for item in results),
        "results": results,
        "errors": errors,
    }


def convert_files_to_parquet(
    input_files: Iterable[Path],
    output_file: Path,
    source_group: str,
    write_debug_jsonl: bool = False,
    workers: int = 1,
    tmp_dir: Optional[Path] = None,
) -> Dict[str, object]:
    require_pyarrow()
    files = sorted(Path(path) for path in input_files)
    results: List[Dict[str, object]] = []
    errors: List[Dict[str, object]] = []
    ordered_columns = discover_group_columns(files)
    schema_column_count = 5 + len(ordered_columns)
    debug_output = output_file.with_suffix(".jsonl") if write_debug_jsonl else None
    effective_tmp_dir = tmp_dir or (output_file.parent / "_tmp" / source_group)
    if workers <= 1 or len(files) <= 1:
        import pyarrow as pa
        import pyarrow.parquet as pq

        ensure_parent(output_file)
        tmp_output = output_file.with_suffix(output_file.suffix + ".tmp")
        debug_tmp = debug_output.with_suffix(debug_output.suffix + ".tmp") if debug_output else None
        schema = pa.schema(
            [
                pa.field("source_file", pa.string()),
                pa.field("source_group", pa.string()),
                pa.field("converted_at", pa.string()),
                pa.field("sheet", pa.string()),
                pa.field("row_index", pa.int64()),
            ]
            + [pa.field(column, pa.string()) for column in ordered_columns]
        )

        debug_handle: Optional[TextIO] = None
        try:
            if debug_tmp is not None:
                ensure_parent(debug_tmp)
                debug_handle = open(debug_tmp, "w", encoding="utf-8")
            with pq.ParquetWriter(tmp_output, schema=schema, compression="zstd", use_dictionary=True) as writer:
                for input_file in files:
                    try:
                        results.append(
                            write_tabular_rows_parquet(
                                input_file,
                                writer,
                                source_group,
                                ordered_columns,
                                schema,
                                debug_handle,
                            )
                        )
                    except Exception as exc:
                        errors.append(
                            {
                                "input_file": str(input_file.resolve()),
                                "error": str(exc),
                                "traceback": traceback.format_exc(),
                            }
                        )
        finally:
            if debug_handle is not None:
                debug_handle.close()

        tmp_output.replace(output_file)
        if debug_tmp is not None and debug_output is not None:
            debug_tmp.replace(debug_output)
    else:
        remove_tree(effective_tmp_dir)
        effective_tmp_dir.mkdir(parents=True, exist_ok=True)
        jobs = []
        shard_files: List[Path] = []
        debug_shards: List[Path] = []
        for idx, input_file in enumerate(files):
            shard_file = effective_tmp_dir / f"part-{idx:04d}.rows.parquet"
            shard_files.append(shard_file)
            if write_debug_jsonl:
                debug_shards.append(shard_file.with_suffix(".jsonl"))
            jobs.append(
                (
                    str(input_file.resolve()),
                    str(shard_file.resolve()),
                    source_group,
                    ordered_columns,
                    write_debug_jsonl,
                )
            )
        results, errors = _parallel_map(workers, jobs, _parquet_shard_worker)
        if not errors:
            _merge_parquet_shards(shard_files, output_file, ordered_columns)
            if write_debug_jsonl and debug_output is not None:
                _merge_jsonl_shards(debug_shards, debug_output)
            remove_tree(effective_tmp_dir)

    roots = sorted({str(path.parent.resolve()) for path in files})
    return {
        "source_group": source_group,
        "roots": roots,
        "output_file": str(output_file.resolve()),
        "debug_jsonl": str(debug_output.resolve()) if debug_output else None,
        "tmp_dir": str(effective_tmp_dir.resolve()) if workers > 1 and len(files) > 1 else None,
        "workers": workers,
        "schema_column_count": schema_column_count,
        "row_column_count": len(ordered_columns),
        "converted_count": len(results),
        "error_count": len(errors),
        "row_total": sum(int(item["total_rows"]) for item in results),
        "results": results,
        "errors": errors,
    }


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Batch convert Excel files to JSON or JSONL files.")
    p.add_argument("--scholars-root", required=True, help="Root directory for scholars Excel files")
    p.add_argument("--frontiers-root", required=True, help="Root directory for frontiers Excel files")
    p.add_argument("--output-root", required=True, help="Output root directory for json files")
    p.add_argument(
        "--output-format",
        default="json",
        choices=["json", "jsonl", "parquet"],
        help="Output format. json writes one file per Excel, jsonl/parquet write one file per source group.",
    )
    p.add_argument(
        "--extensions",
        default=".xlsx,.xlsm,.xls,.csv",
        help="Comma-separated tabular extensions, default: .xlsx,.xlsm,.xls,.csv",
    )
    p.add_argument(
        "--write-debug-jsonl",
        action="store_true",
        help="When using parquet output, also write row-oriented debug JSONL.",
    )
    p.add_argument(
        "--workers",
        default="1",
        help="Worker count for per-file conversion when using group JSONL/parquet outputs.",
    )
    return p


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    scholars_root = Path(args.scholars_root)
    frontiers_root = Path(args.frontiers_root)
    output_root = Path(args.output_root)
    output_root.mkdir(parents=True, exist_ok=True)

    extensions = tuple(x.strip().lower() for x in args.extensions.split(",") if x.strip())
    if not scholars_root.exists():
        raise FileNotFoundError(f"scholars root not found: {scholars_root}")
    if not frontiers_root.exists():
        raise FileNotFoundError(f"frontiers root not found: {frontiers_root}")

    if args.output_format == "json":
        scholars_summary = convert_tree(scholars_root, output_root, "scholars", extensions)
        frontiers_summary = convert_tree(frontiers_root, output_root, "frontiers", extensions)
    elif args.output_format == "jsonl":
        scholars_summary = convert_tree_to_jsonl(
            scholars_root,
            output_root / "scholars.rows.jsonl",
            "scholars",
            extensions,
        )
        frontiers_summary = convert_tree_to_jsonl(
            frontiers_root,
            output_root / "frontiers.rows.jsonl",
            "frontiers",
            extensions,
        )
    else:
        workers = resolve_worker_count(args.workers)
        scholars_summary = convert_files_to_parquet(
            iter_tabular_files(scholars_root, extensions),
            output_root / "scholars.rows.parquet",
            "scholars",
            write_debug_jsonl=bool(args.write_debug_jsonl),
            workers=workers,
        )
        frontiers_summary = convert_files_to_parquet(
            iter_tabular_files(frontiers_root, extensions),
            output_root / "frontiers.rows.parquet",
            "frontiers",
            write_debug_jsonl=bool(args.write_debug_jsonl),
            workers=workers,
        )

    summary = {
        "generated_at": now_iso(),
        "output_root": str(output_root.resolve()),
        "output_format": args.output_format,
        "extensions": list(extensions),
        "groups": {
            "scholars": scholars_summary,
            "frontiers": frontiers_summary,
        },
        "converted_total": scholars_summary["converted_count"] + frontiers_summary["converted_count"],
        "error_total": scholars_summary["error_count"] + frontiers_summary["error_count"],
    }

    summary_path = output_root / "excel_to_json_summary.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(json.dumps({k: summary[k] for k in ["converted_total", "error_total"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
