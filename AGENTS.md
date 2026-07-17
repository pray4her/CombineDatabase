# Repository Guidelines

## Project Structure & Module Organization
Core pipeline scripts live at the repository root. `scan_and_queue.py` manages the SQLite manifest and staged jobs in `manifest.db`; `run_pipeline.py` executes `scan -> bronze -> silver -> vector -> match -> index`; `excel_to_json_batch.py` converts Excel sources to JSON or row-oriented JSONL; `etl_transform_schema_v1.py` writes schema-aligned JSONL and bulk NDJSON outputs. The bronze layer is now group-level JSONL (`data_pipeline/bronze/scholars.rows.jsonl` and `data_pipeline/bronze/frontiers.rows.jsonl`) and silver reads those files directly instead of building merged intermediate JSON. After silver, `vector_embed_publications_v1.py` embeds publications (`embedding` knn_vector) via DashScope `qwen3.7-text-embedding`. Schema and index definitions live in `schema_v1.yaml` and `opensearch_mapping_v1.json`. OpenSearch helpers are `create_indices.ps1`, `bulk_import.ps1`, and `docker-compose-opensearch.yml`. Use `_scan_test/` and `_pipeline_test/` as fixture-driven test inputs; generated artifacts belong under `data_pipeline/` or other output folders, not mixed into source areas.

## Build, Test, and Development Commands
Use Python 3.12+ and PowerShell.

```powershell
python scan_and_queue.py scan --db-path manifest.db --scholars-root .\_scan_test\scholars --frontiers-root .\_scan_test\frontiers --enqueue-stages scan,bronze,silver,vector,match,index
python run_pipeline.py --db-path manifest.db --base-dir . --output-root .\data_pipeline
python excel_to_json_batch.py --scholars-root .\_pipeline_test\scholars --frontiers-root .\_pipeline_test\frontiers --output-root .\output_schema_v1_stream --output-format jsonl
python etl_transform_schema_v1.py --scholars-jsonl .\output_schema_v1_stream\scholars.rows.jsonl --frontiers-jsonl .\output_schema_v1_stream\frontiers.rows.jsonl --output-dir .\output_schema_v1_stream
python vector_embed_publications_v1.py --silver-dir .\data_pipeline\silver\<run_id> --output-dir .\data_pipeline\vector\<run_id>
docker compose -f .\docker-compose-opensearch.yml up -d
powershell -ExecutionPolicy Bypass -File .\create_indices.ps1 -Endpoint http://localhost:9200
```

The first command queues work, the second runs the staged pipeline (including publication embedding after silver), the third builds standalone bronze JSONL files for debugging, the fourth runs ETL directly from bronze JSONL, the fifth embeds publications via DashScope, and the last two start and prepare OpenSearch.

## Coding Style & Naming Conventions
Follow the existing Python style: 4-space indentation, `snake_case` for functions and variables, `UPPER_SNAKE_CASE` for constants, and type hints on public helpers. Keep modules single-purpose and prefer `pathlib.Path` over raw path strings. Preserve UTF-8 handling for Chinese source data. PowerShell parameters should stay PascalCase to match current scripts.

## Testing Guidelines
There is no formal `pytest` suite in this snapshot; use fixture-based smoke tests. Validate scan behavior with `_scan_test/` and end-to-end runs with `_pipeline_test/`. After changes, inspect `pipeline_run_summary.json`, `transform_summary.json`, and `quality_report.json` for record counts, type anomalies, and bulk failures; for bronze validation, also inspect `data_pipeline/bronze/*.rows.jsonl`. Name new fixtures by source and intent, for example `_pipeline_test/frontiers/f2.xlsx`.

## Commit & Pull Request Guidelines
Git history is not available in this workspace export, so use short imperative commit subjects such as `pipeline: tighten silver-stage error handling`. Keep commits focused on one stage or script family. PRs should include: change summary, affected commands, sample input/output paths, and screenshots only when Dashboards or OpenSearch UI behavior changes. Link any issue or data ticket and note schema or mapping changes explicitly.

## Security & Configuration Tips
Keep secrets in `.env`; use `.env.opensearch.example` as the template. Do not commit real credentials, large raw datasets, or generated NDJSON unless the repository intentionally tracks fixtures. When changing schema or mappings, update both `schema_v1.yaml` and `opensearch_mapping_v1.json` together.
