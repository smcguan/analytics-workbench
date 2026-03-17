# Analytics Workbench — Claude Code Context

## What This Project Is

Analytics Workbench is an AI-assisted analytics desktop application.
It is a FastAPI backend + static HTML/JS frontend packaged as a local desktop app.
The AI layer uses OpenAI (GPT-4.1-mini by default) for SQL generation and question suggestions.
DuckDB is the query engine — all SQL runs against local Parquet files.

The product goal is an AI-assisted insight workflow:
Open file → inspect dataset → ask question → generate SQL → review/edit → run SQL → see table + chart

## Current Milestone

Milestone 3 — complete as of March 2026.
All acceptance criteria met. Next work is Milestone 4 (Insights view).

## Repository Structure

```
/
├── frontend/
│   └── index.html              # Entire frontend — single HTML/JS/CSS file
├── backend/
│   └── app/
│       ├── main.py             # FastAPI app, all core endpoints
│       ├── ai/
│       │   ├── routes.py       # /api/ai/* endpoints (generate_sql, suggest_questions)
│       │   ├── provider_openai.py  # OpenAI prompts and API calls
│       │   ├── context_builder.py  # Builds dataset context for AI prompts
│       │   ├── response_parser.py  # Parses AI JSON responses
│       │   ├── schemas.py      # Pydantic request/response models for AI routes
│       │   └── sql_validator.py    # Validates AI SQL with DuckDB EXPLAIN
│       └── services/
│           ├── dataset_import.py   # Import pipeline: CSV/TSV/Excel/Parquet → Parquet
│           └── chart_recommender.py # Deterministic chart type recommendation
├── data/
│   └── datasets/               # Imported datasets live here
│       └── <dataset_name>/
│           ├── source.parquet  # Canonical internal Parquet file
│           ├── metadata.json   # Full import metadata (written by dataset_import.py)
│           └── _meta.json      # Lightweight summary (for fast cache reads)
├── exports/                    # SQL export outputs (xlsx, tsv)
├── .env                        # OPENAI_API_KEY and other config (not committed)
└── CLAUDE.md                   # This file
```

## Key Commands

```bash
# Start the backend (from repo root)
uvicorn backend.app.main:app --reload --port 8000

# The frontend is served at:
http://localhost:8000/ui/

# Run a quick health check
curl http://localhost:8000/api/health
```

## Environment Variables

```
OPENAI_API_KEY=sk-...           # Required for AI features
OPENAI_MODEL=gpt-4.1-mini      # Default model (can override)
AW_DATASETS_DIR=...            # Override datasets directory (absolute path)
AW_EXPORTS_DIR=...             # Override exports directory
AW_CONTEXT_SAMPLE_ROWS=100000  # Rows sampled for AI context on large datasets
AW_DEFAULT_PREVIEW_ROWS=50
AW_MAX_PREVIEW_ROWS=200
AW_MAX_EXPORT_ROWS=200000
```

## Backend Architecture

### main.py responsibilities
- All non-AI API endpoints
- Dataset discovery (list_datasets, dataset_source_path)
- SQL execution and rewriting (FROM dataset → FROM read_parquet('...'))
- Profile / schema / preview endpoints
- Import endpoint (delegates to dataset_import.py)
- Chart recommendation (delegates to chart_recommender.py)
- Dataset delete endpoint

### Critical design decisions in main.py
- DATASETS_DIR is always resolved as absolute at startup — never use relative paths
- dataset_source_path() checks for source.parquet first, then glob *.parquet
- _build_dataset_context() uses USING SAMPLE {_CONTEXT_SAMPLE_ROWS} ROWS on all stat queries
- Use DESCRIBE SELECT * instead of parquet_schema() for column counts everywhere
- Both _meta.json and metadata.json are checked in all metadata read paths

### AI routes (routes.py)
- POST /api/ai/generate_sql — main AI SQL generation
- GET /api/ai/suggest_questions — AI-generated question chips
- The generate_sql endpoint accepts both "question" and "prompt" fields
- Falls back gracefully on all exceptions — never crashes the API
- DuckDB semantic validation runs via EXPLAIN before returning SQL

### dataset_import.py
- Supported types: .parquet, .csv, .tsv, .xlsx, .xls
- Parquet import: schema-only validation + shutil.copy2 (no full read/write — critical for large files)
- inspect_parquet uses pq.read_metadata() footer only — not pq.read_table()
- write_metadata() writes both metadata.json and _meta.json
- import_dataset() accepts overwrite=True to replace existing datasets

### chart_recommender.py
- Pure Python, no FastAPI or DuckDB dependencies
- Input: columns (list), rows (list of dicts)
- Returns: recommendation dict with recommended, chart_type, x_column, y_column, title, reason
- Bar chart: 2 cols, categorical x, numeric y, 2-50 rows
- Line chart: 2 cols, datetime x, numeric y, 2+ rows
- Everything else: recommended=False with plain-English reason

## Frontend Architecture

Single file: frontend/index.html

### Key JS globals
- selectedDataset — currently active dataset name
- datasets — array of dataset objects loaded this session
- lastRun — last SQL execution result including visualization block
- _chartInstance — active Chart.js instance (must call clearChart() before new render)
- suggestionsVisible — tracks suggestions popover open/closed state

### Critical frontend behaviors (frozen — do not change without reason)
- Import uses <label for="file-input"> pattern, NOT programmatic .click()
- Inspect Dataset header is a fixed 7-slot CSS grid — does NOT scroll with data
- Only the inspect data window below scrolls horizontally
- Profile/Schema/Preview are true toggles — click again to hide
- Results block row layout is frozen:
  - Row 1: Run SQL | Explain | execution metadata
  - Row 2: RESULTS label | row count | Table tab | Chart tab | Export Excel | Export TSV
  - Row 3: table or chart content
- Export Excel and Export TSV are direct buttons — no Export tab
- Nav order: Insights → Query → Saved Queries
- App starts on Insights view
- Prompt and SQL editor clear when switching datasets
- SQL editor clears when suggestion chip is clicked

### Chart rendering
- Chart.js 4.4.1 loaded from cdnjs CDN
- renderChart(visualization, columns, rows) — renders from lastRun.visualization
- clearChart() — destroys Chart.js instance, must call before new render
- Chart tab renders on click, not automatically after SQL run
- No auto-switch to Chart tab — user clicks it explicitly

### SQL generation fallback chain (frontend)
1. POST /api/ai/generate_sql with {dataset, question, file_type, ...}
2. POST /api/ai/generate_sql with {dataset, prompt, file_type, ...}
3. POST /api/sql/generate with {dataset, prompt, file_type, ...}
4. POST /api/sql/generate with {dataset, question, file_type, ...}
If all fail: populate editor with SELECT * FROM dataset LIMIT 100

## API Endpoints

```
GET  /api/version
GET  /api/health
GET  /api/datasets
GET  /api/datasets/{name}/meta
POST /api/datasets/{name}/delete
POST /api/datasets/import          ?overwrite=true supported
GET  /api/schema?dataset=
GET  /api/preview?dataset=
GET  /api/profile?dataset=
POST /api/sql
POST /api/sql/generate             (schema-aware fallback, no AI)
POST /api/sql/export
GET  /api/queries
POST /api/queries/save
POST /api/queries/delete
POST /api/ai/generate_sql
GET  /api/ai/suggest_questions?dataset=
POST /api/shutdown
```

## /api/sql Response Shape

```json
{
  "columns": ["region", "revenue"],
  "rows": [{"region": "West", "revenue": 120000}],
  "rowcount": 3,
  "elapsed_seconds": 0.042,
  "visualization": {
    "recommended": true,
    "chart_type": "bar",
    "x_column": "region",
    "y_column": "revenue",
    "title": "Revenue by Region",
    "reason": "categorical x-axis with numeric measure"
  }
}
```

## SQL Execution Convention

All SQL runs against Parquet files via DuckDB.
The frontend writes SQL referencing the logical table name `dataset`.
main.py rewrites `FROM dataset` → `FROM read_parquet('/absolute/path/source.parquet')`
before execution. The AI is instructed to use `dataset` as the table name.

## AI Prompt Notes

provider_openai.py contains two prompts:

**SQL generation prompt** — key instructions:
- DuckDB syntax only (not MySQL/SQLite)
- strftime('%Y-%m', col) — format string FIRST (opposite of SQLite)
- DATE_TRUNC('month', col) for date bucketing
- col::INTEGER for type casting
- Never invent column names — only use columns listed in context
- Table name is always `dataset`
- No semicolon at end of query

**Suggest questions prompt** — generates schema-grounded analytical questions.
No changes needed here — working well.

## Known Gaps / Placeholder Features

- Insights view — nav item exists, no backend implementation yet (Milestone 4)
- Explain button — placeholder toast only
- Suggestions button — working but relies on AI; fails gracefully if AI unavailable
- Chart view — limited to exactly 2-column results by design (Milestone 3 scope)
- sql_validator.py (DuckDB EXPLAIN step) — not reviewed; may over-reject valid SQL

## Coding Conventions

- Python: type hints throughout, dataclasses for data models
- Error handling: always catch and return HTTP 400/404 with detail text — never bare 500s
- Logging: use logger.info/warning/exception — never print()
- All file paths: resolve() to absolute before use
- Frontend JS: vanilla JS only — no framework
- Frontend CSS: CSS custom properties (--bg0, --accent, etc.) — match existing palette
- Comments: explain WHY not just what, especially for non-obvious DuckDB behavior

## Performance Notes

- Large Parquet files (100M+ rows): import is fast because we copy bytes, not data
- Context building for Suggestions/Profile samples 100K rows — accurate enough for AI
- SQL execution on 220M rows is inherently slow — DuckDB is fast but physics applies
- Row counts always come from Parquet footer metadata, not COUNT(*) queries on import

## What NOT To Do

- Do not use parquet_schema() for column counts — use DESCRIBE SELECT * instead
- Do not call loadDatasets() inside the import polling loop — it restores all backend datasets
- Do not use relative paths for DATASETS_DIR — always .resolve() to absolute
- Do not add framework dependencies (React, Vue, etc.) to the frontend
- Do not auto-switch to Chart tab after SQL run — user clicks it explicitly
- Do not expose the internal Parquet conversion to users — import is a single step
