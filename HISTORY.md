# Analytics Workbench — History

Archived milestone detail, resolved bugs, and completed backlog items.
Active state lives in CONTEXT_AW.md. Coding guidance lives in CLAUDE.md.

---

## MILESTONE 3 — WHAT WAS BUILT (COMPLETE)

### Dataset Import Pipeline
- CSV, Excel (.xlsx/.xls), TSV, Parquet all supported
- All formats converted to internal Parquet on import
- Parquet import: schema-only validation + raw file copy (fast for 220M row files)
- TSV: pandas read_csv with sep="\t" and version compatibility fallback
- import_dataset() accepts overwrite=True to replace existing datasets
- Writes both metadata.json and _meta.json on every import
- _meta.json enables fast cached metadata reads by all endpoints

### Dataset Management
- POST /api/datasets/{name}/delete endpoint — removes dataset directory
- Refresh Datasets calls delete endpoint for each dataset before clearing UI
- DATASETS_DIR always resolved as absolute path at startup
- Import supports ?overwrite=true query param

### AI SQL Generation
- 4-route fallback chain in frontend:
  1. POST /api/ai/generate_sql with {dataset, question, file_type}
  2. POST /api/ai/generate_sql with {dataset, prompt, file_type}
  3. POST /api/sql/generate with {dataset, prompt, file_type}
  4. POST /api/sql/generate with {dataset, question, file_type}
- /api/sql/generate in main.py: schema-aware fallback, no AI, returns real column names
- AI system prompt tuned for DuckDB syntax (strftime argument order, DATE_TRUNC, casting)
- generate_sql endpoint accepts both "question" and "prompt" fields

### Suggestions Button
- Calls GET /api/ai/suggest_questions
- Results CACHED in dataset_context.json under "suggested_questions" key
- First click: calls OpenAI, caches result; subsequent clicks return from cache
- Popover shows "Cached suggestions — Refresh" link for force regeneration
- If OpenAI fails on refresh, cached suggestions returned as fallback
- Suggestions popover closes when dataset changes

### Inspect Dataset
- Profile, Schema, Preview all working
- Fixed header (7-slot CSS grid) — does NOT scroll with data
- Profile/Schema/Preview are true toggles (click again to hide)
- Uses DESCRIBE SELECT * instead of parquet_schema() for column counts
- Reads both _meta.json and metadata.json in all metadata paths

### Performance for Large Datasets
- Context builder uses USING SAMPLE 100000 ROWS on all stat queries
- Row counts from Parquet footer metadata (pq.read_metadata()), not COUNT(*)
- Parquet import copies raw bytes — no decompression for large files
- AW_CONTEXT_SAMPLE_ROWS env var to tune sample size

### Automatic Charting
- chart_recommender.py: deterministic rules, no AI
- Bar chart: exactly 2 cols, categorical x, numeric y, 2-50 rows
- Line chart: exactly 2 cols, datetime x, numeric y, 2+ rows
- Chart tab renders on user click — no auto-switch
- clearChart() called before every new SQL run

---

## MILESTONE 4 — INSIGHTS VIEW (COMPLETE)

### Core Experience
User imports dataset → Insights tab activates automatically → AI analyzes structure →
3–5 insight cards appear → each card shows finding + chart/table → Explore button
loads SQL into Query tab.

### Insight Object Schema
```json
{
  "type": "concentration",
  "headline": "Top 5 drugs account for 42% of total spending",
  "explanation": "Medicare Part B spending is highly concentrated...",
  "sql": "SELECT Brnd_Name, Tot_Spndng_2023 FROM dataset ORDER BY...",
  "chart_type": "bar",
  "priority": 1
}
```

### Six Insight Types (priority order)
1. Concentration — Pareto / small number drives disproportionate share
2. Outliers — values far outside the norm for their group
3. Trend — fastest-growing and fastest-declining (requires time column)
4. Distribution skew — heavily skewed numeric columns
5. Missing/anomalous data — high null rates, zero values, formatting issues
6. Correlation — two numeric columns that move together

### Implementation
- GET /api/ai/insights?dataset= (?refresh=true to bypass cache)
- Input: column names, types, numeric stats ONLY (no sample rows — privacy)
- Cached in dataset_context.json under "insights" key
- Skeleton loading cards (3 placeholders) shown while AI runs
- 2-column grid in Insights view; empty state if no insights generated

---

## MILESTONE 4 — REFERENCE TABLE JOIN (COMPLETE)

### How It Works
- "+ Reference Table" button in sidebar imports CSV/TSV/XLSX as lightweight Parquet
- Available in SQL as table name "reference" (e.g. JOIN reference ON ...)
- One reference table at a time — importing a new one overwrites the previous
- AI context builder includes reference table column names when generating SQL
- Error: SQL using "reference" without loaded table → HTTP 400 with clear message

### Implementation Files
- dataset_import.py: import_reference_table()
- context_builder.py: build_reference_context()
- main.py: _rewrite_sql_dataset_reference for FROM/JOIN rewriting
- provider_openai.py: SQL prompt includes reference columns when loaded
- routes.py, schemas.py: reference field on GenerateSQLRequest

### Scope Boundary
One primary dataset + one reference table. Not a general multi-dataset engine.

---

## MILESTONE 4 — DATA PRIVACY & AI TRANSPARENCY (COMPLETE)

### Three Components
1. **Schema-only Insights:** provider_openai.py sends ONLY column names, types,
   and aggregate stats — no sample rows, no top values.
2. **Permanent privacy disclosure:** Muted text in Insights view:
   "AI generates analysis instructions from column names and statistics only.
   Your data stays on your computer."
3. **Per-dataset AI consent:** Confirm dialog on import. Stored as "ai_consent"
   in _meta.json. If declined: Insights and Suggestions disabled; SQL still works.
   Exposed via GET /api/datasets/{name}/meta, stored via POST /api/datasets/{name}/ai_consent.

### Framing Rule
  WRONG: "AI analyzes your data"
  RIGHT: "AI generates analysis instructions that run on your computer"

---

## MILESTONE 4 — RESULT PASSPORT (COMPLETE)

### Purpose
Structured per-column profile (top values, stats, null rates, quality flags) for
clipboard sharing with an AI assistant. Zero raw row data included.

### Endpoint
POST /api/results/passport
Request: { "columns": [...], "rows": [...], "sql": "..." }
Response: { row_count, column_count, columns, per_column_profile, data_quality_flags, grain_hint }

### UI
"Copy Result Summary" button in results toolbar Row 2, after Export TSV.
Toast: "Result summary copied — paste into Claude or any AI assistant"

---

## MILESTONE 4 — EXPORT PASSPORT (COMPLETE)

### Purpose
Download a structured JSON file capturing everything needed to write accurate SQL
against a dataset without seeing the data. Designed for upload to Claude or any AI.

### Endpoint
GET /api/datasets/{name}/passport (?refresh=true)
Grain description cached in dataset_context.json under "grain_description" key.

### Passport JSON Structure (9 sections)
1. identity — dataset_name, row_count, column_count, source_file_type, import_date, file_size_bytes
2. schema — per column: data_type, nullable, null stats, sample_values (random, not top-of-file),
   distribution (strings: top_values/distinct_count), numeric_range (min/max/mean/median/has_negatives/is_year)
3. grain_description — AI-generated 1-2 sentences, schema+stats only (no raw data sent)
4. data_quality_flags — looks_numeric_but_stored_as_text, high_null_rate, trailing_special_chars, low_distinct_count
5. time_series_column_families — detects columns sharing name pattern + year suffix
6. sql_quickstart — select_all, aggregate_by_top_category, measure_columns, group_column
7-9. (rollup detection, possible_rollup_rows flag)

### Critical implementation notes
- Sample values MUST use DuckDB random sampling: SELECT col FROM dataset USING SAMPLE 10000 ROWS WHERE col IS NOT NULL LIMIT 5
- numeric_range runs for ALL BIGINT, INTEGER, DOUBLE, FLOAT columns
- Aggregation ORDER BY prefers paid/spend/cost/amount; falls back to count/claims/beneficiaries
- Validated on 227M rows / 9.5GB Parquet — completes in seconds

---

## MILESTONE 4 — REFERENCE TABLE LIBRARY (COMPLETE)

### Implementation
- Storage: data/reference_library/ with optional _library.json manifest
- Auto-discover: any CSV appears automatically; manifest adds description + join_hint
- GET /api/reference_library — returns manifest + auto-discovered CSVs merged
- POST /api/reference_library/{filename}/load — imports library CSV as active reference

### Current library files
ira_negotiated_drugs.csv, globe_exclusions.csv, guard_ira_exclusions.csv,
ira_exclusions.csv, multisource_exclusions.csv, nonglobe_exclusions.csv

### Friction Reduction Backlog — All Complete (v1.5.0)
1. Result Passport display-cap fix — total_rowcount in request, sampling note
2. Rollup row detection — possible_rollup_rows quality flag in Export Passport
3. JOIN match diagnostic — reference_info in /api/sql response
4. Reference Table Library v1 — IRA 35 drugs, frontend popover UI

---

## COMPASS/FARRAGUT WORKFLOW — VALIDATION (COMPLETE)

Stress test March 2026. Task: identify CMS GLOBE/GUARD drug payment model candidates.
- IRA exclusion: Reference table JOIN, 25 drugs in one CSV, single query, 94→59 drugs
- LIKE pattern handled trailing asterisks in CMS brand names
- Brand name matching caught formulation variants (Enbrel/Enbrel Sureclick, Austedo/Austedo XR)
- Part B / GLOBE memo: 57 confirmed candidates, 14 sole orphan, 40 MFN deal manufacturers
- Part D / GUARD memo: 304 preliminary candidates, $125.9B spending (upper-bound)

---

## MILESTONE 5 — SESSIONS (COMPLETE as of v1.10.0)

- Session Log — v1.5.6. 14 event types, 14 endpoints instrumented. Auto-saves every 10 events.
- Session File replay — v1.6.0. Automatic mode. Machine-executable.
- Session Library / Example Cases — v1.9.0. 7 cases, 4 domains.
- Workspace Snapshot — v1.8.0. Auto-save/restore on shutdown/launch.
- Named Snapshots — v1.9.0. Save/list/restore/delete.
- Reference Guide — v1.10.0. In-app documentation.
- Collapsible sidebar sections — v1.10.0.
- Exit button with save prompts — v1.10.0.
- SESSIONS restructure (4 buttons, 2 rows) — v1.10.0.

---

## KNOWN BUGS — RESOLVED

| # | Summary | Fixed in |
|---|---------|----------|
| 1 | Silent SQL failure on invalid DuckDB syntax | M3 |
| 2 | Long NOT LIKE / NOT IN chains (~26 conditions) | M3 |
| 3 | ORDER BY DESC parser error when AW wraps query | M3 |
| 4 | Suggestions button caching | M3 |
| 5 | Result Passport display-cap | v1.5.0 |
| 7 | Reference Library tables not registering in DuckDB | v1.5.0 |
| 8 | Reference Library case mismatch on JOIN — _title_case_string_columns() | v1.5.0 |
| 10 | Reference table not queryable after load/restart — _resolve_reference_for_sql() | v1.6.1 |
| 11 | AI using APPROX_PERCENTILE_CONT (not in DuckDB) | v1.6.1 |
| 12 | ORDER BY DESC regression — _SQL_KW missing desc/asc/etc | v1.7.1 |
| 13 | Session Log recording insight previews as query_run — internal:true flag | v1.7.1 |
| 14 | Tutorial mode pre-loading state instead of replaying live | v1.7.2 |
| 15 | Tutorial query steps not executing — replaced with runSqlQuery() | v1.8.0 |
| 16 | Tutorial importing multiple datasets — set datasets[] directly | v1.8.0 |
| 17 | Session/snapshot restore shows wrong datasets — list_datasets() marker check + selectedDataset ordering | v1.10.1 |
| 18 | Refresh Datasets permanently deleted files from disk (called /api/datasets/{name}/delete for each dataset), destroying datasets that restore needed. Also: _restoreWorkspace() had no sidebar expand calls; loadDatasets() auto-select silently switched to wrong dataset when preset name not found. Fix: Refresh is now a pure UI re-fetch (no delete); added expand calls to _restoreWorkspace(); loadDatasets() sets selectedDataset="" instead of falling back to datasets[0] when preset name is absent. | v1.10.1 |
