# Analytics Workbench — Claude Code Context

> **See CONTEXT.md for current product and business state.**
> CONTEXT.md is the single source of truth for milestone status, open decisions,
> session log, and next actions. Read it at the start of every Claude Code session.


## PROJECT NAME
Analytics Workbench

## PROJECT PURPOSE
Analytics Workbench is an AI-assisted analytics desktop application that allows a user to:

1. Import a dataset (CSV, Excel, TSV, Parquet)
2. Inspect the dataset (Profile, Schema, Preview)
3. Ask a natural-language question about the dataset
4. Get AI-suggested questions via the Suggestions button
5. Generate SQL with AI
6. Manually review/edit the SQL
7. Run SQL against the dataset
8. View results in a table
9. View automatic chart (bar or line) when result shape supports it
10. Export results (Excel or TSV)

The product direction is an AI-assisted insight workflow, not just a SQL textbox.

## PRIMARY PRODUCT GOAL
Move from:
- AI-assisted query tool

To:
- AI-assisted insight tool

## TARGET USER FLOW
Open file → dataset loads → **insights appear automatically** → inspect dataset → ask question →
generate SQL → review/edit SQL → run SQL → table + chart appear → export results

## CURRENT DEVELOPMENT STAGE
Milestone 3 is COMPLETE as of March 2026.
Milestone 4 is the active target — the Insights view + Reference Table JOIN.

---

## TECH STACK
- Backend: FastAPI (Python)
- Query engine: DuckDB (all SQL runs against local Parquet files)
- AI: OpenAI GPT-4.1-mini (SQL generation + question suggestions + insights)
- Frontend: Single static HTML/JS/CSS file (no framework)
- Packaging: Local desktop app (PyInstaller)
- Chart rendering: Chart.js 4.4.1

---

## REPOSITORY STRUCTURE
```
/
├── frontend/
│   └── index.html              # Entire frontend — single file
├── backend/
│   └── app/
│       ├── main.py             # FastAPI app, all core endpoints
│       ├── ai/
│       │   ├── routes.py       # /api/ai/* endpoints
│       │   ├── provider_openai.py  # OpenAI prompts and API calls
│       │   ├── context_builder.py  # Builds dataset context for AI
│       │   ├── response_parser.py  # Parses AI JSON responses
│       │   ├── schemas.py      # Pydantic models for AI routes
│       │   └── sql_validator.py    # Validates AI SQL with DuckDB EXPLAIN
│       └── services/
│           ├── dataset_import.py   # Import pipeline
│           └── chart_recommender.py # Chart type recommendation
├── data/
│   └── datasets/
│       └── <dataset_name>/
│           ├── source.parquet
│           ├── metadata.json
│           ├── _meta.json
│           └── dataset_context.json  # AI context + cached suggestions + cached insights
├── exports/
├── .env                        # OPENAI_API_KEY (not committed)
├── CLAUDE.md                   # This file
└── start-dev.bat               # Double-click launcher for dev session
```

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
- Actual last error message surfaced in toast for diagnosis

### Suggestions Button
- Calls GET /api/ai/suggest_questions
- Results CACHED in dataset_context.json under "suggested_questions" key
- First click: calls OpenAI, caches result
- Subsequent clicks: returns from cache instantly (no OpenAI call)
- Popover shows "Cached suggestions — Refresh" link for force regeneration
- Refresh link calls endpoint with ?refresh=true to bypass cache
- If OpenAI fails on refresh, cached suggestions returned as fallback
- Suggestions popover closes when dataset changes

### Inspect Dataset
- Profile, Schema, Preview all working
- Fixed header (7-slot CSS grid) — does NOT scroll with data
- Only the data window below scrolls horizontally
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
- /api/sql response includes "visualization" block
- Chart.js 4.4.1 renders in Chart tab
- Chart tab renders on user click — no auto-switch
- clearChart() called before every new SQL run

---

## MILESTONE 4 — ACTIVE TARGET

### Product Goal
The user opens a dataset and within 30 seconds sees something they didn't know
to ask for. No query required. No SQL. Just open a file and get value immediately.

### The Core Experience
```
User imports dataset
        ↓
Insights tab activates automatically
        ↓
AI analyzes the dataset structure and content
        ↓
3–5 insight cards appear
        ↓
Each card shows a finding + supporting chart or table
        ↓
User clicks any card → opens as full query in Query tab
        ↓
User asks follow-up questions from the card
```

### Insight Card Structure
Each card contains:
1. A plain-English headline ("Keytruda accounts for 18% of all Part B spending")
2. A one-sentence explanation of why it's notable
3. A supporting chart or summary table rendered inline
4. An "Explore in Query" button that loads the underlying SQL into the Query tab

### Six Insight Types (in priority order)
1. **Concentration** — small number of items drives disproportionate share of total (Pareto)
2. **Outliers** — values far outside the norm for their group
3. **Trend** — fastest-growing and fastest-declining items (requires time column)
4. **Distribution skew** — heavily skewed numeric columns
5. **Missing/anomalous data** — high null rates, zero values, formatting inconsistencies
6. **Correlation** — two numeric columns that move together in an interesting way

### What NOT to do in Insights
- Do not generate insights that restate the obvious ("This dataset has 734 rows")
- Do not generate more than 5 cards
- Do not make the user wait more than 10 seconds for the first card
- Do not require configuration before insights appear
- Do not make every card a chart — numbers and small tables are fine
- Do not use jargon in headlines — write for business users

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

### New Backend Endpoints Required
```
GET /api/ai/insights?dataset=<name>
GET /api/ai/insights?dataset=<name>&refresh=true
```

Behavior:
- First call: runs AI analysis, caches result in dataset_context.json under "insights" key
- Subsequent calls: returns from cache instantly
- ?refresh=true bypasses cache and regenerates
- Pattern is identical to suggest_questions caching — follow the same implementation

### Technical Approach
1. Use existing /api/profile data as input to insight engine (no new data collection)
2. Pass profile + data sample to GPT-4.1-mini with structured prompt
3. AI returns JSON array of insight objects (same parse pattern as suggestions)
4. For each insight, generate supporting SQL query
5. Render cards using existing Chart.js + results table infrastructure

### UI Changes Required
- On dataset load, auto-trigger insights fetch
- Show skeleton loading cards while AI runs (3 placeholder cards)
- Render insight cards in a 2-column grid in the Insights view
- Each card: headline, explanation, mini chart or table, Explore button
- Add "Refresh Insights" button in toolbar
- Empty state if no insights can be generated — not a blank screen

---

## MILESTONE 4 — REFERENCE TABLE JOIN (COMPLETE)

### Status
COMPLETE — built and working as of March 2026.

### How It Works
- "+ Reference Table" button in sidebar imports CSV/TSV/XLSX as lightweight Parquet
- Reference tables: no profiling, no insights, only _meta.json with column names/types
- Available in SQL as table name "reference" (e.g. JOIN reference ON ...)
- AI context builder includes reference table column names when generating SQL
- One reference table at a time — importing a new one overwrites the previous
- Reference indicator in sidebar shows loaded table name, row count, column count
- "Remove" button clears the reference table

### What This Unlocks
- JOIN an IRA drug exclusion list → automatic IRA filtering in any query
- JOIN a USP category mapping → automatic therapeutic classification
- JOIN a manufacturer MFN list → automatic MFN flagging
- Any lookup table enrichment the user needs

### Implementation
- dataset_import.py: import_reference_table() — lightweight pipeline, always overwrites
- context_builder.py: build_reference_context() — schema-only (column names + types)
- main.py: _rewrite_sql_dataset_reference extended for FROM/JOIN reference rewriting
- main.py: REFERENCES_DIR, /api/references/import, /api/references, /api/references/{name}/delete
- provider_openai.py: SQL prompt includes reference table columns when loaded
- routes.py: AI SQL generation wired with reference context
- schemas.py: reference field on GenerateSQLRequest
- frontend: reference import button, file input, indicator, passes reference in SQL/export/AI calls
- Error: SQL using "reference" without loaded table → HTTP 400 with clear message

### Storage
```
data/references/<registered_name>/
  source.parquet       — reference table file
  _meta.json           — column names, types, row count
```

### Scope Boundary
This is enrichment, not multi-dataset analysis. One primary dataset + one reference
table. Keep it simple. Do not build a general multi-dataset join engine.

---

## MILESTONE 4 — DATA PRIVACY & AI TRANSPARENCY (COMPLETE)

### Status
COMPLETE — all three components built and working.

### Component 1: Schema-only mode for Insights
The insights prompt in provider_openai.py sends ONLY column names, data types,
and aggregate numeric stats (min/max/mean). Sample rows and categorical top
values are excluded. The AI generates SQL that runs locally — no raw data
values need to leave the machine.

### Component 2: Permanent privacy disclosure
A muted text line is permanently visible in the Insights view below the toolbar:
"AI generates analysis instructions from column names and statistics only.
Your data stays on your computer."

### Component 3: Per-dataset AI consent
On import, a confirm dialog asks whether to enable AI features. Decision stored
in _meta.json as "ai_consent" (true/false). If skipped:
- Insights auto-generation disabled
- Suggestions button disabled
- Query tab and manual SQL still work normally

Consent is exposed via GET /api/datasets/{name}/meta (ai_consent field) and
stored via POST /api/datasets/{name}/ai_consent.

### Framing guidance for UI copy
  WRONG: "AI analyzes your data"
  RIGHT: "AI generates analysis instructions that run on your computer"

### Future: Local AI mode (Milestone 5 — not Milestone 4)
For users who need full air-gap operation, plan for a local AI mode using
Ollama + a locally-running model in Milestone 5.

---

## MILESTONE 4 — RESULT PASSPORT (COMPLETE)

### Status
COMPLETE — built and working.

### Purpose
When an analyst collaborates with an external AI on query results, they can
copy a structured summary instead of exporting raw rows. The Result Passport
contains per-column profiles (top values with counts, numeric stats, null
rates, data quality flags) but zero raw row data.

### UI
"Copy Result Summary" button in results toolbar Row 2, after Export TSV.
Active only when a result set is present. Copies JSON to clipboard.
Toast: "Result summary copied — paste into Claude or any AI assistant"

### Endpoint
POST /api/results/passport
Request: { "columns": [...], "rows": [...], "sql": "..." }
Response: { row_count, column_count, columns, per_column_profile, data_quality_flags, grain_hint }

### What the passport includes
- row_count, column_count, column names
- Per-column profile:
  - String: distinct_count, top_values (15) with counts, null_count/pct
  - Numeric: min, max, mean, median, null_count/pct
- Data quality flags: high_null_rate (>10%), looks_numeric_but_stored_as_text
- grain_hint: the SQL query that produced the result

### What it does NOT include
- Raw row data
- The full result set
- Sensitive values beyond what appears in top_values counts

---

## MILESTONE 4 — EXPORT PASSPORT (COMPANION FEATURE)

### Purpose
Allow the user to export a structured JSON file — the "dataset passport" —
that captures everything needed to write accurate SQL against a dataset without
seeing the data. Designed to be uploaded to Claude or any AI assistant as a
cold-start context file.

### Status
COMPLETE — built and validated on a 227M row / 9.5GB Parquet file.
Four iteration cycles. All sections verified working.

### UI
Export Passport button lives in the Inspect row, after Preview:
  Schema  |  Preview  |  Export Passport

Single click triggers a direct download of <dataset_name>_passport.json.
No panel opens. No toggle. Button shows loading state during AI grain
description call (2-3 seconds). On error: toast with error message.

### Endpoint
  GET /api/datasets/{name}/passport

Returns the full passport JSON. Grain description cached in
dataset_context.json under "grain_description" key — same caching
pattern as suggest_questions. Force refresh with ?refresh=true.

### Passport JSON Structure (9 sections)

1. identity
   - dataset_name, row_count, column_count, source_file_type,
     import_date, file_size_bytes

2. schema — per column:
   - column_name, data_type, nullable, null_count, null_pct
   - sample_values: 5 RANDOM values drawn via USING SAMPLE
     (NOT top-of-file — must use DuckDB random sampling)
   - distribution (string columns): top_values (15), distinct_count
   - numeric_range (numeric columns): min, max, mean, median,
     has_negatives (bool), is_year_column (bool)

3. grain_description
   - AI-generated 1-2 sentences describing what one row represents
   - Includes explicit grouping guidance to prevent double-counting
   - Generated by GPT-4.1-mini from schema + stats only (no raw data)
   - Cached in dataset_context.json

4. data_quality_flags (auto-detected, no AI)
   - looks_numeric_but_stored_as_text
   - high_null_rate (>10%)
   - trailing_special_characters (>5% of values contain *, †, etc.)
   - low_distinct_count (flagged with sample size context)

5. time_series_column_families
   - Detects columns sharing a name pattern + year suffix
     (e.g. Tot_Spndng_2019...Tot_Spndng_2023)
   - Reports base pattern and detected years

6. sql_quickstart
   - select_all: SELECT all columns FROM dataset LIMIT 100
   - aggregate_by_top_category: GROUP BY best categorical column,
     SUM all measure columns, ORDER BY paid/spend/cost/amount column
     DESC (falls back to count/claims/beneficiaries if no paid column)
   - measure_columns: list of detected measure columns
   - group_column: detected categorical grouping column
   - Column aliases match source column names exactly (no redundant
     prefixes like "total_total_paid")

### Critical implementation notes
- Sample values MUST use DuckDB random sampling, not top-of-file scan:
    SELECT col FROM dataset USING SAMPLE 10000 ROWS WHERE col IS NOT NULL LIMIT 5
- numeric_range queries must run for ALL BIGINT, INTEGER, DOUBLE, FLOAT columns
- Aggregation query must prefer paid/spend/cost/amount for ORDER BY
- Grain description prompt sends schema + stats ONLY — no sample rows
  (privacy: no raw data values sent to OpenAI)

### Validated behavior (stress test results)
- Works correctly on 227M rows / 9.5GB Parquet
- Random sampling produces genuinely representative sample values
- Grain description correctly identifies aggregation grain and warns
  about double-counting risk
- has_negatives correctly detected on TOTAL_PAID (payment reversals)
- Non-standard provider ID formats detected as data quality flags
- Passport generation completes in seconds on large files

## MILESTONE 4 — SUCCESS CRITERIA

Milestone 4 is complete when:
1. [DONE] User imports any structured dataset and sees 3–5 insight cards within 10 seconds
2. [DONE] Every insight card has a working "Explore in Query" drill-down
3. [DONE] Insights are cached — reloading the dataset is instant
4. [DONE] Reference table JOIN works end-to-end for at least one enrichment use case
5. [DONE] Export Passport produces a valid JSON file with all 9 sections populated,
   including random sample values and numeric ranges
6. [ ] The Compass/Farragut analytical workflow (see below) can be completed entirely
   inside AW without exporting to an external tool

---

## COMPASS/FARRAGUT WORKFLOW — REFERENCE USE CASE

This real-world workflow was used as a stress test in March 2026 and defines
the target capability for Milestone 4. When Milestone 4 is complete, this
entire workflow should run inside AW.

**The task:** Identify drugs likely included in the CMS GLOBE and GUARD payment
models, applying spending thresholds, IRA exclusions, single-source filtering,
and therapeutic category classification.

**Steps that required leaving AW in the stress test (Milestone 4 should fix these):**
1. IRA exclusion list — 40+ drugs across 3 rounds, too many NOT LIKE conditions
   for AW query engine → Fix: Reference table JOIN
2. Therapeutic category classification — CASE statement too complex for AW query
   length limit → Fix: Reference table JOIN with pre-built category mapping CSV
3. Post-processing / enrichment — Python classification after export
   → Fix: Reference table JOIN eliminates this step entirely
4. Silent SQL failures — wrong results returned without error
   → Fix: Bug fixes (see Known Bugs section below)

---

## KNOWN BUGS — ACTIVE (Fix before or during Milestone 4)

### Bug 1: Silent SQL failure on invalid DuckDB syntax
**Status:** FIXED. DuckDB errors now surface as HTTP 400 with readable messages.

### Bug 2: Long NOT LIKE / NOT IN chains fail silently around 26 conditions
**Status:** FIXED. Root cause was _validate_readonly_sql scanning blocked keywords
(insert, update, delete, drop, etc.) inside quoted string literals. Drug names like
'Alteplase' or LIKE patterns like '%update%' triggered false positives. Fix: regex
strips single-quoted literals before keyword scanning (main.py:991).

### Bug 3: ORDER BY DESC parser error when AW wraps query
**Status:** FIXED (could not reproduce). The subquery wrapping logic
`SELECT * FROM ({sql}) t LIMIT 200` correctly preserves ORDER BY ... DESC.
Tested with DuckDB directly — no parser error.

### Bug 4: Suggestions button caching
**Status:** FIXED. Two-layer caching (JS + server) working with refresh mechanism.

### Bug 5: Refresh Datasets — Windows file lock issue
**Symptom:** shutil.rmtree() in /api/datasets/{name}/delete may fail silently
when DuckDB has the Parquet file open.
**Status:** Partially addressed — retry logic added, needs further testing on Windows.
**Priority:** Medium.

---

## DATA QUALITY NOTES (Learned from Compass stress test)

### CMS Dataset Asterisk Contamination
CMS Medicare spending datasets append asterisks to some brand and generic names
(e.g. "Stelara*", "Denosumab*", "Orencia*"). These bypass exact-match SQL
filters (= and IN operators) and cause incorrect exclusion logic.

**Workaround in SQL:** Always use LIKE 'DRUGNAME%' instead of = 'DRUGNAME'
or IN ('DRUGNAME') when filtering CMS brand name columns.

**Recommended fix:** Add optional trailing special-character stripping on import
in dataset_import.py. Should be opt-in (default off) with a checkbox in the
import UI. Strip trailing non-alphanumeric characters from string columns.

### CMS Part D Tot_Mftr Proxy Limitation
The Tot_Mftr column in CMS Part D data counts manufacturers per brand name row,
not per molecule. A drug with biosimilars (e.g. Humira/adalimumab) still shows
Tot_Mftr=1 for its branded row even though biosimilars exist on the market.
Using Tot_Mftr=1 as a single-source proxy will over-include drugs with
biosimilar competition.

**Note for AI SQL generation:** When generating queries that filter on Tot_Mftr,
add a comment in the SQL noting this limitation.

### CMS Part B — No Manufacturer Column
The CMS Part B Spending by Drug dataset does not include a manufacturer name
column. Drug identification is by HCPCS code + brand name + generic name.
Manufacturer data for Part B drugs requires a supplemental source.

---

## UI DECISIONS (FROZEN — do not change without explicit instruction)
- Nav order: Insights → Query → Saved Queries
- App starts on Insights view
- Export Excel and Export TSV are direct toolbar buttons (no Export tab)
- Import uses <label for="file-input"> pattern, NOT programmatic .click()
- Prompt and SQL editor clear when switching datasets
- SQL editor clears when suggestion chip clicked
- Results block row layout:
  Row 1: Run SQL | Explain | execution metadata
  Row 2: RESULTS label | row count | Table | Chart | Export Excel | Export TSV | Copy Result Summary
  Row 3: table or chart content

---

## API ENDPOINTS
```
GET  /api/version
GET  /api/health
GET  /api/datasets
GET  /api/datasets/{name}/meta
POST /api/datasets/{name}/delete
POST /api/datasets/import          (?overwrite=true supported)
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
GET  /api/ai/suggest_questions?dataset=  (?refresh=true to bypass cache)
GET  /api/ai/insights?dataset=           (?refresh=true to bypass cache) [MILESTONE 4]
POST /api/results/passport               (result summary for clipboard) [MILESTONE 4]
POST /api/datasets/{name}/ai_consent     (store AI consent) [MILESTONE 4]
POST /api/references/import              (reference table import) [MILESTONE 4]
GET  /api/references                     (list reference tables) [MILESTONE 4]
POST /api/references/{name}/delete       (delete reference table) [MILESTONE 4]
POST /api/shutdown
```

---

## SQL EXECUTION CONVENTION
All SQL runs against Parquet via DuckDB.
Frontend writes SQL using logical table name "dataset".
main.py rewrites FROM dataset → FROM read_parquet('/absolute/path/source.parquet')
AI instructed to always use "dataset" as table name, no semicolon at end.

When a reference table is loaded, main.py also rewrites "reference" to its
absolute Parquet path. Both rewrites must happen before execution.

---

## /api/sql RESPONSE SHAPE
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

---

## AI PROMPT NOTES (provider_openai.py)

### SQL Generation
- DuckDB syntax only
- strftime('%Y-%m', col) — format string FIRST
- DATE_TRUNC('month', col) for date bucketing
- col::INTEGER for type casting
- Never invent column names
- Table name is always "dataset" (reference table is "reference" if present)
- No semicolon at end of query
- Returns JSON: {status, sql, message, warnings}
- LIKE patterns preferred over = or IN for string matching (handles data quality issues)

### Insights Generation (Milestone 4)
- Input: dataset profile (column names, types, numeric stats ONLY — no sample rows, no top values)
- Output: JSON array of insight objects (see Insight Object Schema above)
- Max 5 insights
- Each insight must include executable SQL
- Headlines must be plain English, no jargon
- Prioritize: concentration > outliers > trend > skew > missing data > correlation

### Suggestions
- Generates schema-grounded analytical questions
- Results cached in dataset_context.json under "suggested_questions" key

---

## DATASET STORAGE STRUCTURE
```
data/datasets/<registered_name>/
  source.parquet       — canonical internal file
  metadata.json        — full import metadata
  _meta.json           — lightweight summary for fast cache reads
  dataset_context.json — AI context (columns, stats, sample rows,
                         suggested_questions, insights)

data/references/<registered_name>/   [MILESTONE 4 — NEW]
  source.parquet       — reference table file
  _meta.json           — column names and types only (no profiling)
```

---

## ENVIRONMENT VARIABLES
```
OPENAI_API_KEY=sk-...
OPENAI_MODEL=gpt-4.1-mini
AW_DATASETS_DIR=...        (absolute path override)
AW_EXPORTS_DIR=...
AW_CONTEXT_SAMPLE_ROWS=100000
AW_DEFAULT_PREVIEW_ROWS=50
AW_MAX_PREVIEW_ROWS=200
AW_MAX_EXPORT_ROWS=200000
```

---

## CODING CONVENTIONS
- Python: type hints throughout, dataclasses for data models
- Error handling: always HTTP 400/404 with detail text — never bare 500s
- Logging: logger.info/warning/exception — never print()
- All file paths: .resolve() to absolute before use
- Frontend: vanilla JS only, no framework
- CSS: use existing custom properties (--bg0, --accent, etc.)
- Comments: explain WHY, especially for non-obvious DuckDB behavior
- Use DESCRIBE SELECT * not parquet_schema() for column counts
- Never call loadDatasets() inside import polling loop

---

## WHAT NOT TO DO
- Do not use parquet_schema() for column counts
- Do not call loadDatasets() in the import polling loop
- Do not use relative paths for DATASETS_DIR
- Do not add frontend frameworks
- Do not auto-switch to Chart tab after SQL run
- Do not expose internal Parquet conversion to users
- Do not change frozen UI decisions without explicit instruction
- Do not use = or IN for string matching on CMS datasets — use LIKE patterns
- Do not swallow DuckDB errors — always surface the actual error message
- Do not build a general multi-dataset engine for Milestone 4 — keep it to
  one primary dataset + one reference table

---

## DEVELOPMENT ENVIRONMENT
- Windows 11
- Project root: C:\dev\analytics-workbench
- Virtual environment: .venv
- Launch command: uvicorn backend.app.main:app --reload --port 8000
- App URL: http://127.0.0.1:8000/ui/
- Claude Code: installed, CLAUDE.md in project root
- Batch file: start-dev.bat in project root

---

## WHEN CONTRIBUTING
1. Always read the actual file before editing — never assume current state
2. Preserve frozen UI decisions unless explicitly told to change them
3. Distinguish frontend vs backend problems clearly
4. Prefer targeted fixes over rewrites
5. Use /plan in Claude Code for any change touching multiple files
6. Always .resolve() file paths to absolute
7. Test that imports still work after any main.py change
8. Never leave bare except: pass blocks — always log the error
9. For any new AI endpoint, follow the suggest_questions caching pattern exactly
10. When in doubt about DuckDB syntax, test with EXPLAIN before executing
