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

## MILESTONE 4 — REFERENCE TABLE JOIN (COMPANION FEATURE)

### Purpose
Allow the user to upload a small reference CSV and JOIN it against the primary
dataset inside AW. This enables enrichment workflows (IRA exclusion lists,
category mappings, manufacturer lists) without leaving AW.

### How It Works
- In the Import area, add a "Reference Table" option alongside primary dataset import
- Reference tables are lightweight — no profiling, no schema inspection, no insights
- Available in SQL as a second table name (user-defined or default "reference")
- AI context builder includes reference table column names when generating SQL
- One reference table per session (not a full multi-dataset engine)

### What This Unlocks
- JOIN an IRA drug exclusion list → automatic IRA filtering in any query
- JOIN a USP category mapping → automatic therapeutic classification
- JOIN a manufacturer MFN list → automatic MFN flagging
- Any lookup table enrichment the user needs

### Backend Changes Required
- dataset_import.py: add lightweight reference table import (CSV → Parquet, no profiling)
- context_builder.py: include reference table schema in AI context when present
- main.py: rewrite FROM dataset logic to handle FROM dataset JOIN reference
- No new endpoints required — extends existing import and SQL execution paths

### Scope Boundary
This is enrichment, not multi-dataset analysis. One primary dataset + one reference
table. Keep it simple. Do not build a general multi-dataset join engine.

---

## MILESTONE 4 — DATA PRIVACY & AI TRANSPARENCY

### Core product promise
Analytics Workbench's primary differentiator is that user data never leaves
the user's computer. This promise must remain true and visible in Milestone 4.
The Insights view makes AI more prominent — which makes the privacy story
more important, not less.

### What actually gets sent to OpenAI
Be precise about this. The context builder sends:
- Column names and data types
- Aggregate statistics (min, max, mean, null counts, top values)
- A sample of rows (capped by AW_CONTEXT_SAMPLE_ROWS)

It does NOT send the full dataset. But top values and sample rows do contain
real data values. For sensitive datasets this is a real exposure.

### Required: Schema-only mode for Insights (DEFAULT)
For the /api/ai/insights endpoint, the AI prompt must use ONLY:
- Column names and data types
- Aggregate statistics (row count, min, max, mean, null rate per column)

Do NOT include sample rows or top values in the insights prompt.

Rationale: Insights are generated as SQL queries that run locally in DuckDB.
The AI generates the analysis instructions — not the analysis itself. The
actual data computation happens entirely on the user's machine. No raw data
values need to leave to make this work.

This is not a quality tradeoff. It is the correct architecture.

### Required: Permanent privacy disclosure in UI
Add a single quiet line of text in the Insights view, visible at all times:

  "AI generates analysis instructions from column names and statistics only.
   Your data stays on your computer."

This is not a modal. Not a consent dialog. Not a legal disclaimer. Just an
honest, permanent, plain-English statement. Styled as secondary text — present
but not intrusive. Visible below the Refresh Insights button without scrolling.

### Required: Per-dataset AI consent on import
When a dataset is imported, show a clear one-time disclosure before AI features
activate for that dataset:

  "AI features will use column names and statistics to generate insights and
   suggested questions. No raw data is sent. Enable AI features?"
   [Enable] [Skip — use manual queries only]

Default: Enable. Store the consent decision in _meta.json per dataset. Do not
ask again once decided. Allow the user to change it in dataset settings.

### Framing guidance for UI copy
Always describe what the AI does accurately:

  WRONG: "AI analyzes your data"
  RIGHT: "AI generates analysis instructions that run on your computer"

  WRONG: "Insights are powered by AI"
  RIGHT: "AI identifies patterns to explore — all computation runs locally"

The distinction is real and important. The AI generates SQL. DuckDB runs it.
The data never moves.

### Future: Local AI mode (Milestone 5 — not Milestone 4)
For users who need full air-gap operation, plan for a local AI mode using
Ollama + a locally-running model in Milestone 5. Do not build it in Milestone 4
but ensure Milestone 4 architecture decisions don't block it. When active, UI
shows "Local AI mode — all processing on this machine."

---


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
1. User imports any structured dataset and sees 3–5 insight cards within 10 seconds
2. Every insight card has a working "Explore in Query" drill-down
3. Insights are cached — reloading the dataset is instant
4. Reference table JOIN works end-to-end for at least one enrichment use case
5. Export Passport produces a valid JSON file with all 9 sections populated,
   including random sample values and numeric ranges
6. The Compass/Farragut analytical workflow (see below) can be completed entirely
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
**Symptom:** Queries with many NOT LIKE or NOT IN conditions in a WHERE clause
fail without a clear error message around line 26. DuckDB itself has no such
limit — this is a frontend or backend issue.
**Investigation needed:** Check for content-length limits, newline/character
parsing issues, or textarea size limits in the frontend SQL editor.
**Fix:** Diagnose the limit. Add a clear error message when a query exceeds any
limit. Do not fail silently.
**Priority:** High.

### Bug 3: ORDER BY DESC parser error when AW wraps query
**Symptom:** When a user query contains ORDER BY ... DESC, the SQL rewriting
logic in main.py (_rewrite_sql_dataset_reference or wrapping into a subquery)
may produce invalid SQL, causing a parser error.
**Investigation needed:** Check how main.py wraps user SQL and whether ORDER BY
clauses survive the rewrite correctly.
**Priority:** High.

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
  Row 2: RESULTS label | row count | Table | Chart | Export Excel | Export TSV
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

### Insights Generation (NEW — Milestone 4)
- Input: dataset profile (column stats, sample rows, top values)
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
