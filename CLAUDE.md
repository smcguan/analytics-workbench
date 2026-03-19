# Analytics Workbench — Claude Code Context

> **IMPORTANT: Read CONTEXT.md before starting any session.**
> CONTEXT.md is the single source of truth for milestone status, open decisions,
> session log, and next actions. Always read CONTEXT.md alongside this file —
> it contains business context, validation results, and backlog items that
> inform coding decisions.


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
Milestone 4 is COMPLETE. M5 planning in progress.

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
├── data/
│   ├── datasets/               # Imported datasets (Parquet + metadata)
│   ├── references/             # Active reference tables (Parquet + _meta.json)
│   └── reference_library/      # Pre-built library CSVs + _library.json manifest
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

## MILESTONE 4 — COMPLETE

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

### Display-cap fix
Row count now reflects full result set via total_rowcount parameter. When stats
are computed from a capped display sample, a note explains the sampling.

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
6. [DONE] The Compass/Farragut analytical workflow — mechanics validated (Reference Table
   JOIN tested with IRA exclusion list). Cold-start validation pending.

---

## COMPASS/FARRAGUT WORKFLOW — REFERENCE USE CASE

This real-world workflow was used as a stress test in March 2026 and defines
the target capability for Milestone 4. When Milestone 4 is complete, this
entire workflow should run inside AW.

**The task:** Identify drugs likely included in the CMS GLOBE and GUARD payment
models, applying spending thresholds, IRA exclusions, single-source filtering,
and therapeutic category classification.

**Steps that required leaving AW in the stress test (all now fixed):**
1. IRA exclusion list → FIXED: Reference table JOIN (25 exclusions in one CSV, single JOIN query)
2. Therapeutic category classification → FIXED: Reference table JOIN with category mapping CSV
3. Post-processing / enrichment → FIXED: Reference table JOIN eliminates external step
4. Silent SQL failures → FIXED: Bug #1 and #2 resolved

**Validation status:**
- Mechanics test: PASSED — 25 exclusions in combined CSV, single JOIN query,
  94→59 drugs. LIKE pattern handled trailing asterisks. Brand name matching
  correctly caught formulation variants (Enbrel/Enbrel Sureclick, Austedo/Austedo XR).
- Cold-start validation: PARTIALLY COMPLETE — Part D/GUARD analysis (March 2026)
  served as informal cold-start. Fresh dataset, fresh policy research, no prior
  knowledge. Formal controlled test not yet run — treat as nice-to-have, not blocker.

**Deliverables:**
- Part B / GLOBE memo — COMPLETE (57 confirmed candidates, 14 sole orphan, 40 MFN deal manufacturers)
- Part D / GUARD memo — COMPLETE preliminary (304 candidates, $125.9B spending)
- Remaining: orphan drug and MFN flags for GUARD candidates, 5 Farragut confirmations pending

---

## KNOWN BUGS — RESOLVED

### Bug 1: Silent SQL failure on invalid DuckDB syntax — FIXED
### Bug 2: Long NOT LIKE / NOT IN chains (~26 conditions) — FIXED
### Bug 3: ORDER BY DESC parser error when AW wraps query — FIXED
### Bug 4: Suggestions button caching — FIXED
### Bug 5: Result Passport display-cap — FIXED
### Bug 7: Reference Library tables not registering in DuckDB — FIXED
### Bug 8: Reference Library case mismatch on JOIN — FIXED
Reference table string columns now title-cased on import so JOINs match
CMS data without LOWER() wrappers. Fix in dataset_import.py
`_title_case_string_columns()`. 3 regression tests added.

## KNOWN BUGS — ACTIVE

### Bug 6: Refresh Datasets — Windows file lock issue
**Symptom:** shutil.rmtree() in /api/datasets/{name}/delete may fail silently
when DuckDB has the Parquet file open.
**Status:** Partially addressed — retry logic added, needs further testing on Windows.
**Priority:** Medium.

---

## FRICTION REDUCTION BACKLOG — ALL COMPLETE

1. **Result Passport display-cap fix** — DONE. total_rowcount in request, sampling note.
2. **Rollup row detection** — DONE. possible_rollup_rows quality flag in Export Passport.
   Detects generic aggregation terms (Overall, Total, All, etc.) appearing >= 3x more
   than the second value. No AI required.
3. **JOIN match diagnostic** — DONE. reference_info in /api/sql response. Frontend shows
   "Reference: {name} ({ref_rows} rows) → {result_rows} results" in results metadata.
4. **Reference Table Library** — DONE. GET /api/reference_library, POST load endpoint.
   First file: ira_negotiated_drugs.csv (35 drugs, IRA Rounds 1-3). Frontend popover UI.

### Reference Table Library — Implementation Details
- Storage: data/reference_library/ with _library.json manifest
- Manifest fields: filename, name, description, columns, row_count, version, join_hint
- GET /api/reference_library — returns manifest
- POST /api/reference_library/{filename}/load — imports library CSV as active reference
- Frontend: "Reference Library" button with popover in sidebar
- Additional library files planned: FDA orphan drugs, biosimilar tracker, USP categories

### Reference Table Library — Roadmap and Update Cadence
Planned files (priority order):
1. FDA orphan drug status — quarterly FDA OOPD publication
2. Manufacturer MFN deal status — irregular, announce-driven
3. USP category mappings (GLOBE 7-category + GUARD 17-category) — every 3 years
4. Biosimilar tracker — monthly FDA approval announcements

IRA list updates annually (new drug selections each February).

Business note: maintained library is a recurring reason for customers to stay on
maintenance contracts. Position as a living resource, not a static file.

---

## NEW PRODUCT IDEAS (from ChatGPT comparison, March 2026)

See CONTEXT.md for full specs. Summary:

1. **Generic Name Pattern Classifier** — Auto-classify drugs into therapeutic
   categories using generic name suffix patterns (e.g. -glutide → GLP-1,
   -mab → Biologic, -nib → Oncology TKI). Covers ~70-80% of branded drugs.
   Lives in Insights view. Medium build.

2. **Human-in-the-Loop Classification Workflow** — When auto-classification
   leaves unclassified residual, present items for manual review with suggested
   categories. Companion to pattern classifier. Medium-large build.

3. **Analysis Summary Artifact** — Auto-generated structured summary of an
   analytical session (filters applied, exclusions, findings, open questions).
   Companion to Export Passport (dataset) and Result Passport (query result).
   Together they form a complete audit trail. Medium build.

4. **Exploration vs. Verification design principle** — Insights view should
   lean into exploration (propose segmentation schemes, flag hypotheses) rather
   than just descriptive summaries. Design direction, not a single feature.

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
GET  /api/reference_library              (list library CSV files) [MILESTONE 4]
POST /api/reference_library/{name}/load  (load library CSV as reference) [MILESTONE 4]
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

data/references/<registered_name>/   [MILESTONE 4]
  source.parquet       — reference table file
  _meta.json           — column names and types only (no profiling)

data/reference_library/              [MILESTONE 4]
  _library.json        — manifest listing available library CSVs
  ira_negotiated_drugs.csv — IRA Rounds 1-3 drug list (35 drugs)
  (future: fda_orphan_drugs.csv, biosimilar_tracker.csv, usp_categories.csv)
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
1. Always read CONTEXT.md at session start — it has current status, backlog, and next actions
2. Always read the actual file before editing — never assume current state
3. Preserve frozen UI decisions unless explicitly told to change them
4. Distinguish frontend vs backend problems clearly
5. Prefer targeted fixes over rewrites
6. Use /plan in Claude Code for any change touching multiple files
7. Always .resolve() file paths to absolute
8. Test that imports still work after any main.py change
9. Never leave bare except: pass blocks — always log the error
10. For any new AI endpoint, follow the suggest_questions caching pattern exactly
11. When in doubt about DuckDB syntax, test with EXPLAIN before executing
