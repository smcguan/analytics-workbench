# Analytics Workbench — Claude Code Context

> **IMPORTANT: Read CONTEXT.md before starting any session.**
> CONTEXT.md is the single source of truth for milestone status, open decisions,
> session log, and next actions. Always read CONTEXT.md alongside this file.
> Historical milestone detail and resolved bugs are in HISTORY.md.

---

## PROJECT NAME
Analytics Workbench

## PROJECT PURPOSE
AI-assisted analytics desktop app. User imports a dataset, gets automatic insights,
asks natural-language questions, generates and runs SQL, views results + charts,
exports results. One primary dataset + one optional reference table per session.

## TARGET USER FLOW
Open file → dataset loads → insights appear automatically → inspect dataset → ask question →
generate SQL → review/edit SQL → run SQL → table + chart appear → export results

## CURRENT DEVELOPMENT STAGE
Milestones 1–4 complete. Milestone 5 in active planning. See CONTEXT.md.

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
│   ├── datasets/               # Imported datasets (Parquet + metadata)
│   ├── references/             # Active reference tables (Parquet + _meta.json)
│   ├── reference_library/      # Pre-built library CSVs + _library.json manifest
│   ├── sessions/               # Saved session JSONs
│   ├── example_cases/          # Curated tutorial/demo cases with sample data
│   │   └── <case_id>/
│   │       ├── metadata.json   # Case name, category, difficulty, has_session
│   │       ├── session.json    # Tutorial session with narration + baselines
│   │       ├── data/           # Sample dataset CSV
│   │       └── reference/      # Reference table CSVs (if any)
│   └── workspace.json          # Auto-snapshot on shutdown for resume
├── exports/
├── .env                        # OPENAI_API_KEY (not committed)
├── CLAUDE.md                   # This file
├── HISTORY.md                  # Archived milestone detail + resolved bugs
└── start-dev.bat               # Double-click launcher for dev session
```

---

## DATASET STORAGE STRUCTURE
```
data/datasets/<registered_name>/
  source.parquet       — canonical internal file
  metadata.json        — full import metadata
  _meta.json           — lightweight summary for fast cache reads
  dataset_context.json — AI context (columns, stats, suggested_questions, insights)

data/references/<registered_name>/
  source.parquet       — reference table file
  _meta.json           — column names and types only (no profiling)

data/reference_library/
  _library.json        — manifest (optional — auto-discovery works without it)
  ira_negotiated_drugs.csv — IRA Rounds 1-3 (35 drugs)
  (+ globe/guard/ira/multisource/nonglobe exclusion CSVs)

data/sessions/
  session_{uuid}_{date}.json — saved session files (named sessions only; UUID files filtered out)

data/example_cases/<case_id>/
  metadata.json, session.json, data/<dataset>.csv, reference/<ref>.csv
```

---

## KNOWN BUGS — ACTIVE

### Bug 10: Reference table title-case normalization breaks JOIN conditions
**Symptom:** AW title-cases all string values on reference table import (e.g. 'TX'→'Tx',
'BENE_ID'→'Bene_Id'). Any SQL JOIN against a reference table using exact string match
will return zero rows unless both sides are wrapped in UPPER().
**Workaround:** Wrap both sides of JOIN string comparisons in UPPER() — e.g.
`UPPER(r.state) = 'TX'` and `UPPER(r.source_column) = 'BENE_ID'`.
**Do not fix yet** — workaround is sufficient for Tutorial #4. Fix would require
either disabling title-case normalization (breaks existing CMS JOIN cases) or
storing both raw and normalized forms.
**Priority:** Medium — known workaround exists; affects any reference table JOIN.

### Bug 11: OH ZIP_CODE is numeric — type mismatch in cross-state UNION
**Symptom:** Ohio's geographic column (ZIP_CODE) is stored as INTEGER. In a UNION ALL
across TX (VARCHAR county) + FL (VARCHAR region) + OH (INTEGER zip), DuckDB raises a
type mismatch error unless OH ZIP_CODE is cast explicitly.
**Workaround:** `ZIP_CODE::VARCHAR AS geography` in the OH leg of any UNION query.
**Do not fix yet** — this is a data generation characteristic; fix belongs in Tutorial #4
SQL authoring, not in AW product code.
**Priority:** Low — only affects cross-state UNION; workaround is a single SQL cast.

### Bug 9: Insights View fails on non-standard column names
**Symptom:** Insights generation fails or produces no results when dataset column names
don't match expected patterns (e.g. abbreviated names like BENE_ID, PRVDR_NPI, SVC_DT
as seen in Medicaid claims data). First observed with TX/FL/OH Medicaid test datasets.
**Recommended fix:** Run insights against the post-JOIN normalized dataset (canonical
column names) rather than the raw import column names.
**Priority:** High — blocks M5 Tutorial #4 Insights step.

### Bug 6: Refresh Datasets — Windows file lock issue
**Symptom:** shutil.rmtree() in /api/datasets/{name}/delete may fail silently
when DuckDB has the Parquet file open.
**Status:** Mitigated — Refresh Datasets no longer calls /api/datasets/{name}/delete at all
(v1.10.2). Refresh now clears UI state only; files remain on disk. The file-lock issue
is no longer a user-facing problem for the Refresh flow.
**Priority:** Low (delete-on-demand per dataset still uses rmtree; edge case on Windows).

---

## DATA QUALITY NOTES (affects AI SQL generation)

### CMS Dataset Asterisk Contamination
CMS Medicare datasets append asterisks to some drug names (e.g. "Stelara*").
These bypass exact-match filters. Always use LIKE 'DRUGNAME%' instead of = or IN
when filtering CMS brand name columns. AI SQL generation prompts encode this rule.

### CMS Part D Tot_Mftr Proxy Limitation
Tot_Mftr counts manufacturers per brand name row, not per molecule. Single-source
filtering via Tot_Mftr=1 over-includes drugs with biosimilar competition.
When generating queries on Tot_Mftr, add a SQL comment noting this limitation.

### CMS Part B — No Manufacturer Column
Part B Spending by Drug has no manufacturer column. Drug ID = HCPCS + brand + generic.
Manufacturer data requires a supplemental source.

---

## UI DECISIONS (FROZEN — do not change without explicit instruction)
- Nav order: Insights → Query → Saved Queries
- App starts on Welcome view — Welcome card is the session management hub
- GET STARTED section: Welcome, Reference Guide, Example Cases
- No SESSIONS sidebar section — session management (Resume + Save) lives on Welcome card
- Sidebar footer button order: Clear Workspace (full-width) → Resume Session | Save As → Exit | Save & Exit
- Clear Workspace button in sidebar footer: clears dataset, reference, SQL editor, Ask Your Data field
- Exit button below sidebar sections — closes immediately, no save prompt
- Save button below sidebar sections — navigates to Welcome card, focuses session name field
- Sidebar sections collapsible with ▼/▶ toggles
- Sidebar section order: GET STARTED → DATA → WORKSPACE → Exit/Save
- Default: GET STARTED expanded, DATA/WORKSPACE collapsed
- Smart auto-expand: DATA on import, WORKSPACE on first query, GET STARTED collapses on import
- Example Cases groups collapsed by default in browser dialog
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
GET  /api/ai/insights?dataset=           (?refresh=true to bypass cache)
POST /api/results/passport
POST /api/datasets/{name}/ai_consent
POST /api/references/import
GET  /api/references
POST /api/references/{name}/delete
GET  /api/reference_library
POST /api/reference_library/{name}/load
GET  /api/session
GET  /api/session/export
GET  /api/session/summary
GET  /api/session/files
POST /api/session/replay
POST /api/session/annotate
POST /api/session/resume
POST /api/session/name
GET  /api/session/load/{filename}
GET  /api/example_cases
POST /api/example_cases/{id}/load
GET  /api/sessions/saved
GET  /api/example_cases/{id}/session
POST /api/example_cases/{id}/import_dataset
POST /api/example_cases/{id}/import_reference
GET  /api/workspace
POST /api/workspace
POST /api/workspace/restore
DELETE /api/workspace
GET  /api/datasets/{name}/passport  (?refresh=true to bypass cache)
POST /api/shutdown
```

---

## SQL EXECUTION CONVENTION
All SQL runs against Parquet via DuckDB.
Frontend writes SQL using logical table name "dataset".
main.py rewrites `FROM dataset` → `FROM read_parquet('/absolute/path/source.parquet')`.
When a reference table is loaded, main.py also rewrites "reference" to its absolute Parquet path.
Both rewrites happen before execution. AI always uses "dataset" as table name, no semicolon.

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
- Table name is always "dataset" (reference table is "reference" if loaded)
- No semicolon at end of query
- Returns JSON: {status, sql, message, warnings}
- LIKE patterns preferred over = or IN for string matching (CMS asterisk contamination)

### Insights Generation
- Input: column names, types, numeric stats ONLY — no sample rows, no top values (privacy)
- Output: JSON array of insight objects, max 5
- Each insight: {type, headline, explanation, sql, chart_type, priority}
- Types in priority order: concentration, outliers, trend, skew, missing_data, correlation
- Headlines must be plain English, no jargon

### Suggestions
- Schema-grounded analytical questions
- Results cached in dataset_context.json under "suggested_questions" key

### Caching pattern (used by insights, suggestions, grain_description)
- First call: runs AI, caches in dataset_context.json under the relevant key
- Subsequent calls: returns from cache instantly
- ?refresh=true bypasses cache; falls back to cache if AI fails

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
- Never leave bare except: pass blocks — always log the error

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
- Do not build a general multi-dataset engine — keep it to one dataset + one reference table

---

## DEVELOPMENT ENVIRONMENT
- Windows 11
- Project root: C:\dev\analytics-workbench
- Virtual environment: .venv
- Launch command: uvicorn backend.app.main:app --reload --port 8000
- App URL: http://127.0.0.1:8000/ui/
- Batch file: start-dev.bat in project root

---

## WHEN CONTRIBUTING
1. Always read CONTEXT.md at session start — current status, backlog, next actions
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
