# Analytics Workbench — Development Record

## Project Summary (as of March 20, 2026)

**Analytics Workbench** is a local-first, AI-assisted analytics desktop application.
An analyst imports a dataset (CSV, Excel, Parquet), gets automatic insights, queries
with natural language or SQL, and exports results with a full audit trail. All data
stays on the analyst's machine — AI generates analysis instructions, not data transfers.

**Tech stack:** FastAPI backend, DuckDB query engine, OpenAI GPT-4.1-mini for AI,
vanilla HTML/JS/CSS frontend, PyInstaller packaging for Windows desktop.

**Current version:** v1.7.2 | **Total commits:** 100+ | **Test suite:** 598 tests (zero xfail)

---

## What Has Been Built

### Milestone 3 — Core Analytics Workflow (COMPLETE)
The foundational import-query-export pipeline:
- Dataset import (CSV, Excel, TSV, Parquet) with Parquet conversion
- Dataset inspection (Profile, Schema, Preview)
- AI SQL generation with 4-route fallback chain
- AI-powered question suggestions with caching
- Manual SQL editing and execution via DuckDB
- Results table with automatic bar/line chart recommendations (Chart.js)
- Export to Excel and TSV
- Validated on 220M row / 9.5GB Parquet files

### Milestone 4 — AI-Assisted Insight Workflow (COMPLETE)
Transformed from a query tool into an insight tool:
- **Insights View** — 3-5 AI-generated insight cards on dataset load (concentration,
  outliers, trends, skew, missing data, correlation). Each card has headline,
  explanation, inline chart, and "Explore in Query" drill-down.
- **Reference Table JOIN** — import a lookup CSV, JOIN it against primary dataset.
  One reference table at a time. Enables IRA exclusion lists, USP category mappings,
  manufacturer flags. Title-case normalization on import for case-insensitive JOINs.
- **Reference Table Library** — pre-built CSVs shipped with AW. Auto-discover: drop
  a CSV in the folder, it appears in the browser. v1 library: IRA negotiated drugs,
  GLOBE/GUARD exclusions, USP categories, orphan drug status. 9 library files.
- **Export Passport** — 9-section JSON profile of a dataset (identity, schema, grain
  description, quality flags, time-series families, SQL quickstart, numeric ranges,
  distributions, sample values). AI-generated grain description cached.
- **Result Passport** — per-column profile of query results (top values, stats,
  null rates, quality flags). Copies to clipboard for AI collaboration.
- **Privacy & Transparency Layer** — schema-only mode for insights (no raw data sent
  to AI), permanent privacy disclosure in UI, per-dataset AI consent toggle.

### Milestone 5 — Sessions & Reproducibility (IN PROGRESS)
Building toward reproducible, auditable, shareable analytical sessions:
- **Session Log** (Component 3, COMPLETE v1.5.6) — append-only record of every session
  event (imports, queries, reference loads, exports). 14 endpoints instrumented.
  Auto-saves every 10 events. Exports on shutdown. Named sessions with descriptions.
- **Session File Replay Engine** (Component 3a, COMPLETE v1.6.0) — re-executes recorded
  sessions against live data. Automatic replay mode compares row counts to baselines.
  Schema mismatch pre-flight detection. Baseline annotation. Stop-on-failure mode.
- **Example Cases UI** (Component 3b, COMPLETE v1.6.0) — "Example Cases" button in
  sidebar Sessions section. Library browser with Resume/Tutorial/Run All per session.
  Bottom tutorial panel for step-through replay with live AW UI response.
  Resume mode restores full session state (dataset, references, last query/results).
  Save Session dialog for naming and describing sessions.
- **Session Log View** — live event log in main panel, auto-refreshes every 5 seconds.

### UI & Developer Experience
- **Sidebar** — 4 sections: Get Started (Welcome), Workspace (nav), Data (datasets +
  references), Sessions (Example Cases, Save Session, Session Log)
- **Welcome card** — onboarding content, auto-opens on first launch (no datasets)
- **Compact DATA buttons** — Import + Refresh in one row, Reference + Library in second
- **Header tagline** — "Your data never leaves your machine."
- **567 automated tests** across 21 test files, pre-commit and pre-push hooks enforced
- **Claude Code permissions** — allow/ask/deny rules, dontAsk mode, .env hard-denied

---

## Bugs Fixed (12 resolved, 1 active)

| Bug | Issue | Fix | Version |
|-----|-------|-----|---------|
| #1 | Silent SQL failure on invalid DuckDB syntax | Surface actual error | — |
| #2 | Long NOT LIKE/IN chains (~26 conditions) | Strip quoted strings before keyword scan | — |
| #3 | ORDER BY DESC parser error | Fix SQL wrapping | — |
| #4 | Suggestions button caching | Cache pattern | — |
| #5 | Result Passport display-cap | total_rowcount parameter | v1.5.0 |
| #7 | Reference Library not registering in DuckDB | Preserve table names as aliases in SQL rewrite | v1.5.1 |
| #8 | Reference Library case mismatch on JOIN | Title-case string columns on import | v1.5.1 |
| #10 | Reference table not queryable after restart | Auto-detect from REFERENCES_DIR + EXPLAIN reference view | v1.5.6 |
| #11 | AI using APPROX_PERCENTILE_CONT | Prompt updated for correct DuckDB syntax | v1.5.6 |
| #12 | ORDER BY DESC regression — keywords captured as aliases | Expanded _SQL_KW list with 20+ SQL keywords | v1.7.1 |
| #13 | Session Log recording all suggestions as query_run | Insight previews marked internal, double-fetch guard | v1.6.1 |
| #6 | Windows file lock on Refresh Datasets | Partially addressed — retry logic, needs more testing | ACTIVE |

---

## Business State

- **Stage:** Pre-revenue. First customer meeting scheduled (healthcare operations, Tier 3).
- **Pricing:** $300-800 (Tier 1) / $1K-2.5K (Tier 2) / $2K-5K (Tier 3) per seat + maintenance.
- **Reference use case:** CMS Medicare Part B/D drug spending — Compass/Farragut engagement.
  Part B GLOBE memo (57 candidates) and Part D GUARD memo (304 candidates) delivered March 2026.
- **Three-tier privacy story:** Schema-only AI → Result Passport → Local AI (Ollama).

---

## Version History

| Version | Date | Highlights |
|---------|------|------------|
| v1.0.0 | — | Baseline: import, query, export |
| v1.3.0 | 2026-03-18 | Reference Table JOIN |
| v1.4.0 | 2026-03-18 | Result Passport + Privacy Layer |
| v1.5.0 | 2026-03-18 | Friction backlog cleared, Reference Library v1, 389 tests |
| v1.5.1 | 2026-03-18 | Bug #7/#8 fixes, 397 tests |
| v1.5.2 | 2026-03-18 | Auto-discover library CSVs, 400 tests |
| v1.5.3 | 2026-03-18 | Packaged build diagnosis, 3 new library CSVs |
| v1.5.4 | 2026-03-19 | 108 commercial tests, pre-commit/pre-push hooks, 508 tests |
| v1.5.5 | 2026-03-19 | Build observability, SYNC_LIBRARY.bat |
| v1.5.6 | 2026-03-19 | Session Log, Bug #10/#11 fixes, 534 tests |
| v1.6.0 | 2026-03-19 | Session File replay engine, Example Cases UI, 557 tests |
| v1.6.1 | 2026-03-19 | Sidebar redesign, Welcome card, compact buttons, Bug #13 fix, 567 tests |
| v1.6.2 | 2026-03-20 | Sessions section, Resume mode, Bug #13 fix, RECORD.md, 567 tests |
| v1.6.3 | 2026-03-20 | 3 xfail fixes, zero xfails, 576 tests |
| v1.7.0 | 2026-03-20 | Example Cases with real CMS sample data, sidebar reorg, 590 tests |
| v1.7.1 | 2026-03-20 | UI polish, Bug #12 ORDER BY DESC, reference cols fix, tutorial step-through, 598 tests |
| v1.7.2 | 2026-03-20 | Tutorial #1 session JSON, tutorial/Run All wiring, Save flush-to-disk, button sizing |

---

## Wrap Records
<!-- Each /wrap appends a 3-line summary below. Most recent at top. -->

**v1.7.2** | 2026-03-20
UI polish + Bug #12 ORDER BY DESC fix + Tutorial #1 wired end-to-end. Session JSON
with narration and baseline row counts (272/10/33/243/50). Save flush-to-disk (fsync).
New endpoint for example case sessions. Consistent sidebar button sizing. 598 tests.

**v1.7.0** | 2026-03-20
Example Cases with real CMS sample data: 3 curated cases (Part D IRA, Part B GLOBE,
USP Classification) with 500-row datasets + reference CSVs. Sidebar reorganized.
Separate example_cases/ and sessions/ directories. Retrieve Session browser. 590 tests.

**v1.6.3** | 2026-03-20
Fixed 3 xfail bugs: pandas>=2.0 string dtype, AI consent server-side enforcement.
6 new tests for session name endpoint + internal SQL flag. Suite fully green —
576 tests, zero xfails for the first time.

**v1.6.2** | 2026-03-20
Sessions section + Resume mode + Bug #13 fix. Sidebar restructured with Sessions section
(Example Cases, Save Session, Session Log). Resume restores full session state. Tutorial
panel restores state before step-through. RECORD.md created. 567 tests.

