# Analytics Workbench — Development Record

## Project Summary (as of March 20, 2026)

**Analytics Workbench** is a local-first, AI-assisted analytics desktop application.
An analyst imports a dataset (CSV, Excel, Parquet), gets automatic insights, queries
with natural language or SQL, and exports results with a full audit trail. All data
stays on the analyst's machine — AI generates analysis instructions, not data transfers.

**Tech stack:** FastAPI backend, DuckDB query engine, OpenAI GPT-4.1-mini for AI,
vanilla HTML/JS/CSS frontend, PyInstaller packaging for Windows desktop.

**Current version:** v1.12.0 | **Total commits:** 115+ | **Test suite:** 603 tests (zero xfail)

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

## Bugs Fixed (13 resolved, 1 active)

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
| #17 | Session/snapshot restore shows wrong datasets | list_datasets() tightened + restore ordering fixed | v1.10.1 |
| #6 | Windows file lock on Refresh Datasets | Mitigated — Refresh no longer calls delete endpoint (v1.10.2) | MITIGATED |
| #18 | Refresh Datasets deleted files from disk + _restoreWorkspace missing expand + loadDatasets auto-select wrong dataset | Refresh → UI-only clear, _restoreWorkspace expand added, loadDatasets clears selectedDataset when missing | v1.10.2 |

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
| v1.8.0 | 2026-03-20 | Bug #15/#16 fixes, Tutorial #2, Workspace Snapshot, NYC Taxi case, category groups |
| v1.8.1 | 2026-03-20 | Taxi TIMESTAMP cast fix, tutorial summary cards show file size + source |
| v1.9.0 | 2026-03-20 | Tutorial #3, 3 new example cases, Named Snapshots, collapsible groups, 607 tests |
| v1.10.0 | 2026-03-20 | Reference Guide, SESSIONS restructure, Exit button, collapsible sidebar, /sync skill |
| v1.10.1 | 2026-03-21 | Bug #17: session/snapshot/workspace restore shows wrong datasets — root cause fix |
| v1.10.2 | 2026-03-21 | Bug #18: Refresh was deleting disk files (root cause of restore loop); Clear SQL button; Sessions Save exits; SQL auto-clears on restore |
| v1.10.3 | 2026-03-21 | Chart tab disabled until query returns chartable result; Sessions Save no longer exits; restore filters dataset list to session dataset only |
| v1.11.0 | 2026-03-21 | SESSIONS sidebar removed; snapshots retired; Welcome card is session hub (Resume + Save); Reference Guide as slide-in drawer; 603 tests |
| v1.12.0 | 2026-03-22 | Resume Session duplicate fix; Clear Workspace sidebar button; resume restores Ask Your Data question; 603 tests |

---

## Wrap Records
<!-- Each /wrap appends a 3-line summary below. Most recent at top. -->

**v1.12.0** | 2026-03-22
Bug fix: Resume Session dropdown no longer shows duplicates — UUID auto-save files with a session
name set were leaking through the named-session filter; fixed with filename pattern matching.
New: Clear Workspace button in sidebar footer (full-width, above Resume/Save As). Session resume now
restores the last natural-language question to Ask Your Data (blank if none). 603 tests.

**v1.11.0** | 2026-03-21
Major UX simplification: SESSIONS sidebar section removed; snapshots retired. Welcome card is now
the session hub — Resume Session (dropdown of all named sessions + Open button) and Save Session
(name field + Save button). Reference Guide converted to right-side slide-in drawer. Exit closes
immediately; Save navigates to Welcome + focuses name field. 4 snapshot endpoints + 4 tests removed. 603 tests.

**v1.10.3** | 2026-03-21
Chart tab greys out and is non-functional by default and after any query that doesn't produce a
chartable result (exactly 2 cols, 2–50 rows, categorical+numeric or date+numeric). Sessions sidebar
Save no longer exits the app. Restore paths filter dataset list to session's dataset only. 607 tests.

**v1.10.2** | 2026-03-21
Bug #18 fix: Refresh Datasets was calling /api/datasets/{name}/delete (rmtree) for every dataset,
permanently destroying files — the root cause of the restore-loop. Refresh is now a UI-only clear.
Also: Clear SQL button (pill overlay in editor), Sessions Save exits after saving, SQL editor
auto-clears on restore when no saved SQL present. Reference Guide updated. 607 tests.

**v1.10.1** | 2026-03-21
Bug #17 fix: session/snapshot/workspace restore showed wrong datasets. Root cause: list_datasets()
matched raw data dirs as datasets + restore set selectedDataset after loadDatasets(). Both fixed.
Session restore bails early on missing dataset. Troubleshooting row added to Reference Guide. 607 tests.

**v1.10.0** | 2026-03-20
Reference Guide (full in-app product documentation). SESSIONS restructured to 4 buttons in 2 rows.
Exit button with 3-step save prompts replaces Quit. Collapsible sidebar sections with smart
auto-expand. Example Cases groups collapsed by default. /wrap updated, /sync skill added. 607 tests.

**v1.9.0** | 2026-03-20
Large feature batch: Tutorial #3 (USP Classification). 3 new example cases (Logistics, Retail,
SaaS) from stress parquets. Named Snapshots (save/restore/delete). Collapsible Example Cases
groups. 7 cases across 4 domains. 9 new tests. 607 tests.

**v1.8.1** | 2026-03-20
Taxi tutorial EXTRACT(HOUR) fix — CSV imports datetime as VARCHAR, needs ::TIMESTAMP cast.
Tutorial dataset import now calls loadDatasetMeta() so summary cards show file size and source.
git push permission moved from ask to allow for automatic /wrap pushes. 598 tests.

**v1.8.0** | 2026-03-20
Bug #15/#16 final fixes (tutorial runs queries live, clean state). Tutorial #2 (Part B GLOBE).
Workspace Snapshot — auto-save on shutdown, resume prompt on launch. NYC Taxi example case
(10K-row sample, 5 queries). Category-grouped Example Cases browser. 4 cases, 2 domains. 598 tests.

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

