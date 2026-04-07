# Analytics Workbench — Development Record

## Project Summary (as of March 20, 2026)

**Analytics Workbench** is a local-first, AI-assisted analytics desktop application.
An analyst imports a dataset (CSV, Excel, Parquet), gets automatic insights, queries
with natural language or SQL, and exports results with a full audit trail. All data
stays on the analyst's machine — AI generates analysis instructions, not data transfers.

**Tech stack:** FastAPI backend, DuckDB query engine, OpenAI GPT-4.1-mini for AI,
vanilla HTML/JS/CSS frontend, PyInstaller packaging for Windows desktop.

**Current version:** v1.20.1 | **Total commits:** 130+ | **Test suite:** 1,090 tests (three suites, AI accuracy 100%)

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
| v1.20.1 | 2026-04-06 | BUG-010/011 fixed: config.enc in .gitignore, corrupted/wrong-machine key auto-deleted; 11 new key_manager tests; 883 tests |
| v1.20.0 | 2026-04-06 | Customer API key management: Fernet-encrypted storage, first-launch overlay, Settings panel, 402 guard on all AI endpoints, developer key removed; 872 tests |
| v1.19.2 | 2026-04-02 | Logo refresh: new JetWare AI branded logo in both asset dirs, cache-busting ?v=2 on img refs; 872 tests |
| v1.19.1 | 2026-04-01 | BUG-001 fix: Result Narrative race condition in demo runner — await _fetchResultNarrative() in query_run and ai_ask handlers; 1,079 tests |
| v1.19.0 | 2026-03-29 | Three-suite test infrastructure (1,100 tests); Bugs #9/#10/#11 permanently fixed; query accuracy golden dataset suite; AI accuracy 100% (20/20); pre-push hook |
| v1.18.1 | 2026-03-24 | PyInstaller DLL portability fix — dynamic pythonXYZ.dll lookup, works on Python 3.13 and 3.14; pyarrow.tests excluded; BUILD_RELEASE.bat verification dynamic; 642 tests |
| v1.18.0 | 2026-03-24 | JetWare AI logo, tutorial integration test suite, 4 new event types (explain/chart/save/load), Tutorial #4 +6 steps, Tutorial #6 +5 steps, 3 engine guards, DATE CAST AI prompt fix; 642 tests |
| v1.17.0 | 2026-03-24 | Tutorial #6 Parameterized Workflow Retail, PyInstaller build hardening, 22 ai_ask conversions across 7 demos, Clear Workspace closes Insights, Library button style fix; 611 tests |
| v1.16.0 | 2026-03-24 | Tutorial #5 Real Estate, About button, Library button, awPrompt extra field, Reference Guide workflow docs, shimmer buttons; 611 tests |
| v1.15.0 | 2026-03-23 | M5 Demo Sprint: unified Workflows dialog, workflow replay engine, reference table sidebar items, Edit panel, session isolation, PyInstaller fix; 603 tests |
| v1.14.0 | 2026-03-23 | Tutorial #4 Multi-State Medicaid Diligence; 8 example cases; 4 reference tables; narrated 12-step session; Farragut demo ready; 603 tests |
| v1.13.0 | 2026-03-22 | Multi-dataset UNION/JOIN backend; schema normalization JOIN validated (TX/FL/OH); reference bleed-through fix on resume; 603 tests |
| v1.12.1 | 2026-03-22 | Custom tooltip system; descriptive tooltips on all buttons; popover visual polish; Clear Workspace completeness; .gitignore runtime data; 603 tests |
| v1.12.0 | 2026-03-22 | Resume Session duplicate fix; Clear Workspace sidebar button; resume restores Ask Your Data question; 603 tests |

---

## Wrap Records
<!-- Each /wrap appends a 3-line summary below. Most recent at top. -->

**v1.20.1** | 2026-04-06
BUG-010: config.enc added to .gitignore. BUG-011: corrupted/wrong-machine config.enc
auto-deleted by has_key() and get_key(), triggers first-launch overlay. 11 new tests.

**v1.20.0** | 2026-04-06
Customer API key management. Fernet-encrypted key storage at %APPDATA%\JetWareAI\config.enc with
machine-specific derivation. First-launch setup overlay, Settings sidebar panel, 402 guard on all
AI endpoints. Developer key removed from all code paths. cryptography added to deps + PyInstaller spec.

**v1.19.2** | 2026-04-02
Logo refresh: replaced jetware_logo.png in frontend/assets/ and src/assets/ with new JetWare AI
branded logo. Added cache-busting ?v=2 on both img references. CSS sizing reverted to original.

**v1.19.1** | 2026-04-01
BUG-001 fixed: Result Narrative race condition in demo runner. _fetchResultNarrative() was
fire-and-forget; now awaited in both query_run and ai_ask handlers. Root cause: CONCURRENCY.

**v1.19.0** | 2026-03-29
Three-suite test infrastructure: unit (872), feature+workflow (185), query accuracy (43). Bugs #9/#10/#11
permanently fixed: Insights column-name agnostic, reference table title-casing removed (LOWER() at query
time), ZIP/NPI/FIPS/phone/*_ID/*_CODE forced to VARCHAR on import. AI accuracy 100% (20/20).

**v1.18.1** | 2026-03-24
PyInstaller Python DLL portability fix: AnalyticsWorkbench.spec now dynamically resolves the
correct pythonXYZ.dll at build time (4-path search: python_dir → sys.base_prefix → SYSTEMROOT →
System32) rather than hardcoding python313.dll. upx_exclude and binaries both use the dynamic name.
BUILD_RELEASE.bat post-build verification now queries Python for the version-correct DLL name.
Builds on Python 3.13 (desktop) and Python 3.14 (laptop). pyarrow.tests excluded from bundle.

**v1.18.0** | 2026-03-24
JetWare AI logo replaces text header + Welcome card heading. Tutorial integration test suite
(test_tutorial_queries.py) validates every query_run SQL against sample data — found and fixed 3 bad
baselines. 4 new tutorial event types: explain, chart_view, query_save, query_load with full playback
handlers. Tutorial #4 gains 6 steps (bar chart, explain, FL monthly trend, line chart, query save/load).
Tutorial #6 gains 5 steps (2 bar charts, explain, query save/load). 3 engine-level guards added to
_executeTutorialStep: auto-close Insights, auto-restore Table tab after Chart, auto-close Explain panel.
DuckDB DATE CAST guidance added to AI SQL prompt (CSV date columns are VARCHAR). Reference Guide updated
for parameterized workflow tutorial. PyInstaller spec updated with src/assets. 642 tests.

**v1.17.0** | 2026-03-24
Tutorial #6: Parameterized Workflow — Retail Sales Performance (electronics → sporting goods via Edit
panel, 4 CSVs, 13-step session, all baselines validated). PyInstaller build hardened: python313.dll
force-bundled from sys.base_prefix, upx_exclude expanded to 14 entries, BUILD_RELEASE.bat rewritten
(process kill, dist/build wipe, post-build DLL verification), graceful shutdown handlers added. 3
consecutive clean builds verified. 22 query_run → ai_ask conversions across 7 example case sessions.
Features Exercised section added for Tutorial #6. Clear Workspace closes Insights panel. Library button
matched to Reference Table style. 611 tests.

**v1.16.0** | 2026-03-24
Tutorial #5: Real Estate Market Analysis — multi-phase tutorial with Austin analysis + Denver workflow
reuse via Edit panel. About button on workflow cards (expandable description panel). Library button
re-added for reference tables. awPrompt enhanced with optional second textarea for workflow descriptions.
Reference Guide expanded with full Workflows documentation (Record/Edit/Reuse, Edit Panel, Reusable
Analysis Patterns). Shimmer button animations. 7 example case session.json files enriched with Schema
inspect and Insights steps. 8 new tests for example case file validation. 611 tests.

**v1.15.0** | 2026-03-23
M5 Demo Sprint: unified Workflows dialog (Stored + Example Workflows with Step Through/Run All/Resume/
Edit/Delete). Workflow replay engine with session isolation and 3-tier reference restore. Reference
tables as individual sidebar items with REF badge. Workflow Edit slide-in panel for dataset remapping.
Session save captures all_datasets + all_references. Clear Workspace resets session + closes panels.
SQL editor stays open during playback. Baseline mismatch = warning not failure. PyInstaller fix
(python313.dll bundled). Session Log checkbox toggle. Refresh/Library buttons removed. 6 new endpoints.

**v1.14.0** | 2026-03-23
Tutorial #4: Multi-State Medicaid Diligence added to Session Library. 500-row samples for
TX/FL/OH claims + all 4 reference tables (medicaid_schema_map, service_category_map, mco_lookup,
audit_risk_flags). 12-step narrated session: 3 dataset imports, 4 reference loads, 5 analytical
queries, export, Result Passport, session log. Baselines validated from sample data. Narration
embedded at Steps 4 (schema normalization), 7 (audit risk JOIN), 8 (cross-state reimbursement),
12 (audit trail). Farragut PE diligence demo is ready to run. 8 example cases total. 603 tests.

**v1.13.0** | 2026-03-22
Multi-dataset UNION/JOIN: extended `_rewrite_sql_dataset_reference()` to resolve any registered
dataset name in FROM/JOIN — enables cross-state UNION queries (TX+FL+OH 13,000 rows validated).
Schema normalization JOIN validated Phase 2: title-case normalization requires UPPER() on both sides;
OH ZIP_CODE is numeric (needs ::VARCHAR in UNION); all three states return correct canonical columns
with zero data loss. Reference bleed-through on session resume fixed (always reset before restore).

**v1.12.1** | 2026-03-22
UX polish: custom tooltip system replaces native browser title attributes with styled, animated
tooltips. Descriptive tooltips added to every interactive element (sidebar nav, toolbar buttons,
action buttons). Popover/suggestion chip visual refinements. Clear Workspace now fully resets results
table, row count, explain panel, and chart. .gitignore updated to exclude runtime data files. 603 tests.

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

