# Analytics Workbench — Shared Context File
# This file is the bridge between Claude Code (coding) and Claude.ai (business development).
# Update it at the end of every session in either environment. One or two lines is enough.
# Claude Code: this file is referenced in CLAUDE.md and should be read at session start.
# Claude.ai: paste this file at the start of any session where product state matters.

---

## PRODUCT STATE

**Current milestone:** Milestone 4 complete. M5 planning in progress.

**M4 features and status:**
- Insights View — COMPLETE
- Export Passport — COMPLETE
- Reference Table JOIN — COMPLETE — mechanics validated March 2026
- Privacy and Transparency Layer — COMPLETE
- Result Passport — COMPLETE (display-cap bug fixed in v1.5.0)
- Reference Table Library — COMPLETE v1 (IRA drug list, 35 drugs Rounds 1-3)
- Bug #2 (NOT LIKE chains) — FIXED
- Bug #3 (ORDER BY DESC parser error) — FIXED
- Bug #4 (Result Passport display-cap) — FIXED
- Bug #5 (Rollup row detection) — FIXED (new feature, not a bug fix per se)
- Bug #6 (Windows file lock on Refresh Datasets) — partially addressed, medium priority
- Bug #7 (Reference Library tables not registering in DuckDB) — FIXED. Tables showed
  in UI but were not queryable. Fix applied in Claude Code session same day.
- Bug #8 (Reference Library case mismatch) — FIXED v1.5.1. Reference table string
  columns now title-cased on import. JOINs match CMS data without LOWER() wrappers.
- Bug #10 (Reference table not queryable after load/restart) — FIXED v1.5.6. Two causes:
  (1) EXPLAIN validation missing reference view, (2) frontend not sending reference name
  after restart. Backend now auto-detects loaded reference tables from REFERENCES_DIR.
- Bug #11 (AI using APPROX_PERCENTILE_CONT) — FIXED v1.5.6. Function doesn't exist in
  DuckDB. Prompt updated to use PERCENTILE_CONT/QUANTILE_CONT/MEDIAN.
- Bug #12 (ORDER BY DESC regression) — FIXED v1.7.1. Expanded _SQL_KW keyword list.
  6 regression tests.
- Bug #13 (Session Log recording insight previews as query_run) — FIXED v1.6.2. Insight
  card mini-previews marked internal + double-fetchInsights race condition guard added.

**Reference Table JOIN validation:**
- Mechanics test: PASSED (Part B, March 2026)
- Cold-start: INFORMALLY VALIDATED — Part D/GUARD analysis (March 2026) used
  fresh dataset and fresh policy research with no prior knowledge. Real analytical
  work, not a controlled test. Formal controlled test is nice-to-have, not a blocker.

**Validated at scale:** 220M rows, DuckDB local execution, sub-second import.

**Test suite:** 607 automated tests, all passing (zero xfail), runs under 10 seconds.
Pre-commit and pre-push git hooks enforce green suite on every commit and push.

---

## REFERENCE TABLE LIBRARY — STATUS AND ROADMAP

**v1 shipped (M4):**
- ira_negotiated_drugs.csv — 35 drugs, IRA Rounds 1-3, prices and effective dates
- Frontend: library browser popover in sidebar
- Backend: /api/reference_library endpoints

**Planned for M5 (priority order):**
1. FDA orphan drug status — needed for Farragut Deliverable 2, currently applied
   from training knowledge only. High churn risk as new designations are added.
2. Manufacturer MFN deal status — needed for Farragut Deliverable 3, currently
   applied from web research. Changes as new deals are announced (irregular cadence).
3. USP category mappings — both 7-category GLOBE list and 17-category GUARD list.
   Currently applied via manual CASE statements. Stable; updates with USP MMG versions.
4. Biosimilar tracker — needed for single-source filtering. Currently applied from
   training knowledge. FDA biosimilar approvals are the update trigger.

**Update cadence:**
- IRA list: annual (new drug selections each February)
- MFN deal status: irregular (announce-driven, roughly monthly during active periods)
- Orphan drug: quarterly FDA OOPD publication
- Biosimilar: monthly FDA approval announcements
- USP categories: every 3 years (USP MMG revision cycle)

**Business note:** Maintained reference library is a recurring reason for customers
to stay on maintenance contracts. Position as a living resource, not a static file.

---

## NEW PRODUCT IDEAS — FROM CHATGPT COMPARISON (March 2026)

Identified by comparing the AW/Claude approach to a parallel ChatGPT session on
the same Compass dataset. Four new product directions.

### 1. Generic Name Pattern Classifier
**What it is:** Auto-classify drugs into therapeutic categories using generic name
suffix patterns. Well-established pharmaceutical naming conventions map reliably:
- glutide → GLP-1 / Diabetes / Obesity
- flozin → SGLT2
- gliptin → DPP-4
- mab / umab → Autoimmune / Biologic
- nib → Oncology (TKI / kinase inhibitor)
- ciclib → Oncology (CDK inhibitor)
- limid → Oncology (IMiD)
- aban → Anticoagulant
- vir → Antiviral
- pril / sartan → Cardiovascular (ACE/ARB)

**Where it lives:** Insights view — when a drug spending dataset is imported, AW
auto-attempts classification and surfaces a suggested therapy_class column in the
first insight card. Covers ~70-80% of branded drugs; remainder flagged for review.

**Why it matters:** Therapeutic category classification was the most time-consuming
step in both the GLOBE and GUARD analyses. Manual CASE statements or external
research required. A pattern classifier eliminates this for the majority of drugs
and makes classification a first-class AW feature rather than an analyst task.

**Build estimate:** Medium. Pattern library is a CSV; matching logic is simple SQL
or Python. UI integration into Insights view is the heavier lift.

### 2. Human-in-the-Loop Classification Workflow
**What it is:** When auto-classification leaves an unclassified residual (the "Other"
bucket), AW presents those items for manual review with suggested categories. Analyst
clicks to accept or override. Classifications are saved per dataset.

**Why it matters:** Both the AW session and the ChatGPT session ended with a large
"Other" residual — 24 drugs in ChatGPT's analysis. Pattern matching will never
classify everything. A structured review workflow makes the residual manageable
rather than just flagging it and walking away.

**Relationship to pattern classifier:** These two features are companions.
Classifier handles the automatable portion; human-in-the-loop handles the rest.

**Build estimate:** Medium-large. Requires a new UI component and per-dataset
classification storage.

### 3. Analysis Summary Artifact
**What it is:** A structured, exportable summary of an analytical session capturing:
- Which filters were applied and why
- What was excluded and the reason
- Key findings at each step
- Open questions flagged for review

Essentially what our Farragut memos contain — but auto-generated from the session
rather than written manually.

**Why it matters:** The ChatGPT session produced a handoff prompt as its primary
deliverable. That's useful but manual. AW sessions already contain all the
information needed to auto-generate this — the queries run, the row counts at each
step, the reference table joins applied, the result summaries. Packaging that
automatically would make every AW session self-documenting.

**Relationship to existing features:** Companion to Export Passport (documents the
dataset) and Result Passport (documents a query result). Analysis Summary documents
the session. Together they form a complete analytical audit trail.

**Build estimate:** Medium. Most data is already available; the work is structuring
and formatting it into a readable output.

### 4. Two Analytical Modes — Exploration vs. Verification
**What it is:** Not a feature per se, but a product design principle that emerged
from the comparison.

The ChatGPT approach excels at exploration — quick segmentation, pattern finding,
generating hypotheses. AW currently excels at verification — applying specific
criteria to produce defensible, auditable output.

The Insights view should lean harder into exploration. Rather than running
predefined insight types (concentration, outliers, trend), it should:
- Propose segmentation schemes based on column patterns
- Flag concentration and outlier hypotheses for the analyst to investigate
- Suggest classification approaches before the analyst has to ask

This makes AW genuinely complementary to ChatGPT-style tools rather than
competing with them on different terms.

**Design implication:** When planning Insights view enhancements, bias toward
exploratory, hypothesis-generating cards over descriptive summary cards.

---

## MILESTONE 5 — PRIVACY ARCHITECTURE (PLANNED)

### Component 1 — Result Passport (M4 — COMPLETE)

### Component 2 — Local AI Mode via Ollama
True air-gap for Tier 3 customers. **Build estimate:** 1-2 weeks.

**AI Mode Switch — session-level design (decided March 2026):**
- Mode is selected once at session start (or first import). Cannot be toggled mid-session.
- Two modes: Local (Ollama) and Cloud (OpenAI/Claude API)
- Mode is displayed persistently in UI so analyst always knows which is active
- Mode declaration is written to session log and Result Passport
- UX: prompt on session start — "Select AI Mode for this session" with plain-language
  privacy description for each option
- Rationale: session-level lock enables clean audit trail ("all queries in this session
  ran in Local mode"), simplifies analyst workflow, satisfies Tier 3 compliance needs

**Provider architecture:** Current `provider_openai.py` is the cloud backend.
Add `provider_ollama.py` with same interface. Mode switch toggles active provider.

### Component 3 — Session Log — COMPLETE (v1.5.6)
Persistent, append-only record of everything that happened in an AW session.
**Status:** Built and wired in. 14 endpoints instrumented. Auto-saves every 10
events. Exports on shutdown. 3 new endpoints: /api/session, /api/session/export,
/api/session/summary. 15 tests. No frontend UI yet (v1.1 enhancement).

**What it captures:**
- Session start time and AI mode selected
- Each dataset imported (name, rows, columns, timestamp)
- Each reference table loaded
- Each query run (SQL, row count returned, timestamp)
- Each export or passport generated
- Session end time

**Design principles:**
- Append-only during session — no editing
- Exported automatically at session end (or on demand)
- Feeds into Analysis Summary Artifact (auto-generated memo from session activity)
- Feeds into Session File (machine-executable replay — see below)
- Complements Result Passport (per-query) and Export Passport (per-dataset)
- Together these three form the complete analytical audit trail

**Build estimate:** Small-medium. Most data already flows through AW — work is
capturing it systematically and writing to a structured log format.

### Component 3a — Session File (Reproducible Session + Test Harness) — AUTOMATIC MODE COMPLETE (v1.6.0)
Machine-executable version of the Session Log. Every step recorded as a replayable
instruction sequence. Inspired by session file pattern from prior enterprise software.
**Status:** Automatic replay mode built. SessionReplayEngine replays query_run,
reference_load, reference_delete events. Schema mismatch detection, baseline
annotation, stop-on-failure. 3 new endpoints. 23 tests. Interactive and Tutorial
modes pending (need frontend UI).

**Two modes of use:**

**1. Reproducible analysis**
- Record a complete analysis session, verify results are correct, save as session file
- Replay against same or updated data to reproduce results exactly
- "Show me how you got this number" → run the session file
- Recurring analyses (monthly reports, quarterly updates) become one-click replays
- Send a colleague the session file — they run it locally and get the same analysis
- For Tier 3: replay in front of regulators to demonstrate methodology

**2. End-to-end test suite (workflow testing)**
- Known-good sessions become end-to-end tests for the software itself
- Distinct from unit tests (508, test individual functions) — session file tests
  exercise the complete analytical workflow as a real analyst would run it
- Three levels of testing, each catching different problems:
  - Unit tests → a function returning the wrong value
  - Integration tests → two components not connecting correctly
  - End-to-end / workflow tests → the full workflow producing the wrong result
- You can have all unit tests passing and still have a broken end-to-end workflow —
  session file tests fill that gap
- After every new feature or bug fix, replay recorded sessions and confirm outputs match
- The Compass Part D analysis is the natural first end-to-end test:
  import → IRA exclusion JOIN → 3,549 non-IRA drugs → correct top spenders
- New features get tested by adding a step to an existing session file and replaying
- Proven pattern from prior company: run full session library on every commit

**Replay modes:**
- **Automatic** — run all steps in sequence, confirm outputs match expected values.
  Best for end-to-end testing and recurring analyses where inputs are stable.
- **Interactive** — step through one at a time, analyst confirms each step before
  proceeding. Best for first-time replay or when upstream data has changed.
- **Tutorial** — step through with narration. Each step pauses, explains what is
  about to happen, executes, then explains what just happened and why. Analyst
  follows along and learns the workflow by doing on real data, not toy examples.

**Tutorial Mode details:**
- Narration is AI-generated from session context (what the query does, why this
  step matters in the analysis) with optional analyst annotations added at record time
- The Compass Part D analysis becomes Tutorial #1 — "Analyzing Medicare drug spending
  data end-to-end." New users run it and learn the full workflow on a validated example
- Each major AW capability gets a dedicated tutorial session file:
  - Tutorial 1: Core query workflow (import, insights, natural language query)
  - Tutorial 2: Reference Table JOIN (IRA exclusion, drug classification)
  - Tutorial 3: Export and Passport generation (audit trail, reproducible output)
- Onboarding use: new analyst runs three tutorials in an afternoon, is productive
- Sales use: prospect runs a tutorial on their own data during evaluation —
  experiences the workflow firsthand rather than watching a demo
- Tier 3 consulting engagement: tutorial session files are a standard deliverable

**Tutorial content authoring:**
- AI generates first draft narration from session context automatically
- Analyst can annotate steps at record time to refine or add domain context
- Annotations travel with the session file — shared with the file, not stored separately

**Relationship to existing features:**
- Session Log → human-readable record
- Session File → machine-executable record
- Analysis Summary Artifact → AI-generated narrative from the session
- Together: complete analytical audit trail, reproducible, explainable, testable

**Build estimate:** Medium. Core replay engine is the main lift. Interactive mode
and end-to-end test integration add scope but build on the same foundation.

### Component 3b — Session Library (Demo Mode + Learning + Test Suite) — UI COMPLETE (v1.6.0)
**Status:** Example Cases button + browser + step-through + Run All replay built.
Pending: sample data packaging, v1 library sessions, per-step execution, tutorial narration.
The Session Library is a curated collection of fully documented, replayable session
files built into AW. This is a proven adoption accelerator — validated in prior
enterprise software. The same session files serve three distinct purposes unified
into one system.

**The three-purpose system:**

**Purpose 1 — Demo Mode (sales and evaluation)**
- User opens AW, navigates to Demo Library from within the tool
- Browses sessions organized by domain: healthcare policy, pharmaceutical, finance,
  operations, consulting
- Selects a demo that matches their problem — most prospects find one that applies
- Runs it: steps through a complete, documented engagement on included sample data
- Sees their problem solved before writing a single query
- This is the fastest path from "evaluating the tool" to "I need this tool"
- Key requirement: demos must be domain-relevant enough that a prospect sees
  themselves in them immediately — not generic examples, real workflows

**Purpose 2 — Tutorial / Learning Mode (onboarding)**
- Same session files, same library — tutorial narration layer added
- Every step explains what's happening, why this approach was chosen, what the
  analyst should be thinking
- New user learns by doing on a real workflow, not reading documentation
- Three sessions in an afternoon → analyst is productive
- Proven pattern: dramatically accelerates adoption vs. traditional documentation

**Purpose 3 — Regression Suite (software quality)**
- Same session files run automatically on every code change
- Each session has recorded baseline outputs (row counts, column values, key results)
- If any session produces a different result after a code change, the build fails
- The demo library IS the end-to-end test suite — same artifact, three purposes
- Every new feature gets a new or updated session file — documentation and test
  in one step
- Validated pattern from prior company: full library run on every commit

**UI Specification (decided March 2026):**
- "Example Cases" button in sidebar, positioned just below Saved Queries
- Click opens a dialog showing browsable list of available cases by category
- User selects a case — step-by-step view opens
- Each step shows:
  - Pre-step narration: what is about to happen
  - Action executes live in the AW environment (query runs, data loads, JOIN fires —
    user sees real results on screen in real time, not a video or simulation)
  - Post-step explanation: what just happened and why this step matters
- User controls pace — Next Step button, moves through at their own speed
- Everything happens in the live tool — the actual AW interface responds at each step

**Phase 2 — User-generated examples:**
- "Save as Example Case" option at session end (or on demand)
- User adds: title, category, per-step annotations
- Saved to local library first
- Future: submit to curated library or share with colleagues
- This is how the library grows organically beyond the built-in cases
- Organized by vertical/domain, not by feature
- Each session: sample dataset included, full step sequence, narration, baseline outputs
- v1 sessions (healthcare policy pack):
  - "Medicare Part D Drug Spending Analysis" — the Compass workflow end-to-end
  - "IRA Drug Exclusion with Reference Table JOIN" — policy filtering workflow
  - "Therapeutic Category Classification" — USP mapping and CASE logic
  - "High-Spend Concentration Analysis" — outlier identification
  - "Export and Audit Trail Generation" — full passport workflow

**Library grows through real work:**
- Every validated client engagement is a candidate library session
- Compass → healthcare policy pack
- Casey's healthcare org engagement → Demo #2 in healthcare pack
- Each vertical requires its own pack to be immediately recognizable to prospects
- Consulting engagements produce custom sessions — billable work that also builds
  the library
- Maintenance contract value: new sessions released on cadence

**Example Cases and the end-to-end test suite are the same thing:**
- Every Example Case is automatically part of the end-to-end test suite
- No separate test files to maintain — the demo library IS the test suite
- Add a case to the library → it is immediately a test
- Automatic replay mode runs every Example Case and confirms outputs match baseline
- A case failing = something broke in that workflow
- New feature added → new or updated Example Case → immediately tested
- Demos and tests are permanently in sync — they cannot drift apart
- Claude Code runs the full Example Case suite on every commit, alongside the
  existing 508-test unit suite — two levels, both enforced

**Schema mismatch handling (critical build requirement):**
- Sessions recorded on one dataset may not match prospect's column names
- Replay engine must detect mismatches gracefully before running
- Auto-map by name similarity or prompt analyst to confirm mappings
- A broken demo is worse than no demo — reliability is non-negotiable here

**Build estimate:** Medium-large. Depends on Session File engine (Component 3a).
Library browser UI, sample data packaging, baseline output recording, and schema
mismatch handling are the main additions beyond the core replay engine.

### Component 4 — In-App Analyst Chat
Full collaboration loop inside AW. Result Passport as context bridge.
**Build estimate:** 2-3 weeks. After Local AI Mode.

### Component 5 — Reference Table Library (M4 v1 COMPLETE — M5 expansion planned)
Additional files: orphan drugs, biosimilar tracker, USP categories, MFN status.

---

## THREE-TIER PRIVACY STORY (Go-to-Market)

| Tier | Customer | Privacy Level | What it means |
|------|----------|---------------|---------------|
| 1 | Consultants / freelancers | Schema-only AI | AI sees column names + stats only. Data stays local. |
| 2 | Mid-market finance / ops | Result Passport | Analyst collaboration loop works without raw data leaving. |
| 3 | Healthcare / government | Local AI mode | Zero external API calls. Full air-gap. Auditable. |

---

## BUSINESS STATE

**Stage:** Pre-revenue. First customer meetings in progress.

**Pricing structure:**
- Tier 1 (consultants/freelancers): $300–800 one-time + 15–20% annual maintenance
- Tier 2 (mid-market finance/ops): $1,000–2,500 per seat + maintenance
- Tier 3 (healthcare/government): $2,000–5,000 per seat + consulting engagement
- Consulting/onboarding: $150–300/hr or fixed project fee

**Business model:** One-time desktop license + annual maintenance + consulting engagements.
Customer supplies their own OpenAI API key (Tiers 1-2) or Ollama install (Tier 3).

**Active pipeline:**
- Healthcare operations team meeting — March 2026 (Tier 3, first confirmed)
- Compass/Farragut CMS Medicare workflow — substantially complete

**Reference use case:** CMS Part B/D drug spending analysis — IRA exclusion lists,
therapeutic category classification, GLOBE/GUARD payment model candidates.
Validated March 2026. Both Part B and Part D memos delivered.

---

## COMPASS/FARRAGUT DELIVERABLE STATUS

**Part B / GLOBE memo — COMPLETE**
- 57 confirmed GLOBE candidates, 2 borderline (Farragut review needed)
- 14 sole orphan drugs, manufacturer and MFN flags applied
- Word memo delivered March 18, 2026

**Part D / GUARD memo — COMPLETE (preliminary)**
- 304 preliminary GUARD candidates, $125.9B combined 2023 spending
- Upper-bound — Tot_Mftr proxy for single-source; Avalere estimate ~170 drugs / ~$93B
- 3 vaccines and Wegovy flagged for Farragut confirmation
- IRA Round 3 not yet applied — Farragut to check
- Orphan drug status and MFN flags not yet applied
- Word memo delivered March 18, 2026

**Remaining:**
- Farragut confirmation on 5 flagged items
- Orphan drug + MFN flags for Part D list

---

## GO-TO-MARKET POSITION

**Primary positioning:**
"Local-first AI analytics for analysts who work with sensitive data.
Import your files, get insights automatically, explore with natural language —
without your data ever leaving your machine."

**Core differentiator:** Privacy architecture — three tiers, each honest.
**Correct language:** "AI generates analysis instructions that run on your computer."

**Target sequence:** Consultants/freelancers → Mid-market finance/ops → Healthcare/government
**University beachhead:** Planned

---

## LAST SESSION LOG
# Append one line per session. Most recent at top. Format: [DATE] [ENV] — summary.

[2026-03-20] [CODE] — Large feature batch: Tutorial #3 (USP Classification, 68 classified drugs,
  9 categories). 3 new example cases from stress parquets — Logistics (Supply Chain), Retail
  (E-Commerce), SaaS (Technology) — each 10K-row sample with 5 queries + narration. Named
  Snapshots (save/list/restore/delete named workspace states). Collapsible Example Cases groups
  with toggle arrows + case counts. 7 example cases across 4 domains. 9 new tests (snapshot
  CRUD, workspace CRUD, example case validation). 607 tests. v1.9.0.
[2026-03-20] [CODE] — Post-v1.8.0 fixes: taxi tutorial EXTRACT(HOUR) cast (CSV imports datetime
  as VARCHAR, needs ::TIMESTAMP), tutorial dataset import now shows file size + source in summary
  cards (calls loadDatasetMeta after import). Both pushed.
[2026-03-20] [CODE] — Bug #15/#16 final fixes + Tutorial #2 (Part B GLOBE) + Workspace Snapshot
  (M5 Component 6) + NYC Taxi example case (10K-row sample, 5 queries) + category-grouped
  Example Cases browser. Workspace auto-saves on shutdown, resume prompt on next launch.
  4 example cases across 2 domains (Medicare & Drug Policy, Transportation & Operations).
  New endpoints: workspace CRUD, example case import_dataset/import_reference. 598 tests. v1.8.0.
[2026-03-20] [BD] — Tutorial #1 validated end-to-end in live AW: clean state entry, live dataset
  import (single dataset in sidebar), live reference load, all 5 queries execute with results
  visible, narration at each step. Bug #15/#16 confirmed fixed. Button heights confirmed
  matching. Ready for Tutorial #2 (Part B GLOBE Candidates).
[2026-03-20] [CODE] — Bug #15 real fix: query_run steps now call runSqlQuery() directly instead
  of duplicating logic. Bug #16 fix: dataset_import sets datasets array directly instead of
  calling loadDatasets() which fetched all backend datasets. Button rows use explicit height:36px
  with align-items:stretch so both buttons match. 598 tests.
[2026-03-20] [CODE] — Bug #14/#15: Tutorial mode rewritten as true live replay from clean state.
  No pre-loading — each step (dataset_import, reference_load, query_run, export) executes live.
  Two-click flow: execute + show post-narration, then advance. 2 new backend endpoints
  (import_dataset, import_reference per example case). Run All also replays from clean. 598 tests.
[2026-03-20] [BD] — Tutorial #1 recorded live and validated: Part D IRA Exclusion,
  5 queries, baseline row counts confirmed (272/10/33/243/50). Session wired to
  Example Cases package by Claude Code in v1.7.2. CLAUDE.md reviewed — needs update
  after sprint settles to reflect M5 repo structure and storage changes.
[2026-03-20] [CODE] — Tutorial #1 wired end-to-end: session JSON with narration + baselines for
  Part D IRA Exclusion (11 events, 5 queries with row count baselines: 272/10/33/243/50).
  New endpoint GET /api/example_cases/{id}/session. Tutorial/Run All buttons execute client-side
  with baseline validation. Save Session flush-to-disk fix (fsync). Consistent sidebar button
  sizing (min-width on secondary buttons). 598 tests. v1.7.2.
[2026-03-20] [BD] — Recording Tutorial #1 (Part D IRA Exclusion) against packaged sample
  data in example_cases directory. Dataset loads as 'part_d_spending_sample' — naming
  convention to standardize across all three packages. Five queries validated against
  500-row sample. v1.7.1 fixes confirmed: buttons styled, DESC fixed, cols fixed.
[2026-03-20] [CODE] — UI polish + bug fixes: sidebar button styling (Refresh/Library/Save now proper
  buttons), Bug #12 fix (ORDER BY DESC — expanded _SQL_KW keyword list + 6 regression tests),
  Bug #13 tests (suggestions don't log query_run), reference "undefined cols" fix (column_count
  in all API responses + frontend fallback), tutorial step-through fix (auto-skip non-replayable
  steps), session naming on shutdown (prompt if unnamed), 598 tests. v1.7.1.
[2026-03-20] [CODE] — Example Cases with real sample data: 3 curated cases (Part D IRA Exclusion,
  Part B GLOBE Candidates, USP Category Classification) with 500-row CMS samples + reference CSVs.
  Sidebar reorganized: Example Cases in Get Started, Sessions has Current Session/Save/Retrieve.
  Separate directories: data/example_cases/ (curated) vs data/sessions/ (analyst's own). 3 new
  backend endpoints. Retrieve Session browser for saved sessions. 590 tests. v1.7.0.
[2026-03-20] [CODE] — Fixed 3 xfail bugs: strip_trailing_special_chars pandas>=2.0 fix, AI consent
  enforced server-side for insights + suggestions. 6 new tests (session name endpoint, internal
  SQL flag). Zero xfails for first time. 576 tests. v1.6.3.
[2026-03-20] [CODE] — Sessions section added to sidebar (Example Cases, Save Session, Session Log).
  Resume mode: restore full session state (dataset, references, last query). Tutorial panel
  restores state before step-through. Bug #13 fixed (insight previews logged as query_run).
  Header tagline updated. RECORD.md created with full project summary. /wrap now appends
  wrap records. 567 tests. v1.6.2.
[2026-03-19] [CODE] — Sidebar redesign: Get Started section (Welcome + Example Cases) at top,
  Workspace (nav), Data (datasets + references). Welcome card with onboarding content, auto-opens
  on first launch (no datasets). Compact DATA buttons (2 rows instead of 4). Example Cases UI
  with step-through + Run All replay. Session File engine + Session Log. Bug #10 + #11 fixed.
  108 commercial tests. /wrap auto-pushes. Permissions configured. 557 tests. v1.6.1.
[2026-03-19] [BD] — Session Log validated end-to-end: session JSON confirmed capturing
  session_start, dataset_import, insights_generated, query_run, reference_load, export
  events with SQL, row counts, elapsed times, user, machine, AI mode. All correct.
  Session File replay engine not yet built — no UI. Next build item.
[2026-03-19] [CODE] — Session File replay engine (M5 Component 3a) built: automatic replay mode,
  schema mismatch detection, baseline annotation. Replays query_run, reference_load,
  reference_delete events. 3 new endpoints (/api/session/files, replay, annotate).
  Bug #10 real fix (auto-detect reference tables). Bug #11 fix (AI percentile prompt).
  Permissions configured. 557 tests. v1.6.0.
[2026-03-19] [BD] — usp_guard_categories JOIN validated: 8 matches on Part B dataset.
  Categories correct. Reference table must be manually loaded each session — auto-load
  on startup is a UX improvement to spec.
[2026-03-19] [CODE] — Bug #10 real fix: backend auto-detects reference tables from REFERENCES_DIR
  when frontend doesn't pass reference name (e.g. after restart). Bug #11: AI percentile prompt
  fixed. Session Log built (M5 Component 3): 14 event types, 14 endpoints instrumented, 3 new
  endpoints, auto-save + shutdown export. 108 commercial tests added earlier. Packaged build
  rebuilt twice. Claude Code permissions configured. 534 tests. v1.5.6.
[2026-03-18] [BD] — Example Cases explicitly linked to end-to-end test suite:
  demo library IS the test suite. Add a case = add a test. Run suite = run all demos.
  Two-level testing: 508 unit tests + full Example Case suite, both on every commit.
[2026-03-18] [BD] — Example Cases UI specced: sidebar button below Saved Queries,
  dialog with browsable library, step-by-step execution in live AW environment,
  per-step narration and explanation. Phase 2: user-generated cases saved locally.
[2026-03-18] [BD] — Terminology clarified: Session File tests = end-to-end tests
  (workflow tests), distinct from unit tests. Three levels: unit → integration →
  end-to-end. Session Library = end-to-end test suite + demo library + tutorial system.
[2026-03-18] [BD] — Session Library fully specced: three-purpose system — Demo Mode
  (sales/evaluation), Tutorial Mode (onboarding), Regression Suite (engineering).
  Same session files serve all three. Proven pattern from prior company. Every client
  engagement is a library candidate. Compass = healthcare policy pack v1 session #1.
[2026-03-18] [BD] — Tutorial Mode added to Session File spec: narrated step-by-step
  replay for onboarding and sales demos. Three standard tutorials defined. Compass
  analysis = Tutorial #1. AI-generated narration + analyst annotations.
[2026-03-18] [BD] — Session File specced: machine-executable replay of session steps.
  Dual use — reproducible analysis AND software test harness for regression testing.
  Automatic and interactive replay modes. Compass analysis = first regression session.
[2026-03-18] [BD] — M5 spec updated: AI Mode Switch (session-level lock, Local/Cloud)
  and Session Log component specced. Mode selected once at session start, locked for
  duration, written to audit trail. Session Log feeds Analysis Summary Artifact.
[2026-03-18] [CODE] — Packaged build observability: startup log + /api/health now show all dir paths
  including reference_library_dir. BUILD_RELEASE.bat adds explicit library sync step. New
  SYNC_LIBRARY.bat for post-build CSV sync without full rebuild. 508 tests. v1.5.5.
[2026-03-18] [CODE] — Commercial test hardening: 108 new tests across 5 files (import pipeline,
  context builder, AI parsers, reference JOIN integration, export correctness, audit log, profile
  depth, scan/register, robustness). Pre-commit + pre-push hooks added. 3 xfail findings:
  strip_trailing_chars broken on pandas>=2.0, AI consent not enforced server-side. 508 tests. v1.5.4.
[2026-03-18] [CODE] — Diagnosed Reference Library file-not-showing bug: packaged build reads from
  dist/data/reference_library/, not source data/. Synced files to dist. No code change needed. 400 tests. v1.5.3.
[2026-03-18] [CODE] — Reference Library auto-discover: CSVs dropped into library dir appear
  without manifest editing. GLOBE exclusions + 4 new library CSVs added. Pattern classifier
  specced. 400 tests. v1.5.2.
[2026-03-18] [CODE] — Bug #8 fix: reference table string columns title-cased on import.
  3 new tests (Bug #7 regression + Bug #8 unit + end-to-end JOIN). 397 tests. v1.5.1.
[2026-03-18] [BD] — Bug #8 validated in AW: INNER JOIN returns matches with no LOWER()
  needed. Reference Library fully functional end-to-end. New CSV files pending load test.
[2026-03-18] [BD] — Reference Table Library CSV drafts built: usp_globe_categories (78 drugs,
  7 categories), usp_guard_categories (102 drugs, 17 categories), orphan_drug_status (65 drugs).
  From training knowledge — needs spot-check vs FDA OOPD / USP MMG before production use.
[2026-03-18] [BD] — Healthcare meeting materials built: 8-slide deck, one-page leave-behind,
  full talking points with demo script. Privacy/air-gap angle. Tier 3 audience (VP + new analyst).
  Compass CMS analysis used as proof of concept throughout.
[2026-03-18] [BD] — Reference Library end-to-end validation: Bug #7 (DuckDB registration)
  found and fixed. Case mismatch (Bug #8) identified — LOWER() workaround confirmed working.
  IRA JOIN against Part D dataset fully validated: 3,549 non-IRA drugs, clean grain.
[2026-03-18] [BD] — ChatGPT comparison session: identified 4 new product directions —
  generic name pattern classifier, human-in-the-loop classification, Analysis Summary
  artifact, exploration vs. verification design principle. All added to roadmap.
[2026-03-18] [CODE] — Cleared full friction backlog: display-cap fix, rollup detection,
  JOIN match diagnostic, Reference Table Library v1 (IRA 35 drugs). 389 tests. v1.5.0.
[2026-03-18] [BD] — Post-session friction analysis: 4 product improvements specced.
[2026-03-18] [BD] — Part D / GUARD analysis complete. 304 candidates. GUARD memo delivered.
  Display-cap bug identified. AW Partner Template updated to v3.
[2026-03-18] [BD] — Reference Table JOIN mechanics test PASSED. Cold-start informally validated.
[2026-03-18] [CODE] — Result Passport + Privacy Layer built. v1.4.0.
[2026-03-18] [CODE] — Reference Table JOIN built. v1.3.0.
[2026-03-18] [BD] — Milestone 5 privacy architecture specced. Three-tier story defined.
[2026-03-18] [BD] — Part B GLOBE analysis complete. 57 candidates. GLOBE memo delivered.
[2026-03-18] [CODE] — M4 status audit. No code changes.
[2026-03-18] [CODE] — Verified wrap.md and CONTEXT.md bridge. No code changes.
[2026-03-18] [BD] — Established context bridge system.

---

## OPEN DECISIONS

- [ ] Session File tutorial narration — AI-generated only, or analyst-annotated first?
- [ ] Session File replay — automatic vs interactive as default mode?
- [ ] Session File format — JSON, SQL script, or proprietary? Needs to be readable and portable.
- [ ] Demo build timeline — M4 complete, mechanics passed. Formal cold-start needed
      before demo, or is current validation sufficient?
- [ ] Healthcare meeting pricing — $3k/seat or higher given compliance angle?
- [ ] Proposal template needed before first Tier 2/3 meeting
- [ ] University outreach — program, contact, ask?
- [ ] Local AI mode quality bar — Ollama good enough for Tier 3?
- [ ] In-App Chat — replaces or complements claude.ai partnership workflow?
- [ ] Farragut confirmations — 5 flagged items pending
- [ ] GUARD orphan drug and MFN flags — not yet applied
- [ ] Generic name pattern classifier — build in M5 or as M4.1 patch?
- [ ] Analysis Summary artifact — scope: auto-generated or analyst-curated?
- [ ] Reference Table Library maintenance model — who updates, how distributed?

---

## NEXT ACTIONS

**Business development (Claude.ai):**
- ~~Build healthcare meeting prep materials (three-tier privacy story + Compass demo)~~ COMPLETE
- Apply orphan drug + MFN flags to GUARD Part D candidate list
- Await Farragut confirmation on flagged items
- Draft one-pager for Tier 1 consultant outreach
- Draft proposal and contract template skeleton

**Product / code (Claude Code):**
- Standardize example case dataset naming: 'part_d_spending_sample' →
  'part_d_spending_by_drug_sample' across all three example case packages for consistency
- ~~Add recorded session JSONs to example case packages~~ Tutorial #1 COMPLETE v1.7.2
- Record Tutorial #2 (Part B GLOBE Candidates) and Tutorial #3 (USP Classification)
- ~~Session File (Component 3a) — automatic replay mode~~ COMPLETE v1.6.0
- ~~Session Library (Component 3b) — Example Cases button + browser UI~~ COMPLETE v1.7.0
- ~~Tutorial narration + baseline validation~~ COMPLETE v1.7.2
- Session Library remaining work:
  - Every Example Case automatically included in end-to-end test suite
- UX improvement: auto-load previously active reference tables on session start
- Build AI Mode Switch (Component 2 addition) — session-level Local/Cloud toggle
- Build additional Reference Table Library files (MFN deal status, biosimilar tracker)
- Spot-check usp_globe_categories, usp_guard_categories, orphan_drug_status CSVs
  against FDA OOPD and USP MMG before adding to production library
- Spec Generic Name Pattern Classifier
- Spec Human-in-the-Loop Classification Workflow
- Spec Analysis Summary Artifact
- ~~Build Session Log (Component 3) — append-only record of session activity~~ COMPLETE v1.5.6
- ~~Fix Bug #8 (case mismatch in Reference Library JOIN)~~ COMPLETE v1.5.1
- ~~Add regression test for Bug #7 (Reference Library DuckDB registration)~~ COMPLETE v1.5.1
- Fix Bug #6 (Windows file lock on Refresh Datasets)
- Spec Milestone 5 Local AI Mode (Ollama) — provider_ollama.py
- Spec Milestone 5 In-App Analyst Chat

---

## HOW TO USE THIS FILE

**At the end of a Claude Code session**, append to Last Session Log:
[DATE] [CODE] — what was built, what changed, what broke, what's next

**At the end of a Claude.ai session**, append to Last Session Log:
[DATE] [BD] — decision made, document produced, or strategy updated

**At the start of any session**, read this file first.
For Claude Code: add to CLAUDE.md → See CONTEXT.md for current product and business state.
For Claude.ai: paste the full file or the relevant sections.

**To trigger a context update:** say "update the context file" at end of session.
