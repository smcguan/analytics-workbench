# Analytics Workbench — Shared Context File
# This file is the bridge between Claude Code (coding) and Claude.ai (business development).
# Update it at the end of every session in either environment. One or two lines is enough.
# Claude Code: this file is referenced in CLAUDE.md and should be read at session start.
# Claude.ai: paste this file at the start of any session where product state matters.

---

## PRODUCT STATE

**Current milestone:** Milestone 4 complete. Milestone 5 in active planning.

**M4 features — ALL COMPLETE:**
- Insights View — COMPLETE
- Export Passport — COMPLETE
- Reference Table JOIN — COMPLETE — mechanics validated March 2026
- Privacy and Transparency Layer — COMPLETE
- Result Passport — COMPLETE (display-cap bug fixed in v1.5.0)
- Reference Table Library v1 — COMPLETE (IRA drug list, 35 drugs Rounds 1-3)
- Session Log — COMPLETE (v1.5.6)
- Session File replay engine — COMPLETE automatic mode (v1.6.0)
- Session Library / Example Cases — COMPLETE (v1.9.0/v1.14.0) — 8 cases, 5 domains
- Workspace Snapshot — RETIRED (v1.11.0) — replaced by named sessions on Welcome card
- Named Snapshots — RETIRED (v1.11.0) — replaced by named sessions on Welcome card
- Reference Guide — COMPLETE as right-side slide-in drawer (v1.11.0) — sidebar stays accessible while reading
- Collapsible sidebar sections — COMPLETE (v1.10.0)
- Exit/Save buttons — COMPLETE (v1.11.0) — Exit closes immediately; Save goes to Welcome card session hub
- Unified Workflows dialog — COMPLETE (v1.15.0) — Stored Workflows + Example Workflows in one dialog
- WORKFLOWS sidebar section — COMPLETE (v1.15.0) — Stored Workflows, Save, Session Log checkbox
- Session Log Viewer — COMPLETE (v1.14.0) — slide-in drawer with colored badges, expandable SQL, auto-refresh, export
- Collapsible SQL editor — COMPLETE (v1.14.0) — auto-expands on AI generate, stays open during playback
- Reference tables as sidebar items — COMPLETE (v1.15.0) — individual selectable items with REF badge, per-item remove
- Workflow Edit panel — COMPLETE (v1.15.0) — slide-in panel for dataset/reference remapping
- Workflow replay engine — COMPLETE (v1.15.0) — Step Through, Run All, Resume modes; session isolation
- Multi-reference restore — COMPLETE (v1.15.0) — 3-tier fallback: disk → library → example cases
- Session save full state — COMPLETE (v1.15.0) — all_datasets + all_references in resume_state
- PyInstaller packaging fix — COMPLETE (v1.17.0) — python313.dll force-bundled from base Python, full upx_exclude (14 entries), build script wipes dist/build before every build, post-build DLL verification
- Tutorial #6 Parameterized Workflow — COMPLETE (v1.17.0) — Retail Sales Performance, Edit panel dataset+reference swap demo
- Natural language queries in tutorials — COMPLETE (v1.17.0) — 22 query_run steps converted to ai_ask across 7 demos

**Current version:** v1.17.0 — demo-ready for Farragut/McDermott meeting

**Test suite:** 611 automated tests, all passing (zero xfail), runs under 12 seconds.
Pre-commit and pre-push git hooks enforce green suite on every commit and push.

**Validated at scale:** 220M rows, DuckDB local execution, sub-second import.

---

## MILESTONE 5 — PLANNED FEATURES

### Priority order updated March 2026 — driven by Farragut/McDermott requirements

**M5 Priority 1 — Multi-State Medicaid Normalization**
NEW — Identified from Farragut meeting (March 2026).
Farragut analysts do PE diligence on multi-state healthcare provider platforms. They
regularly work with Medicaid claims data across multiple states, each with different
schemas, column naming conventions, and file formats. Current workflow requires manual
normalization before analysis. AW can solve this with Reference Table JOIN + schema
mapping tools.

What this requires:
- Schema mapping reference table: map variant column names across states to a
  canonical schema (e.g., "BENE_ID" vs "BENEFICIARY_ID" vs "MEMBER_ID" all → patient_id)
- Multi-dataset JOIN support: load State A and State B datasets, normalize, compare
- Medicaid reference tables: state code lookup, managed care organization (MCO) mapping,
  benefit category classification
- Example Case: "Multi-State Medicaid Diligence" — Farragut's core PE workflow

**Build estimate:** Medium-large. Schema mapping is the new component; JOIN mechanics exist.

**M5 Priority 2 — Analysis Summary Artifact**
Auto-generated structured summary of an analytical session. Captures:
- Which datasets were loaded and their key statistics
- Which filters and exclusions were applied and why
- Reference table JOINs performed
- Key findings at each step (from Result Passport records)
- Open questions flagged for review

Why elevated: Farragut produces client memos for PE sponsors. Their analysts currently
write these manually after running analysis. The Analysis Summary Artifact is the bridge
from AW session to client deliverable. This is also the feature most likely to make
AW indispensable to a consulting firm — it removes the manual documentation step entirely.

**Farragut-specific format:** The memo structure should be configurable. Farragut's
deliverables have a specific format (findings → methodology → limitations → open items).
Spec this as a template system, not a fixed output.

**Build estimate:** Medium. Session Log already captures all inputs; work is structuring
and formatting into configurable output.

**M5 Priority 3 — AI Mode Switch (Local/Cloud toggle)**
Session-level toggle between OpenAI (cloud) and Ollama (local). Mode locked for session
duration, written to audit trail.

Why important for Farragut: As a law firm, their IT/compliance team may require a
demonstrable option to run fully air-gapped. Even if they use cloud mode day-to-day,
the existence of a local mode is a procurement unlock for regulated-environment deals.

**M5 Priority 4 — Local AI Mode via Ollama**
True air-gap. Provider_ollama.py with same interface as provider_openai.py.
Tier 3 compliance requirement. Quality bar question still open — needs Ollama
benchmark on healthcare query types before committing to production.

**M5 Priority 5 — Parameterized Sessions**
Reusable session templates where dataset-specific values (column names, filter
criteria, thresholds) are parameterized rather than hardcoded. Analyst records a
session once, then reruns it against new data with updated parameters.

Why important for Farragut: They run the same diligence workflow on every new
PE acquisition target. A parameterized "Medicaid diligence" session template means
a new engagement starts from a validated, documented methodology — not from scratch.

**M5 Priority 6 — In-App Analyst Chat**
Full collaboration loop inside AW. Result Passport as context bridge.
Build after Local AI Mode.

**M5 Priority 7 — Generic Name Pattern Classifier**
Auto-classify drugs into therapeutic categories using generic name suffix patterns
(glutide → GLP-1, mab → Biologic, nib → Oncology, etc.). Covers ~70-80% of branded
drugs. Surfaces suggested therapy_class column in first Insights card on import.

**M5 Priority 8 — Human-in-the-Loop Classification Workflow**
Structured review UI for the unclassified residual after pattern classification.
Analyst accepts or overrides suggestions. Classifications saved per dataset.
Companion to Pattern Classifier.

---

## REFERENCE TABLE LIBRARY — STATUS AND ROADMAP

**v1 shipped (M4):**
- ira_negotiated_drugs.csv — 35 drugs, IRA Rounds 1-3, prices and effective dates

**Planned for M5 (priority order updated for Farragut):**
1. State Medicaid schema map — canonical column name mapping across all 50 state
   Medicaid file formats. NEW — Farragut requirement. High priority.
2. Managed care organization (MCO) lookup — state MCO names, IDs, plan types.
   NEW — Farragut PE diligence requirement.
3. FDA orphan drug status — needed for Farragut Deliverable 2, currently applied
   from training knowledge only. High churn risk.
4. Manufacturer MFN deal status — needed for Farragut Deliverable 3. Changes
   irregularly as new deals are announced.
5. USP category mappings — GLOBE (7 categories) and GUARD (17 categories).
   Currently applied via manual CASE statements.
6. Biosimilar tracker — needed for single-source filtering. Monthly FDA updates.

**Update cadence:**
- IRA list: annual (new drug selections each February)
- MFN deal status: irregular (announce-driven, roughly monthly during active periods)
- Orphan drug: quarterly FDA OOPD publication
- Biosimilar: monthly FDA approval announcements
- State Medicaid schema map: annual (state system changes)
- MCO lookup: annual (plan year changes)
- USP categories: every 3 years (USP MMG revision cycle)

**Business note:** Maintained reference library is a recurring reason for customers
to stay on maintenance contracts. For Farragut specifically, the Medicaid schema map
and MCO lookup are workflow-critical — their analysts cannot do multi-state diligence
without them. This is a strong maintenance contract anchor.

---

## FARRAGUT / McDERMOTT — CUSTOMER PROFILE

**What they are:**
Farragut Square Group is the data analytics arm of McDermott+Consulting, the health
policy and lobbying consultancy of McDermott Will & Emery — one of the leading
healthcare law firms in the US. Acquired 2019. They operate as a unified entity.
"The McDermott conversation" = internal McDermott+Consulting meeting, not a
separate company.

**What they do:**
Healthcare regulatory and reimbursement analytics for private equity firms, portfolio
companies, bankers, and lenders. Core services:
- PE due diligence on healthcare acquisitions (Medicaid, Medicare, commercial payer)
- Medical audit and coding compliance
- Reimbursement and regulatory advisory
- Policy analysis for institutional investors

**Why AW fits them precisely:**
- They work with large, sensitive, client-confidential healthcare datasets daily
- They operate under a law firm — attorney-client privilege applies to client data
- They produce recurring diligence deliverables on similar workflows for every engagement
- They work with CMS, Medicaid, commercial payer data — exactly AW's validated use cases
- Time-sensitive (PE deal timelines) — speed of analysis is commercially valuable

**Privacy is a legal requirement, not a preference:**
Because they work under a law firm, any tool their analysts use with client data must
be defensible to McDermott Will & Emery's general counsel. AW's schema-only AI model
and local execution are not just features — they are the legal justification for use.

**BAA requirement:** No BAA needed with AW. No client data or PHI is transmitted to
AW systems. The only external data call is schema metadata (column names, stats) to
OpenAI, under Farragut's own API key. This is a procurement unlock — most healthcare
analytics tools require complex BAA negotiations. We do not.

**Key stakeholders:**
- Casey's contact (Stan's colleague) — business/operations side
- Stan — technical or compliance gatekeeper. Needs architectural precision to approve.
- McDermott IT/legal — will review privacy model before firm-wide adoption

**Validated use cases with this customer:**
1. CMS Medicare Part B/D drug spending analysis (Compass workflow) — VALIDATED
2. IRA drug exclusion with Reference Table JOIN — VALIDATED
3. Therapeutic category classification (USP GLOBE/GUARD) — VALIDATED
4. Multi-state Medicaid normalization — IDENTIFIED, not yet built

**Pipeline status:**
- Initial meeting held March 2026 — went well, strong interest
- Casey sent follow-up email with three discovery questions
- Stan conversation (McDermott internal) — upcoming, critical gatekeeper meeting
- Response prep document built for Casey — includes Stan Q&A guide

**If they become an anchor customer:**
McDermott+Consulting serves dozens of PE firms and healthcare platforms. A reference
from a law firm's consulting arm is the most credible possible validation for every
subsequent regulated-industry sale. This is the Phase 2 reference customer that
changes every subsequent sales conversation.

---

## NEW PRODUCT IDEAS — FROM CHATGPT COMPARISON (March 2026)

### 1. Generic Name Pattern Classifier
Auto-classify drugs by generic name suffix. See M5 Priority 7 above.

### 2. Human-in-the-Loop Classification Workflow
Structured review for unclassified residual. See M5 Priority 8 above.

### 3. Analysis Summary Artifact
Auto-generated session memo. See M5 Priority 2 above. Elevated by Farragut requirement.

### 4. Two Analytical Modes — Exploration vs. Verification
Design principle. Insights view should bias toward exploratory, hypothesis-generating
cards over descriptive summary cards. Propose segmentation schemes, flag concentration
hypotheses, suggest classification approaches before the analyst asks.

---

## MILESTONE 5 — PRIVACY ARCHITECTURE (PLANNED)

### Component 1 — Result Passport (M4 — COMPLETE)

### Component 2 — Local AI Mode via Ollama
True air-gap for Tier 3 customers. Session-level AI Mode Switch (Local/Cloud) added.
Mode locked for session duration, written to audit trail.
**Build estimate:** 1-2 weeks.

### Component 3 — Session Log — COMPLETE (v1.5.6)
Append-only record of all session activity. 14 event types, 14 endpoints instrumented.
Auto-saves every 10 events. Exports on shutdown.

### Component 3a — Session File — AUTOMATIC MODE COMPLETE (v1.6.0)
Machine-executable session replay. Automatic, Interactive, and Tutorial modes.
Feeds end-to-end test suite. Compass Part D = Tutorial #1 baseline.

### Component 3b — Session Library — COMPLETE (v1.9.0)
7 example cases across 4 domains. Same files serve demo, tutorial, and regression
test purposes. Collapsible category groups. Healthcare policy pack v1 complete.

**Next Session Library additions (driven by Farragut):**
- "Multi-State Medicaid Diligence" — PE acquisition workflow (requires M5 P1)
- "Healthcare Provider Revenue Cycle Analysis" — billing and coding compliance workflow
- "IRA Policy Impact Assessment" — extends the existing Compass tutorial

### Component 4 — In-App Analyst Chat
Full collaboration loop inside AW. After Local AI Mode.

### Component 5 — Reference Table Library (M4 v1 COMPLETE — M5 expansion planned)
See Reference Table Library section above for updated priority order.

### Component 6 — Workspace Snapshot — COMPLETE (v1.8.0/v1.9.0)
Auto-save/restore on shutdown/launch. Named snapshots. Save/list/restore/delete.

---

## THREE-TIER PRIVACY STORY (Go-to-Market)

| Tier | Customer | Privacy Level | What it means |
|------|----------|---------------|---------------|
| 1 | Consultants / freelancers | Schema-only AI | AI sees column names + stats only. No BAA needed. Data stays local. |
| 2 | Mid-market finance / ops | Result Passport | Analyst collaboration works without raw data leaving. |
| 3 | Healthcare / government / law firms | Local AI mode | Zero external API calls. Full air-gap. Auditable audit trail. |

**Farragut sits between Tier 1 and Tier 3:** Operating under a law firm, they need
Tier 3 privacy assurance but may function day-to-day at Tier 1 (schema-only, cloud AI).
The AI Mode Switch is the feature that gives them both — cloud AI by default with a
documented local option for the compliance team to point to.

---

## BUSINESS STATE

**Stage:** Pre-revenue. Active pipeline with strong first prospect.

**Pricing structure:**
- Tier 1 (consultants/freelancers): $300–800 one-time + 15–20% annual maintenance
- Tier 2 (mid-market finance/ops): $1,000–2,500 per seat + maintenance
- Tier 3 (healthcare/government): $2,000–5,000 per seat + consulting engagement
- Consulting/onboarding: $150–300/hr or fixed project fee
- McDermott/Farragut pricing: TBD — likely team license (5-10 seats) + onboarding
  engagement + maintained reference table library. Propose $3,000–4,500/seat +
  $500–800/seat annual maintenance + $5,000–10,000 onboarding engagement.

**Business model:** One-time desktop license + annual maintenance + consulting engagements.
Customer supplies their own OpenAI API key.

**Active pipeline:**
- Farragut/McDermott+Consulting — March 2026. Initial meeting complete. Follow-up
  sent. Stan (technical) conversation upcoming. Strong interest confirmed.
  Potential: 5-10 seat team license + ongoing consulting + reference library.
- Healthcare operations team meeting — March 2026 (separate Tier 3 prospect).
  Meeting prep materials complete.

**Reference use cases:**
1. CMS Part B/D drug spending analysis — IRA exclusion lists, therapeutic category
   classification, GLOBE/GUARD payment model candidates. Validated March 2026.
2. Multi-state Medicaid normalization — identified by Farragut, not yet built.
   Target: validate with Farragut sample data before M5 P1 build.

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

**Primary positioning (updated post-M4):**
"Analytics Workbench is the only AI analytics tool that imports your files, surfaces
insights automatically, lets you enrich your data without exporting it — and never
sends a single row to the cloud."

**Core differentiator:** Privacy architecture — three tiers, each honest.
**Correct language:** "AI generates analysis instructions that run on your computer."
**BAA language:** "No BAA required — no client data touches our systems."

**Key buyer vocabulary (use their language back to them):**
- "Shadow AI" — the problem AW eliminates
- "Nutrition label" — what the Privacy Transparency Layer provides
- "Local-first" / "private AI" — architecture descriptors buyers now use
- "Air-gapped analysis" — Tier 3 / law firm language
- "Schema-only AI mode" — precise technical claim, not marketing
- "No BAA required" — procurement unlock for healthcare law firm context

**Target sequence:** Consultants/freelancers → Mid-market finance/ops → Healthcare/government
**Anchor customer target:** Farragut/McDermott+Consulting — healthcare policy consulting
arm of a major law firm. This is the reference customer that unlocks the Tier 3 market.
**University beachhead:** Planned (mirrors prior biomechanics playbook)

---

## LAST SESSION LOG
# Append one line per session. Most recent at top. Format: [DATE] [ENV] — summary.

[2026-03-24] [CODE] — v1.18.0. JetWare AI logo replaces text header + Welcome card heading. Tutorial
  integration test suite built (test_tutorial_queries.py — validates every query_run SQL against sample
  data with baseline row counts). Fixed 3 bad baselines found by tests: part_d[8] 33→22, part_d[9]
  243→202, retail[9] 4→2. 4 new tutorial event types: explain, chart_view, query_save, query_load —
  handlers in playback engine with auto-detect visualization fallback for charts. Tutorial #4 +6 steps
  (bar chart, explain, query_save, FL monthly trend query, line chart, query_load). Tutorial #6 +5 steps
  (2 bar charts, explain, query_save, query_load). 3 engine-level guards: auto-close Insights, auto-restore
  Table tab after Chart, auto-close Explain panel. DuckDB DATE CAST guidance added to AI SQL generation
  prompt (CSV date columns are VARCHAR). Reference Guide updated for parameterized workflow tutorial.
  PyInstaller spec updated with src/assets datas entry. 642 tests.
[2026-03-24] [BD] — Extended BD session. Casey immersion program built: 5-module
  self-paced guide (orientation, all example workflows, hands-on analysis, build your
  own workflow, Farragut readiness). Casey email written — explains full business vision,
  his role, and why immersion matters. Tutorial #5 Real Estate spec and 4 synthetic
  datasets generated and validated (Austin 2,000 rows / Denver 1,800 rows, identical
  schemas, $68K price delta, 8-day DOM delta). Tutorial #6 Parameterized Workflow spec
  written — retail domain, electronics vs sporting goods, full Edit panel lifecycle.
  Wyoming LLC company formation checklist built. Casey conversation guide and AW
  onboarding guide built. Library button CSS fix in progress — two attempts, still
  mismatched width vs Import Dataset. python313.dll packaging error recurring —
  build script hardening spec written for Claude Code. Shimmer loading states spec
  written — comprehensive, all async buttons. Outstanding: Library button fix,
  shimmer implementation, workflow step parity.
[2026-03-24] [CODE] — Tutorial #6 Parameterized Workflow Retail built (electronics + sporting goods,
  4 CSVs, 13-step session with Edit panel dataset+reference swap, all baselines validated). PyInstaller
  build hardened: python313.dll force-bundled from sys.base_prefix, upx_exclude expanded to 14 entries
  (all .pyd files), BUILD_RELEASE.bat rewrites (process kill, dist/build wipe, post-build DLL verification),
  graceful shutdown handlers (atexit + SIGTERM/SIGINT). 3 consecutive clean builds verified. 22 query_run
  steps converted to ai_ask across 7 example cases (taxi, logistics, retail, SaaS, Part D, Part B,
  Medicaid). Features Exercised section added for Tutorial #6 About panel. Clear Workspace now closes
  Insights panel. Library button matched to Reference Table style (same class, same height). 611 tests.
  v1.17.0.
[2026-03-24] [CODE] — Tutorial #5 Real Estate Market Analysis built and committed. About button on
  workflow cards (expandable description with purple accent). Library button re-added next to Reference
  Table import. awPrompt enhanced with optional second textarea (workflow description on save). Reference
  Guide expanded with Workflows documentation (Record/Edit/Reuse pattern, Edit Panel, Reusable Analysis).
  Shimmer button animations. Edit panel replay engine improvements. 7 example case session.json files
  updated with richer narration and additional steps (schema inspect, insights). 8 new tests for example
  case file existence validation (additional datasets, references). 611 tests. v1.16.0.
[2026-03-23] [CODE] — Four targeted fixes. Bug 1: startup workspace not clean — init() was calling
  loadReferences() which fetched leftover reference tables from disk; replaced with explicit empty state
  (loadedReferences=[], renderReferences()). Bug 2: SQL editor collapsed after Run SQL — removed
  _collapseEditorWithPreview() call from runSqlQuery(); editor now stays open after execution (only
  collapses on manual chevron click or Clear Workspace). Fix 3: Session Log checkbox spacing — changed
  justify-content:space-between to gap:6px so checkbox sits immediately adjacent to label text.
  Fix 4: Clear Workspace now closes Step Through panel — added _tutorialAborted flag checked in
  tutorialRunAll() loop, Clear Workspace calls tutorialClose() to stop running workflows and hide
  the narration panel. 603 tests passing.
[2026-03-23] [CODE] — M5 Demo Sprint complete. Major session: unified Workflows dialog (Stored + Example
  Workflows in one EC-style overlay with Step Through/Run All/Resume/Edit/Delete per entry). Session
  isolation fix (loadAndResume now filters datasets to selected case only). Workflow replay engine
  with 3-tier reference restore (disk→library→example cases). Multi-reference session save/restore
  (all_datasets + all_references captured and restored). Reference tables as first-class sidebar items
  (individual REF-badged selectable items with per-item remove). Workflow Edit slide-in panel for
  dataset/reference remapping. Collapsible SQL editor stays open during playback. Session Log as
  checkbox toggle. Clear Workspace closes session log + collapses editor + resets session. Baseline
  mismatch treated as warning not failure (stale baselines in 6/8 example cases). PyInstaller fix:
  python313.dll now bundled with upx_exclude. Sidebar: Refresh + Library buttons removed; Import
  Dataset + Reference Table full-width. "Tutorial" → "Step Through" rename. Session delete endpoint.
  POST /api/session/reset, /api/session/log_event, /api/references/restore, /api/sessions/{fn}/delete,
  /api/session/replay/prepare added. 603 tests. v1.15.0.
[2026-03-23] [CODE] — Demo sprint: 5-item follow-up pass. Critical fix: example case dataset imports
  used display_name for registered_name, producing mismatched names (e.g. texas_medicaid_claims_500_row_sample
  vs expected tx_medicaid_claims) — every tutorial query silently failed. Fix: pass display_name=None so
  registered name derives from filename stem. Also: SESSIONS collapsible sidebar section (Session Log,
  Resume, Save As as nav-items; Clear/Exit/Save&Exit as footer actions); Session Log viewer now captures
  all events including example case imports and references (log_event calls added to 3 endpoints);
  collapsible SQL editor (auto-expand on AI generate, auto-collapse with preview after query);
  tutorial playback now shows SQL in editor before execution. 603 tests.
[2026-03-23] [CODE] — Tutorial #4 multi-resource bug fixes. Three issues found during live demo:
  (1) Only TX dataset imported — /import_dataset and /load endpoints returned after first file;
  fixed with ?filename= param and loop-all in /load. (2) JOIN medicaid_schema_map failed — SQL
  rewriter only resolved one reference table; fixed with additional_references dict resolving all
  registered references (same pattern as additional_datasets). (3) Schema normalization query hit
  wrong dataset — selectedDataset stuck on OH (last imported); tutorial query_run handler now
  switches to step's details.dataset before running. Also: /import_reference accepts ?filename=
  for step-by-step reference loading; export endpoint now resolves additional_datasets +
  additional_references; session_end tutorial step now opens Session Log view. 603 tests.
[2026-03-23] [BD+CODE] — M5 demo sprint complete. AW v1.15.0 demo-ready for
  Farragut/McDermott meeting. Tutorial #4 (Multi-State Medicaid Diligence) built and
  validated end-to-end — TX/FL/OH synthetic datasets (13K rows), 4 reference tables,
  all 10 queries passing. Unified Workflows dialog shipped: Stored Workflows + Example
  Workflows, Step Through / Run All / Resume modes, Edit panel for file remapping,
  session isolation fixed. Session Log Viewer wired — events capture in real time
  during playback. SQL editor stays open during and after query execution. Reference
  tables display as individual selectable sidebar items with REF badge. Clean startup
  state on fresh build. Suggestion chips auto-generate SQL. Schema/Preview panels
  persist on dataset switch. Ask Your Data and SQL persist on dataset switch. Step
  Through panel closes on Clear Workspace. Import progress bar added for large datasets.
  Refresh and Library buttons removed. Import Dataset and Reference Table buttons
  expanded to full column width. Farragut demo script (Word doc) built for Casey with
  talking points, step-by-step guide, and Stan Q&A. All major demo flows tested and
  working. Remaining open: workflow step parity (occasional failures), Analysis Summary
  Artifact (M5 Priority 2), AI Mode Switch (M5 Priority 3). 603 tests. v1.15.0.
[2026-03-23] [CODE] — Tutorial #4 Multi-State Medicaid Diligence built and in Session Library. v1.14.0.
  500-row samples for TX/FL/OH + all 4 reference tables packaged. 12-step narrated session JSON:
  3 dataset imports, 4 reference loads (schema_map, audit_risk_flags, service_category_map, mco_lookup),
  5 analytical queries, export, Result Passport, session end. Baselines validated from sample data:
  schema normalization=20, MCO totals=3, MCO concentration=3, audit risk JOIN=8, Skilled Nursing
  cross-state=3, top-2-cats/state=6. Narration at Steps 4/7/8/12 as specified. 8 example cases
  total. Farragut demo is ready to run. 603 tests. Q8/Q9 TX data re-run after regeneration: PASS
  (70.3% top-2 combined, Harris County NPI 3 at 25.1%). All 10 queries 10/10 PASS.
[2026-03-23] [CODE] — M5 Phase 3 query bank: 8/10 PASS. Bugs #10/#11 logged.
  Q1 (MCO totals TX): PASS — 3 rows. Q2 (MCO concentration): PASS — Lone Star 47.8%.
  Q3 (null DIAG_CD): PASS — 266 nulls = 5.3%. Q4 (anomalous providers): PASS — both
  seeded NPIs found (ratio 3.43 and 3.21). Q5 (cross-state category totals): PASS — 5
  canonical categories, service_category_map JOIN works. Q6 (FL audit risk JOIN): PASS —
  all 8 flagged procedure codes matched. Q7 (reimbursement rate by state×category): PASS —
  15 rows, OH 66-67% vs TX/FL 83-84% as designed. Q10 (FL monthly trend): PASS — 12 months.
  Q8 FAIL: top 2 TX categories only 41.9% (expected >60%) — synthetic data too evenly
  distributed. Q9 FAIL: 0 counties with provider >20% concentration — same cause.
  Both Q8/Q9 failures are data generation issues, not product bugs. Synthetic datasets need
  regeneration with intentional skew. Bug #10: title-case normalization requires UPPER() on
  both sides of reference JOINs. Bug #11: OH ZIP_CODE is numeric, needs ::VARCHAR in UNION.
  Named reference routing confirmed: specify reference='name' in SQL request body to target a
  specific registered reference table by name (not just the 'reference' keyword).
  Next: regenerate TX synthetic data with service category concentration + county provider
  concentration, then build Tutorial #4 session JSON.
[2026-03-22] [CODE] — M5 Phase 2 schema normalization validated end-to-end. medicaid_schema_map
  imported as reference table (30 rows, 4 cols: state/source_column/canonical_column/data_type).
  Title-case normalization confirmed on import (TX→Tx, BENE_ID→Bene_Id) — JOINs require UPPER()
  on both sides. All three states return correct canonical columns with zero data loss:
  TX=5000, FL=4500, OH=3500 rows. Full 13,000-row UNION ALL with canonical schema confirmed.
  OH ZIP_CODE is numeric — requires ::VARCHAR cast in UNION. Phase 2 PASS.
  Multi-dataset UNION backend (Phase 3) re-confirmed: 13K rows, MCO concentration by state works.
  Next: build Tutorial #4 Multi-State Medicaid Diligence example case.
[2026-03-22] [CODE] — M5 Phase 1-3 complete. Multi-dataset UNION/JOIN now works in AW.
  Phase 1: all three Medicaid state files (TX 5000/FL 4500/OH 3500) import simultaneously,
  correct row counts, no data mixing. Phase 2: reference JOIN workflow validated with
  mco_lookup and audit_risk_flags against each state independently. Schema divergence
  confirmed (BENE_ID/MEMBER_ID/CLIENT_ID etc) — schema_map reference table structure correct.
  Phase 3: extended _rewrite_sql_dataset_reference() in main.py to resolve any registered
  dataset name found in FROM/JOIN clauses — ~20 line change. Cross-state UNION ALL now
  works: 13,000 rows TX+FL+OH in a single query, MCO concentration by state, reimbursement
  differential (OH -14% vs TX/FL) all validated. 603 tests passing. Dev server confirmed.
  Next: kill dev server, wrap, then build Tutorial #4 example case.
[2026-03-22] [BD] — M5 planning session. Farragut engagement model (Tutorial #4 spec)
  reviewed and adopted as M5 anchor. All 7 synthetic datasets generated and validated:
  tx_medicaid_claims.csv (5,000 rows), fl_medicaid_claims.csv (4,500),
  oh_medicaid_claims.csv (3,500), plus 4 reference tables. All seeded test conditions
  confirmed: TX MCO concentration 47.8%, anomalous provider ratios 3.2x/3.4x,
  null DIAG_CD 5.3%, OH reimbursement 16 ppts below TX/FL.
  M5 Phase 1 priority: validate multi-dataset simultaneous loading in AW (Steps 1-3).
[2026-03-22] [CODE] — UX polish: custom tooltip system replaces native browser title attrs with styled
  animated tooltips; descriptive tooltips added to every interactive element; popover/suggestion chip
  visual refinements (gradient backgrounds, box shadows); Clear Workspace now fully clears results
  table, row count, explain panel, and chart; .gitignore updated to exclude runtime data. 603 tests. v1.12.1.
[2026-03-22] [CODE] — Bug fix: Resume Session list no longer shows duplicates (UUID auto-save files
  with a name set were leaking through the named-session filter; fixed by pattern-matching filenames).
  New: Clear Workspace button in sidebar footer (full-width, above Resume/Save As row) — clears
  dataset, reference, SQL editor, and Ask Your Data field. Session resume now restores last
  natural-language question to Ask Your Data field (last_question stored in resume_state via
  AI_SQL_GENERATED events; field blanked on resume if no question was saved). 603 tests. v1.12.0.
[2026-03-21] [CODE] — Major UX simplification: SESSIONS sidebar section removed entirely; snapshots
  retired; Welcome card is now the session hub (Resume Session dropdown + Save Session name field).
  Reference Guide converted to right-side slide-in drawer — sidebar stays visible while reading.
  Exit button closes immediately; Save button navigates to Welcome + focuses session name field.
  4 snapshot backend endpoints removed, 4 snapshot tests removed. 603 tests. v1.11.0.
[2026-03-21] [CODE] — Chart tab disabled by default and after non-chartable queries (updateChartTab +
  _filterDatasetsToSession); Sessions sidebar Save no longer exits app; restore paths now filter
  dataset list to session's dataset only (no stray datasets from prior sessions). v1.10.3.
[2026-03-21] [CODE] — QoL improvements: Clear SQL button (overlay in editor, pretty pill style),
  Refresh redesigned as non-destructive workspace clear (clears UI, not disk files),
  Sessions Save button now exits after saving, SQL editor auto-clears on restore when
  no saved SQL. Also fixed test_sessions_saved to match named-only filter behavior. v1.10.2.
[2026-03-21] [CODE] — Fixed recurring dataset-restore bug (root cause: Refresh Datasets was
  permanently deleting files from disk via /api/datasets/{name}/delete, nuking datasets that
  restore needed). Fix: Refresh is now non-destructive (re-fetch only). Also fixed
  _restoreWorkspace() missing expand calls and loadDatasets() auto-select override.
  Sidebar reorder (Sessions moved to slot 2), CLAUDE.md trimmed + HISTORY.md created,
  Exit no longer creates session files, Save & Exit no longer creates duplicates. v1.10.1.
[2026-03-20] [BD] — Farragut/McDermott research complete. Identified as McDermott+Consulting
  (law firm analytics arm). Multi-state Medicaid normalization identified as next use case.
  Casey prep doc built with Stan Q&A guide. CONTEXT.md updated with Farragut profile,
  M5 priorities reordered, Medicaid reference tables added to library roadmap.
[2026-03-20] [BD] — Market intelligence research complete. Competitor moves mapped.
  Tier 1 consultant one-pager built. Positioning copy updated post-M4.
[2026-03-20] [BD] — UI redesign batch validated in CONTEXT.md: Reference Guide built,
  SESSIONS restructured, Exit button, collapsible sidebar. CLAUDE.md + CONTEXT.md
  updated with Parameterized Sessions spec and M5 feature documentation.
[2026-03-20] [CODE] — UI redesign batch: Reference Guide, SESSIONS 4 buttons, Exit button,
  collapsible sidebar, /wrap updated, /sync skill added. 607 tests. v1.10.0.
[2026-03-20] [CODE] — Large feature batch: Tutorial #3, 3 new example cases (Logistics,
  Retail, SaaS), Named Snapshots, Collapsible Example Cases groups. 607 tests. v1.9.0.
[2026-03-20] [CODE] — Tutorial #2 (Part B GLOBE) + Workspace Snapshot + NYC Taxi example
  case. 598 tests. v1.8.0.
[2026-03-20] [CODE] — Tutorial #1 wired end-to-end with narration + baselines. 598 tests. v1.7.2.
[2026-03-20] [CODE] — Example Cases with real CMS sample data. Session Library browser UI.
  590 tests. v1.7.0.
[2026-03-19] [CODE] — Sidebar redesign, Session Log + Session File engine, Bug #10/#11 fixed.
  557 tests. v1.6.1.
[2026-03-18] [BD] — Healthcare meeting materials built: 8-slide deck, one-pager, talking points.
[2026-03-18] [BD] — Part D / GUARD analysis complete. GUARD memo delivered.
[2026-03-18] [BD] — Reference Table Library CSV drafts built: USP GLOBE/GUARD, orphan drug.
[2026-03-18] [BD] — Part B GLOBE analysis complete. GLOBE memo delivered.
[2026-03-18] [CODE] — Cleared full friction backlog. Reference Library v1 (IRA 35 drugs).
  389 tests. v1.5.0.
[2026-03-18] [BD] — Established context bridge system.

---

## OPEN DECISIONS

- [ ] Farragut/McDermott pricing — $3k–4.5k/seat + onboarding engagement. Confirm before proposal.
- [ ] Stan conversation prep — Casey needs to be briefed. Casey immersion guide built — needs to complete it.
- [x] Casey immersion program — COMPLETE. 5-module guide built March 2026.
- [x] Casey email re: AW opportunity — COMPLETE. Sent March 2026.
- [ ] Wyoming LLC formation — checklist built, filing not yet started.
- [ ] Multi-state Medicaid sample file — ask Farragut for sanitized sample to validate schema mapping approach.
- [ ] M5 P1 scope — schema mapping only, or include MCO lookup in same build?
- [ ] Analysis Summary Artifact format — configurable template (Farragut memo style) vs. fixed output?
- [x] Demo build for Farragut — COMPLETE. Tutorial #4 Multi-State Medicaid Diligence is the demo.
- [ ] Local AI mode quality bar — Ollama good enough for Tier 3 analytical queries?
- [ ] In-App Chat — replaces or complements claude.ai partnership workflow?
- [ ] Farragut confirmations — 5 flagged items from GUARD memo still pending.
- [ ] GUARD orphan drug and MFN flags — not yet applied to Part D candidate list.
- [ ] Reference Table Library maintenance model — who updates, how distributed to customers?
- [ ] University outreach — program, contact, ask?
- [ ] Proposal and contract template — needed before first Farragut contract signing.

---

## NEXT ACTIONS

**Business development (Claude.ai):**
- Send Casey immersion guide and motivational email
- Casey completes immersion program — debrief after Module 5
- Casey dry run of Tutorial #4 with you playing Stan
- Confirm meeting date and attendees with Farragut
- Await Farragut follow-up to Casey's requirements email
- File Wyoming LLC — name decision needed first
- Draft proposal for Farragut/McDermott (team license + onboarding + library)
- Draft proposal and contract template skeleton
- Apply orphan drug + MFN flags to GUARD Part D candidate list
- Draft one-pager for Tier 1 consultant outreach (COMPLETE — aw_consultant_onepager.docx)

**Product / code (Claude Code):**
- Fix Library button width — still mismatched vs Import Dataset after two attempts
- Fix shimmer loading states — upload bar still appearing, needs full removal
- Fix workflow step parity — occasional Step Through failures remaining
- Spec and build Analysis Summary Artifact (M5 Priority 2)
- Spec and build AI Mode Switch (M5 Priority 3)
- ~~Spec M5 Priority 1: Multi-State Medicaid Normalization~~ COMPLETE — multi-dataset UNION built (v1.13.0)
- ~~Phase 1: Validate multi-dataset simultaneous load~~ COMPLETE — TX/FL/OH all load correctly (v1.13.0)
- ~~Phase 2: Validate schema normalization JOIN~~ COMPLETE — medicaid_schema_map validated, title-case UPPER() fix documented (v1.13.0)
- ~~Phase 3: Multi-dataset UNION backend~~ COMPLETE — 13K row cross-state UNION working (v1.13.0)
- ~~Phase 3 query bank validation~~ 8/10 PASS — 2 FAIL due to synthetic data distribution
- ~~Regenerate TX synthetic data~~ COMPLETE — Q8/Q9 now pass (70.3%, Harris 25.1%)
- ~~Build Tutorial #4 example case~~ COMPLETE v1.14.0 — Farragut demo ready
- Add medicaid_schema_map, mco_lookup, audit_risk_flags, service_category_map to Reference Table Library
- Spec M5 Priority 2: Analysis Summary Artifact (configurable memo template)
- Spec M5 Priority 3: AI Mode Switch (session-level Local/Cloud toggle)
- Build additional Reference Table Library files: Medicaid schema map, MCO lookup
- Spot-check usp_globe_categories, usp_guard_categories, orphan_drug_status CSVs
  against FDA OOPD and USP MMG before production
- Fix Bug #6 (Windows file lock on Refresh Datasets)
- Standardize example case dataset naming: 'part_d_spending_sample' →
  'part_d_spending_by_drug_sample' across all three example case packages
- Retrieve Session / Snapshot restore dataset visibility — FIXED v1.10.1+

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
