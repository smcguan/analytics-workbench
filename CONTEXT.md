# Analytics Workbench — Shared Context File
# This file is the bridge between Claude Code (coding) and Claude.ai (business development).
# Update it at the end of every session in either environment. One or two lines is enough.
# Claude Code: this file is referenced in CLAUDE.md and should be read at session start.
# Claude.ai: paste this file at the start of any session where product state matters.

---

## PRODUCT STATE

**Current milestone:** Milestone 4 complete. Pending cold-start validation before healthcare demo.

**M4 features and status:**
- Insights View — COMPLETE
- Export Passport — COMPLETE
- Reference Table JOIN — COMPLETE — mechanics validated March 2026
- Privacy and Transparency Layer — COMPLETE
- Result Passport — COMPLETE
- Bug #2 (NOT LIKE chains ~26 conditions) — FIXED
- Bug #3 (ORDER BY DESC parser error) — FIXED

**Reference Table JOIN validation status:**
- Mechanics test: PASSED — 25 exclusions in one combined CSV, single JOIN query,
  94→59 drugs. LIKE pattern handled trailing asterisks. Brand name matching
  correctly caught formulation variants (Enbrel/Enbrel Sureclick, Austedo/Austedo XR).
- Cold-start test: NOT YET RUN — requires a fresh dataset neither analyst nor Claude
  has seen before. Cannot use Compass data — prior knowledge contaminates the test.

**AW display cap issue identified:** Result Passport generates from displayed rows (capped
at 200), not full result set. When query returns >200 rows, Result Summary row count is
wrong. Needs fix — Result Passport should reflect full result count, not display cap.

**Validated at scale:** 220M rows, DuckDB local execution, sub-second import.

**Test suite:** 333 automated tests, all passing, runs under 1 second.

---

## MILESTONE 5 — PRIVACY ARCHITECTURE (PLANNED)

**Goal:** Close the gap between the privacy promise and the real-world analytical
collaboration workflow. Deliver a three-tier privacy story that maps to the three
customer tiers.

### Component 1 — Result Passport (added to M4 — COMPLETE)
Copy Result Summary button in results toolbar. Generates structured JSON profile —
row count, top values, numeric ranges, quality flags. No raw row data.
Known bug: row count reflects display cap (200 rows), not full result set.

### Component 2 — Local AI Mode via Ollama (M5 core)
Locally-running model handles all AI features on-machine. No OpenAI API call.
True air-gap for healthcare/government customers.
**Build estimate:** 1-2 weeks. **Priority: Milestone 5.**

### Component 3 — In-App Analyst Chat (M5 companion)
Dedicated Chat tab — analyst describes goal, AW generates/runs query locally,
AI validates result and suggests next step. Full collaboration loop inside AW.
Result Passport is the context bridge — AI sees summary, not raw rows.
**Build estimate:** 2-3 weeks. **Priority: Milestone 5, after Local AI Mode.**

### Component 4 — Reference Table Library (NEW)
Pre-built, maintained reference CSVs for common analytical use cases.
Eliminates cold-start knowledge dependency for recurring workflows.
Priority candidates: IRA negotiated drug list, FDA orphan drug status,
biosimilar tracker, USP category mappings.
**Priority: Milestone 5 or ongoing maintenance.**

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
- Compass/Farragut CMS Medicare workflow — SUBSTANTIALLY COMPLETE (see below)

**Reference use case:** CMS Part B/D drug spending analysis — IRA exclusion lists,
therapeutic category classification, GLOBE/GUARD payment model candidates.
Validated March 2026. Both Part B and Part D memos delivered.

---

## COMPASS/FARRAGUT DELIVERABLE STATUS

**Part B / GLOBE memo — COMPLETE**
- 57 confirmed GLOBE candidates, 2 borderline (Farragut review needed)
- 14 sole orphan drugs identified
- Manufacturer and MFN deal flags applied
- Word memo delivered March 18, 2026

**Part D / GUARD memo — COMPLETE (preliminary)**
- 304 preliminary GUARD candidates, $125.9B combined 2023 spending
- Upper-bound estimate — Tot_Mftr proxy used for single-source filter
- Independent estimate (Avalere, 2024 data): ~170 drugs / ~$93B
- 3 vaccines flagged for Farragut scope confirmation
- Wegovy IRA overlap flagged for Farragut confirmation
- IRA Round 3 (Feb 2026) not yet applied — Farragut to check
- Word memo delivered March 18, 2026

**Remaining Farragut items:**
- Orphan drug status for GUARD candidates (not yet applied)
- Manufacturer and MFN flags for GUARD candidates (not yet applied)
- Farragut confirmation on 5 flagged items across both memos

---

## GO-TO-MARKET POSITION

**Primary positioning:**
"Local-first AI analytics for analysts who work with sensitive data.
Import your files, get insights automatically, explore with natural language —
without your data ever leaving your machine."

**Core differentiator:** Privacy architecture — three tiers, each honest.
**Correct language:** "AI generates analysis instructions that run on your computer."

**Target sequence:** Consultants/freelancers → Mid-market finance/ops → Healthcare/government
**University beachhead:** Planned (mirrors prior biomechanics playbook)

---

## LAST SESSION LOG
# Append one line per session. Most recent at top. Format: [DATE] [ENV] — summary.

[2026-03-18] [BD] — Completed Part D / GUARD analysis: 304 preliminary candidates, $125.9B spending, IRA exclusions via Reference Table JOIN, Tot_Mftr single-source proxy. Delivered GUARD Word memo. Identified Result Passport display-cap bug. Updated AW Partner Template to v3.
[2026-03-18] [BD] — Reference Table JOIN mechanics test PASSED: 25 exclusions in one query, 94→59 drugs in single JOIN, Result Summary validated throughout. Cold-start test identified as true validation — needs fresh dataset. Reference Table Library added to M5 roadmap.
[2026-03-18] [CODE] — Built Result Passport + Privacy Layer: Copy Result Summary button, schema-only insights prompt, privacy disclosure in Insights view, per-dataset AI consent on import. v1.4.0.
[2026-03-18] [CODE] — Built Reference Table JOIN end-to-end: import pipeline, SQL rewrite, AI context, frontend UI. Confirmed Bug #2 and #3 already fixed. v1.3.0.
[2026-03-18] [BD] — Specced Milestone 5 privacy architecture: Result Passport (M4 add), Local AI via Ollama, In-App Analyst Chat. Defined three-tier privacy story mapped to customer tiers.
[2026-03-18] [BD] — Completed Part B GLOBE analysis for Compass/Farragut: 57 confirmed candidates, 14 sole orphan, 40 MFN deal manufacturers. Produced Word memo.
[2026-03-18] [CODE] — No code changes. M4 status audit: Insights + Passport done, Reference Table JOIN + Privacy/Consent UI + Bug #2 (NOT LIKE chains) still open.
[2026-03-18] [CODE] — No code changes. Brief session: verified wrap.md workflow and CONTEXT.md bridge.
[2026-03-18] [BD] — Established context bridge system. Healthcare meeting next priority.

---

## OPEN DECISIONS
# Things not yet resolved. Remove when closed.

- [ ] Demo build timeline for healthcare meeting — M4 complete; is cold-start validation
      required before demo, or is mechanics test sufficient?
- [ ] Pricing for healthcare meeting — $3k/seat baseline or higher given compliance angle?
- [ ] Proposal template needed before first Tier 2/3 meeting
- [ ] University outreach — which program, which contact, what's the ask?
- [ ] Cold-start validation — which fresh dataset to use?
- [ ] Local AI mode quality bar — is Ollama output good enough for Tier 3 buyers?
- [ ] In-App Chat — does this replace or complement the claude.ai partnership workflow?
- [ ] Reference Table Library — which files to build first?
- [ ] Result Passport display-cap bug — fix so row count reflects full result, not
      display cap (200 rows). Claude Code task.
- [ ] Farragut confirmations — 5 flagged items across Part B and Part D memos pending
      Farragut input before analysis is finalized.
- [ ] GUARD orphan drug and MFN flags — not yet applied to Part D candidate list.

---

## NEXT ACTIONS

**Business development (Claude.ai):**
- Build prep materials for healthcare operations meeting (lead with three-tier privacy story)
- Apply orphan drug status and MFN flags to GUARD Part D candidate list
- Await Farragut confirmation on flagged items in both memos
- Draft one-pager for Tier 1 consultant outreach
- Draft proposal and contract template skeleton

**Product / code (Claude Code):**
- Fix Result Passport display-cap bug (row count should reflect full result, not 200-row display cap)
- Spec Milestone 5 Local AI Mode (Ollama)
- Spec Milestone 5 In-App Analyst Chat
- Plan cold-start validation test with fresh dataset
- Spec Reference Table Library (Component 4)

---

## HOW TO USE THIS FILE

**At the end of a Claude Code session**, append to Last Session Log:
[DATE] [CODE] — what was built, what changed, what broke, what's next

**At the end of a Claude.ai session**, append to Last Session Log:
[DATE] [BD] — decision made, document produced, or strategy updated

**At the start of any session**, read this file first.
For Claude Code: add to CLAUDE.md → `See CONTEXT.md for current product and business state.`
For Claude.ai: paste the full file or the relevant sections.

**To trigger a context update:** say "update the context file" at end of session.
