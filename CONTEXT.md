# Analytics Workbench — Shared Context File
# This file is the bridge between Claude Code (coding) and Claude.ai (business development).
# Update it at the end of every session in either environment. One or two lines is enough.
# Claude Code: this file is referenced in CLAUDE.md and should be read at session start.
# Claude.ai: paste this file at the start of any session where product state matters.

---

## PRODUCT STATE

**Current milestone:** Milestone 3 complete. Milestone 4 in active development.

**M4 features and status:**
- Insights View — COMPLETE (auto-generates 3-5 AI insight cards on dataset load, cached)
- Export Passport — COMPLETE (9-section JSON profile, validated on 227M row file)
- Reference Table JOIN — not started (lookup CSV joined to primary dataset in-app)
- Privacy and Transparency Layer — not started (schema-only AI mode, per-dataset consent)
- Result Passport — not started (query result summary for external AI collaboration) [NEW]
- Bug #2 (NOT LIKE chains ~26 conditions) — FIXED (literal-stripping in readonly validator)
- Bug #3 (ORDER BY DESC parser error when AW wraps query) — FIXED (could not reproduce)

**Validated at scale:** 220M rows, DuckDB local execution, sub-second import.

**Test suite:** 333 automated tests, all passing, runs under 1 second.

---

## MILESTONE 5 — PRIVACY ARCHITECTURE (PLANNED)

**Goal:** Close the gap between the privacy promise and the real-world analytical
collaboration workflow. Deliver a three-tier privacy story that maps to the three
customer tiers.

### The gap this milestone addresses
AW already prevents raw data from reaching the AI during SQL generation and insights.
The remaining gap: when an analyst collaborates with an external AI on query *results*,
there is currently no mechanism to do so without exporting raw rows. The Compass/Farragut
session (March 2026) required five Excel exports containing real data to claude.ai.
Milestone 5 closes this loop.

### Component 1 — Result Passport (small, add to M4)
When a query runs, a "Copy Result Summary" button appears alongside Export Excel/TSV.
Generates a structured profile of the result set: row count, column list, top values
with counts, numeric ranges, data quality flags. Analyst shares this instead of raw rows.
Covers the vast majority of external AI collaboration needs without exposing row-level data.
**Build estimate:** 1 day. **Priority: Add to Milestone 4.**

### Component 2 — Local AI Mode via Ollama (M5 core)
A locally-running model (Llama 3, Mistral, or similar via Ollama) handles SQL generation,
suggestions, and insights entirely on the machine. No OpenAI API call. True air-gap
operation for healthcare and government customers. UI shows permanent status indicator:
"Local AI — all processing on this machine."
- Customer supplies Ollama install (free, open source)
- AW detects Ollama at startup and offers Local AI mode toggle
- All existing AI features (SQL gen, suggestions, insights) route through local model
- Quality tradeoff vs. GPT-4.1-mini is real but acceptable for compliance-driven buyers
**Build estimate:** 1-2 weeks. **Priority: Milestone 5.**

### Component 3 — In-App Analyst Chat (M5 companion)
A dedicated Chat tab where the analyst describes what they want to accomplish. AW
generates and runs the query locally, shows the result, and the AI (local or cloud)
validates it and suggests next steps — all without leaving the app. The human-AI
collaboration loop that today requires claude.ai + Excel exports runs entirely inside AW.
- Works with both cloud AI (OpenAI) and local AI (Ollama)
- Result Passport is the context bridge — AI sees the summary, not raw rows
- Multi-turn: analyst asks follow-up questions, AW refines and re-runs
- Session log exportable as a record of the analytical process
**Build estimate:** 2-3 weeks. **Priority: Milestone 5, after Local AI Mode.**

---

## THREE-TIER PRIVACY STORY (Go-to-Market)

Maps directly to the three customer tiers:

| Tier | Customer | Privacy Level | What it means |
|------|----------|---------------|---------------|
| 1 | Consultants / freelancers | Schema-only AI | AI sees column names + stats only. Data stays local. |
| 2 | Mid-market finance / ops | Result Passport | Analyst collaboration loop works without raw data leaving. Satisfies corporate data governance. |
| 3 | Healthcare / government | Local AI mode | Zero external API calls. Full air-gap. Auditable. Compliant with strictest data handling requirements. |

This is an honest tiered story — each level actually delivers what it promises.
Use in healthcare meeting prep and Tier 2/3 sales materials.

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
- Compass/Farragut CMS Medicare workflow — validated use case, drives M4/M5 roadmap

**Reference use case:** CMS Part B/D drug spending analysis — IRA exclusion lists, therapeutic
category classification, GLOBE/GUARD payment model candidates. Validated March 2026.

---

## GO-TO-MARKET POSITION

**Primary positioning:**
"Local-first AI analytics for analysts who work with sensitive data.
Import your files, get insights automatically, explore with natural language —
without your data ever leaving your machine."

**Core differentiator:** Privacy architecture — three tiers, each honest.
- Tier 1: AI generates analysis instructions from schema only. Data stays local.
- Tier 2: Result Passport enables AI collaboration without raw data exposure.
- Tier 3: Full local AI mode. Zero external calls. Air-gap compliant.

**Correct language:**
"AI generates analysis instructions that run on your computer."
NOT: "AI analyzes your data."

**Target sequence:** Consultants/freelancers → Mid-market finance/ops → Healthcare/government
**University beachhead:** Planned (mirrors prior biomechanics playbook)

---

## LAST SESSION LOG
# Append one line per session. Most recent at top. Format: [DATE] [ENV] — summary.

[2026-03-18] [BD] — Specced Milestone 5 privacy architecture: Result Passport (M4 add), Local AI via Ollama, In-App Analyst Chat. Defined three-tier privacy story mapped to customer tiers.
[2026-03-18] [BD] — Completed Part B GLOBE analysis for Compass/Farragut: 57 confirmed candidates, 14 sole orphan, 40 MFN deal manufacturers. Produced Word memo. Identified Result Passport as next product feature.
[2026-03-18] [CODE] — No code changes. M4 status audit: Insights + Passport done, Reference Table JOIN + Privacy/Consent UI + Bug #2 (NOT LIKE chains) still open.
[2026-03-18] [CODE] — No code changes. Brief session: verified wrap.md workflow and CONTEXT.md bridge.
[2026-03-18] [BD] — Established context bridge system. Healthcare meeting next priority.

---

## OPEN DECISIONS
# Things not yet resolved. Remove when closed.

- [ ] Demo build timeline for healthcare meeting — when is M4 stable enough to show?
- [ ] Pricing for healthcare meeting — $3k/seat baseline or higher given compliance angle?
- [ ] Proposal template needed before first Tier 2/3 meeting
- [ ] University outreach — which program, which contact, what's the ask?
- [ ] Result Passport scope — confirm top-values-with-counts is sufficient; what fields are needed vs. risky?
- [ ] Local AI mode quality bar — is Ollama output good enough for Tier 3 buyers or does it undersell?
- [ ] In-App Chat — does this replace or complement the claude.ai partnership workflow?

---

## NEXT ACTIONS

**Business development (Claude.ai):**
- Complete Part D / GUARD analysis for Compass/Farragut
- Build prep materials for healthcare operations meeting (lead with three-tier privacy story)
- Draft one-pager for Tier 1 consultant outreach
- Draft proposal and contract template skeleton

**Product / code (Claude Code):**
- Build M4 Reference Table JOIN
- Build M4 Privacy and Transparency Layer
- Build M4 Result Passport (small add — 1 day)
- ~~Fix Bug #2 (NOT LIKE/NOT IN chains)~~ DONE — literal-stripping fix already in place
- ~~Fix Bug #3 (ORDER BY DESC parser error)~~ DONE — could not reproduce, wrapping logic correct
- Validate M4 with Compass/Farragut CMS workflow before healthcare demo
- Spec Milestone 5 Local AI Mode (Ollama) in CLAUDE.md when M4 is complete
- Spec Milestone 5 In-App Analyst Chat when Local AI Mode is underway

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
