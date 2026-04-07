# JetWare AI — Business Development Context File
# Paste this at the start of any Claude.ai BD session.
# Say "JetWare AI BD context loaded" to proceed.
# Updated: 2026-04-07
# For full technical product detail see CONTEXT.md.

---

## COMPANY

**Company name:** JetWare AI (Jetware AI LLC — California LLC, rename COMPLETE 2026-03-31)
**Product:** Analytics Workbench (AW) — local-first AI analytics desktop application
**Stage:** Pre-revenue. Brand launched. Evaluation package complete. First customer engagement in progress.
**Developer:** Shawn (product owner, sole developer, 100% owner)
**BD lead:** Casey (Shawn's son, CS degree, embedded at Farragut/McDermott)

---

## PRODUCT SUMMARY

**Current version:** v1.21.0 — Privacy Mode complete and verified, SOW stress test passed, evaluation package ready for Farragut sandbox install.
**Test suite:** 1,098 automated tests passing. AI SQL accuracy: 100% (20/20).

Core capabilities: import up to 220M rows, automatic insight cards, natural language to SQL, Reference Table JOIN, Session Log, Workflows, Parameterized Workflows, Customer API Key Management, Privacy Mode toggle, 8 Example Workflows including Multi-State Medicaid Diligence.

**SOW stress test COMPLETE 2026-04-07:** Tutorial #4 ran end-to-end. All 10 queries passed baselines. Workflow replay identical to original run. Ready for Farragut sandbox.

**Known demo limitation:** Schema normalization JOIN requires pre-written SQL. Frame as "save once, rerun" — sets up Parameterized Sessions story.

**Customer onboarding requirement:** Customer must supply own OpenAI API key on first launch. Casey must brief Farragut before install.

---

## PRIVACY ARCHITECTURE

| Tier | Customer | Privacy Level |
|------|----------|---------------|
| 1 | Consultants/freelancers | Schema-only AI — data stays local |
| 2 | Mid-market finance/ops | Result Passport collaboration |
| 3 | Healthcare/government/law firms | Local AI mode — full air-gap |

**Privacy Mode (v1.21.0 COMPLETE):** Toggle in Settings. When ON, all AI restricted to schema and aggregate statistics only. Verified via prompt-level audit. When OFF (default), SQL generation sends up to 5 sample rows + 10 categorical values; Result Narrative/Explain send up to 5-10 result rows.

**Ollama local AI mode:** ON ROADMAP. Not built. Documented accurately in all customer-facing materials as "available upon request."

---

## FIVE FARRAGUT USE CASES

| Use Case | Product | Status | Build Needed |
|---|---|---|---|
| CIM extraction (Andrew) | Document Workbench | Not built | 6-8 weeks |
| CMS rule repository | Document Workbench + library | Not built | 6-8 weeks + curation |
| PE ownership database | AW query + new viz | Partial | Org chart: 3-4 weeks |
| Cash pay entity analysis | Analytics Workbench | Available now | None |
| IDRE data analysis | Analytics Workbench | Available now | Confirm format |

Cash pay and IDRE are the strongest cards — available today, no caveats.
CIM and CMS rules require Document Workbench — spec complete, 6-8 week build.
PE ownership — query works, org chart output is the gap.

---

## DOCUMENT WORKBENCH SPEC — COMPLETE

Spec uploaded to both projects (DOCUMENT_WORKBENCH_SPEC.md). Key decisions: separate .exe (Option A), PyMuPDF ingestion, ChromaDB vector store, abstract provider interface supporting both OpenAI and Ollama from day one, CMS regulatory library v1 content plan, recurring maintenance revenue model.

Open before build: Ollama model benchmark, ChromaDB vs LanceDB eval, CMS library scope confirmation with Farragut post-NDA.

---

## EVALUATION PACKAGE — COMPLETE

All documents accurate and consistent as of 2026-04-07:
- AW_Evaluation_Setup_Guide.docx — COMPLETE
- AW_Privacy_Architecture_Final.docx — COMPLETE
- In-app Reference Guide — COMPLETE
- Tutorial #4 Multi-State Medicaid Diligence — SOW STRESS TEST PASSED

---

## PRICING

- Tier 1: $300-800 one-time + 15-20% maintenance
- Tier 2: $1,000-2,500/seat + maintenance
- Tier 3: $2,000-5,000/seat + consulting

**Farragut proposal (not yet drafted — hold until post-NDA):**
5-10 seat AW license at $3,000-4,500/seat + $500-800/seat maintenance + $5,000-10,000 onboarding + Document Workbench license (TBD) + CMS library subscription.

---

## ACTIVE PIPELINE

**Farragut/McDermott+Consulting (PRIMARY)**
McDermott+Consulting is the analytics arm of McDermott Will & Emery law firm. They do PE diligence on healthcare acquisitions. Contacts: Jackie (primary), Andrew (CIM use case), Stan (technical gatekeeper).
Casey scoping meeting imminent. Goal: NDA, Casey inside feeding requirements to Shawn, build toward full-time employment for Casey.

**Healthcare operations team (SECONDARY):** Tier 3 prospect. Materials ready.

---

## CASEY

Goal: full-time Farragut employee. Shawn supports this fully.
JetWare AI angle: Casey inside as employee = best reference position for AW.

**CRITICAL before Casey signs employment agreement:**
- Shawn/Casey conversation on JetWare AI referral/finder arrangement — must happen before signing
- Review employment agreement for IP assignment and tools/software clauses
- Brief Casey on OpenAI API key onboarding requirement

---

## COMPANY FORMATION

Entity: Jetware AI LLC (CA) — rename COMPLETE. CPA: Paul (engaged 2026-04-02).
Tax strategy: stay LLC, convert to C Corp if revenue scales.

Pending: QuickBooks, business bank account, company credit card, IRS name change (Paul), USPTO Class 9 + 42, Casey referral agreement, IP assignment, proposal template, Operating Agreement update.

---

## BRAND AND WEB

Logo: correct in-app and on web. Email signature updated 2026-04-07.
jetwareai.com live on GitHub Pages. Repo: github.com/smcguan/jetwareai-site.
Local clone on both desktop and laptop at C:\dev\JetWareAI-site.
LinkedIn live. Notifications OFF — launch post pending Casey coordination.

---

## LAST SESSION LOG

[2026-04-07] [BD+CODE] — SOW stress test COMPLETE. Privacy Mode verified. All evaluation documents updated and consistent. Five Farragut use cases mapped to product. Document Workbench spec written with Ollama designed in. Casey employment/referral arrangement discussion identified as critical pre-meeting item. Email signature fixed. Website repo cloned to laptop.

[2026-04-06] [BD] — v1.20.0. API key management complete. Tutorial #4 step-through. Schema normalization natural language confirmed unreliable.

[2026-04-02] [BD] — Met CPA Paul. Tax strategy confirmed. Contact form working. Git repo set up.

[2026-03-31] [BD] — Company launch. CA LLC rename complete. Brand locked. jetwareai.com live. LinkedIn up.

---

## OPEN DECISIONS

Immediate — before Casey signs:
- [ ] Shawn/Casey referral/finder arrangement conversation
- [ ] Casey reviews employment agreement with Shawn
- [ ] Casey briefed on OpenAI API key requirement

Business formation:
- [ ] QuickBooks, bank account, credit card
- [ ] IRS name change (Paul)
- [ ] USPTO Class 9 + 42
- [ ] IP assignment, proposal template, Operating Agreement

Product:
- [ ] Document Workbench build start (post-Farragut NDA)
- [ ] Farragut 5 flagged items confirmation
- [ ] Part D orphan drug + MFN flags
- [ ] LinkedIn launch post (Casey coordination)
- [ ] Farragut proposal (post-NDA)

---

## HOW TO USE THIS FILE

Start BD session: paste this file, say "JetWare AI BD context loaded."
End BD session: produce updated version. Upload to BD project only.
Full technical detail: see CONTEXT.md (upload to both projects).
