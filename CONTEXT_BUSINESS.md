# JetWare AI — Business Development Context File
# Paste this at the start of any Claude.ai BD session.
# Say "JetWare AI BD context loaded" to proceed.
# Updated at the end of every BD session and by Claude Code wrap script.
# For full technical product detail see CONTEXT.md.

---

## COMPANY

**Company name:** JetWare AI (Jetware AI LLC — California LLC, rename from LifeModeler Services LLC COMPLETE 2026-03-31)
**Product:** Analytics Workbench (AW) — local-first AI analytics desktop application
**Stage:** Pre-revenue. Brand launched. First customer engagement active.
**Developer:** Shawn (product owner, sole developer, 100% owner)
**BD lead:** Casey (Shawn's son, CS degree, embedded at Farragut/McDermott)

---

## PRODUCT SUMMARY

Analytics Workbench is a Windows desktop application for analysts who work with
sensitive data. It imports CSV, Excel, TSV, and Parquet files locally, surfaces
insights automatically, and answers natural language questions via AI-generated
SQL — without sending data to any cloud server.

**Current version:** v1.19.0 — demo-ready for Farragut/McDermott meeting
**Test suite:** 1,100 automated tests, all passing. AI SQL accuracy: 100% (20/20).

**Core capabilities (all shipped):**
- Import up to 220M rows — loads in seconds
- Automatic insight cards on import — concentration, outliers, trends
- Natural language to SQL — AI generates, analyst reviews and runs
- Reference Table JOIN — enrich data against lookup CSVs without exporting
- Session Log — full audit trail of every query and result
- Workflows — record, save, and replay analytical sessions
- Parameterized Workflows — swap files and rerun same methodology on new data
- Save as Dataset — materialize JOIN results without Excel export
- Column Name Interpreter — human-readable aliases for cryptic column names
- Result Narrative — two-sentence plain English summary after every query
- Sanity Check — automatic warnings on suspicious results
- Smarter Suggestions — three-step analytical sequence on any dataset
- Example Workflows — 6 built-in tutorials across 5 domains
- JetWare AI logo — branding in header and Welcome card

**Key selling points by audience:**
- Consultants: "No NDA violations — data never leaves your machine"
- Law firms: "No BAA required — no client data touches our systems"
- Healthcare/government: "Full air-gap option available — zero external API calls"
- PE diligence firms: "Replayable workflows — same methodology, new target, one click"
- Enterprise IT: "1,100 automated tests — 100% AI SQL accuracy verified"

---

## PRODUCT FAMILY — JETWARE AI WORKBENCH SUITE

JetWare AI is a suite of local-first AI workbench tools. All products share the same
privacy architecture. Product naming convention: "[X] Workbench" under JetWare AI brand.

| Product | Status | Description |
|---------|--------|-------------|
| Analytics Workbench | Available | Structured data analytics — CSV, SQL, charts, insights |
| Document Workbench | In development | Local RAG — index prior deliverables, query in plain English |
| Research Workbench | Roadmap | External data sourcing — fee schedules, CMS, regulatory filings |

**Document Workbench — spec drafted 2026-04-01:**
- Indexes PDFs, Word, Excel locally using ChromaDB or LanceDB (embedded, no server)
- Analyst queries firm's prior work product in plain English
- AI returns answer grounded in source passages with citations and confidence scores
- Privacy architecture identical to AW — documents never leave the machine
- Embeddings via OpenAI text-embedding-3-small (customer's own API key) or Ollama (local)
- Build trigger: confirmed paying customer relationship with Farragut/McDermott
- Estimated build: 6–8 weeks from start given AW infrastructure already exists
- Pricing target: $2,000–3,500/seat; bundle with AW at $4,500–7,000/seat

**Why Document Workbench is the right second product:**
- Same buyer (Stan, Jackie), same privacy justification, same procurement argument
- Directly addresses Stan's stated use case: "crawl prior projects and deliverables"
- Local-first RAG for regulated consulting firms does not exist as a polished product
- Every engagement adds to the index — compounding value for repeat diligence work

---

## PRIVACY ARCHITECTURE — THREE TIERS

| Tier | Customer | Privacy Level | What it means |
|------|----------|---------------|---------------|
| 1 | Consultants/freelancers | Schema-only AI | AI sees column names + stats only. Data stays local. |
| 2 | Mid-market finance/ops | Result Passport | Analyst collaboration without raw data leaving. |
| 3 | Healthcare/government/law firms | Local AI mode | Zero external API calls. Full air-gap. Auditable. |

Farragut sits between Tier 1 and Tier 3 — law firm context requires Tier 3 assurance
but may function day-to-day at Tier 1. AI Mode Switch (coming in M5) gives them both.

**Key privacy language (use precisely):**
- Correct: "AI generates analysis instructions from column names and statistics only"
- Correct: "AI generates instructions that run on your computer"
- Never say: "AI analyzes your data"
- Procurement unlock: "No BAA required" — eliminates a major procurement barrier for law firms

**On certifications:**
SOC 2, ISO 27001, and similar certifications are designed for cloud vendors handling
customer data on their systems. AW has no vendor systems — data never leaves the
customer's machine. These certifications are largely irrelevant to AW's architecture.
The higher-value move is a plain-English technical architecture document for legal review
— already built (see BD Materials below). CMMC may become relevant for future
government/federal contractor buyers.

---

## PRICING STRUCTURE

- Tier 1 (consultants/freelancers): $300–800 one-time + 15–20% annual maintenance
- Tier 2 (mid-market finance/ops): $1,000–2,500 per seat + maintenance
- Tier 3 (healthcare/government): $2,000–5,000 per seat + consulting engagement
- Consulting/onboarding: $150–300/hr or fixed project fee

**Farragut/McDermott proposal (not yet drafted):**
- 5–10 seat team license at $3,000–4,500/seat
- $500–800/seat annual maintenance
- $5,000–10,000 onboarding engagement
- Ongoing reference table library maintenance (recurring revenue)
- Do NOT use $499 Tier 1 price in any Farragut materials

Customer supplies their own OpenAI API key — no data intermediary.

---

## GO-TO-MARKET POSITION

**Primary positioning:**
"Analytics Workbench is the only AI analytics tool that imports your files, surfaces
insights automatically, lets you enrich your data without exporting it — and never
sends a single row to the cloud."

**Tagline:** "AI analytics for data that stays put."

**Core differentiator:** Privacy architecture — three tiers, each honest and verifiable.
**Quality differentiator:** 1,100 automated tests including AI SQL accuracy verification.

**Key buyer vocabulary:**
- "Shadow AI" — the problem AW eliminates
- "Local-first" / "private AI" — architecture descriptors buyers now use
- "Air-gapped analysis" — Tier 3 / law firm language
- "No BAA required" — procurement unlock for healthcare law firm context
- "Schema-only AI mode" — precise technical claim, not marketing

**Target sequence:** Consultants/freelancers → Mid-market finance/ops → Healthcare/government
**Anchor customer:** Farragut/McDermott+Consulting — law firm analytics arm doing PE diligence.
**University beachhead:** Planned.

---

## ACTIVE PIPELINE

**Farragut/McDermott+Consulting (PRIMARY)**
- What they are: McDermott+Consulting, analytics arm of McDermott Will & Emery law firm
- What they do: Healthcare policy consulting, PE diligence on healthcare acquisitions
- Key contacts: Jackie (primary relationship, business-side), Stan (technical gatekeeper)
- Casey's role: Delivered Compass work (GLOBE/GUARD memos), introduced AW, sent
  requirements email. Both memos delivered March 2026.
- Status: Stan responded to Casey's requirements email (2026-04-01). McDermott AI
  policy call scheduled for Monday. Stan to send update next week.
- Potential: 5–10 seat team license + onboarding + reference library. Anchor customer.
  Expanded potential with Document Workbench — same buyer, broader scope.
- Next step: Wait for Stan's update after Monday policy call. Do not send materials
  before Stan's update arrives — Stan set the pace explicitly.

**Stan's email — key points (received 2026-04-01):**
Three use cases identified beyond Medicaid data analysis:
  1. Internal query/database — crawl prior projects and deliverables (→ Document Workbench)
  2. External data finder — source fee schedules, primary materials (→ Research Workbench)
  3. Copy editing — AI first draft on notes (→ not an AW use case, separate tool)
Stan confirmed privacy and accuracy are paramount. Interested in RAG and LLM-as-a-judge.
McDermott AI policy call Monday — Stan to send update next week.

**What this means strategically:**
Stan described a broader AI platform vision. AW fits the structured data analytics layer.
Document Workbench fits the institutional knowledge layer. Present as complementary tools,
not competing. Do not stretch AW to cover use cases it doesn't handle — Stan is technical
and will see through it.

**Healthcare operations team (SECONDARY)**
- Tier 3 prospect. Meeting prep materials complete. Date not confirmed.

---

## CASEY — ROLE AND STATUS

Casey is Shawn's son. CS degree. Has connections at Farragut from prior work.
His role: embedded at Farragut as analyst or consultant, feeds requirements back
to Shawn, becomes indispensable to their workflow.

**Materials sent to Casey:**
- Casey Immersion Guide (5 modules — orientation through Farragut readiness)
- Casey Motivation Email (explains full business vision and his role)
- Casey Conversation Guide (step-by-step for the Stan meeting)
- AW Onboarding Guide (install, demo, answer questions)
- Farragut Demo Script (talking points, Stan Q&A, pre-meeting checklist)

**Casey still needs to:**
- Complete 5-module AW Immersion Program
- Wait for Stan's update before next outreach — do not push before Monday's call
- Run Tutorial #4 cold dry run with Shawn playing Stan
- Update his LinkedIn to show JetWare AI connection (coordinate with launch)

**Casey's company relationship:** Subcontractor of JetWare AI LLC (agreement not yet drafted)

**Introducing the software license through Casey:**
Casey already mentioned AW was built with his dad — foundation is laid. Correct framing:
Casey is a co-creator, not a reseller. Two commercial tracks must stay separate: Casey's
consulting engagement and the AW software license. Do not bundle them.
Timing: introduce the license after Stan clears AI policy, after second engagement is
scoped, after Casey has demonstrated independent value. Casey's line: "As we get deeper
into this workflow, it's probably worth talking about a proper software license for the
team. The product is commercialized through my dad's company — JetWare AI. I can make
that introduction when you're ready." Casey introduces — Shawn owns the proposal and
contract. Casey never negotiates the software deal.

---

## COMPASS/FARRAGUT DELIVERABLE STATUS

**Delivered:**
- Part B / GLOBE memo — 57 confirmed candidates, 14 sole orphan drugs. March 18, 2026.
- Part D / GUARD memo — 304 preliminary candidates, $125.9B spending. March 18, 2026.

**Pending:**
- Farragut confirmation on 5 flagged items
- Orphan drug + MFN flags not yet applied to Part D list

**SOW coverage:** AW covers Steps 2-3 fully. Steps 4-6 need M5 reference library additions.
SOW stress test ready to run at v1.19.0 — all quality gates passed.

---

## REFERENCE TABLE LIBRARY

**Shipped:** IRA negotiated drugs (35 drugs, Rounds 1-3)
**In draft (need verification):** USP GLOBE/GUARD categories, orphan drug status
**Not yet built:** Biosimilar tracker, MFN deal status, State Medicaid schema map

Maintained reference library = recurring maintenance contract revenue.

---

## BD MATERIALS PRODUCED

**Customer-facing:**
- Tier 1 consultant one-pager
- 8-slide deck, one-pager, talking points + demo script (healthcare operations)
- Data Privacy and Security Architecture document — written for general counsel;
  covers installation, data flow, AI mechanics, OpenAI API key ownership,
  HIPAA/BAA inapplicability. Ready to send to Stan after Monday's policy call
  if data security questions arise. Do not send proactively before Stan's update.

**Internal/BD:**
- Casey Immersion Guide, motivation email, conversation guide, onboarding guide
- Farragut demo script
- SOW capability mapping memo
- Company formation checklist

**Brand/web:**
- JetWare AI family landing page (Analytics, Document, Research Workbench)
- Document Workbench UI mockup — same design system, amber accent color
- LinkedIn company page copy, Shawn personal LinkedIn rewrite
- LinkedIn banner, email signature

---

## COMPANY FORMATION AND ADMIN

**Entity:** Jetware AI LLC (California)
**Ownership:** 100% Shawn (co-member Sheila Penke — Operating Agreement update needed)
**Status:** CA LLC rename from LifeModeler Services LLC — COMPLETE 2026-03-31

**Completed:**
- CA LLC rename — DONE
- CPA engaged (existing relationship) — DONE
- EIN — same, no change needed

**Still pending:**
- IRS name change notification — CPA to handle
- Bank account rename to Jetware AI LLC
- USPTO trademark filings — Class 9 and Class 42 (~$500 total)
- Casey subcontractor agreement — needed before contract
- IP assignment — AW to Jetware AI LLC
- Proposal template — needed before first contract
- Operating Agreement update — signatures from Shawn and Sheila Penke

---

## BRAND AND WEB PRESENCE

**Identity locked:**
- Logo: Blue gradient JET, thin WARE AI, horizontal swoosh with jet detail
- Tagline: "AI analytics for data that stays put."
- Color palette: Dark navy (#11192c) + blue accent (#3b9fe8)
- Product family accent colors: AW = green (#00E87A), Document Workbench = amber (#F59E0B),
  Research Workbench = blue (#3B82F6)

**Web presence:**
- jetwareai.com — live on GitHub Pages (migrated from Cloudflare Pages 2026-04-01)
  - Repo: github.com/smcguan/jetwareai-site
  - Contact Us form: Formspree endpoint xlgolljk — email delivery confirmed
- LinkedIn company page — live (linkedin.com/company/jetware-ai)
- LinkedIn personal profile (linkedin.com/in/shawnmcguan) — fully updated

**LinkedIn status:**
- Company page: live, complete, not yet announced
- Personal profile: updated, notifications OFF — public launch pending Casey coordination
- Launch post: not yet written or published

---

## LAST SESSION LOG (BD-RELEVANT)

[2026-04-01] [BD] — Stan email received and analyzed. Three additional use cases identified
  beyond Medicaid data (document RAG, external sourcing, copy editing). Product family
  defined: Analytics Workbench, Document Workbench, Research Workbench. Document Workbench
  product spec drafted. Family landing page and Document Workbench UI mockup produced (HTML).
  Privacy architecture document produced for McDermott GC review (DOCX). Decision: hold all
  outbound materials until Stan's Monday policy call update lands — Stan set the pace.
  Casey/Shawn software license introduction timing discussed — post policy clearance.
  Local model threat (Ollama/YouTube demo) assessed — M5 P4 Ollama integration is correct
  response, not a threat. Certifications discussed — SOC 2 not relevant for local-first
  architecture; plain-English legal document is the right move (built). Agentic BD second
  project created with prompt file for ongoing AI/BD research.

[2026-04-01] [BD] — jetwareai.com migrated from Cloudflare Pages to GitHub Pages
  (github.com/smcguan/jetwareai-site). Formspree contact form added (endpoint xlgolljk)
  — resolves email obfuscation issue that persisted through prior migration. Infrastructure
  summary document created. CONTEXT_BUSINESS.md updated.

[2026-03-31] [BD] — Company launch day. CA LLC rename confirmed complete. CPA engaged.
  Brand identity locked: blue palette, all logo formats. jetwareai.com live on Cloudflare
  Pages. LinkedIn company page built. LinkedIn personal profile fully updated.
  Email signature complete. Notifications OFF — public launch post pending Casey coordination.

[2026-03-29] [BD] — Wrap workflow established. SKILL.md updated. Testing procedure
  Word doc produced. SOW stress test ready for next session. v1.19.0, 1,100 tests.

[2026-03-25] [BD+CODE] — Feature testing. CONTEXT_BUSINESS.md created.
  Column Name Interpreter confirmed working.

[2026-03-24] [BD] — Casey Immersion Guide, motivation email, conversation guide,
  onboarding guide, company formation checklist all built. SOW capability mapping memo built.

---

## OPEN DECISIONS

- [ ] Farragut meeting date — pending Stan's Monday policy call update
- [ ] Farragut pricing — $3k–4.5k/seat, confirm before proposal
- [ ] Farragut proposal — hold until after Stan's update; highest BD priority once cleared
- [ ] Document Workbench build start — trigger: confirmed paying Farragut relationship
- [ ] Casey follow-up — hold until Stan's update lands next week
- [ ] Casey immersion program — must complete before demo meeting
- [ ] Casey/Shawn software license introduction — after Stan clears AI policy
- [ ] LinkedIn public launch post — pending Casey coordination. Notifications currently OFF.
- [ ] Casey subcontractor agreement — needed before contract
- [ ] IP assignment — AW to Jetware AI LLC
- [ ] Proposal template — needed before first contract
- [ ] IRS name change notification — CPA to handle
- [ ] Bank account rename to Jetware AI LLC
- [ ] USPTO trademark filings — Class 9 and Class 42 (~$500 total)
- [ ] Operating Agreement update — Shawn + Sheila Penke signatures needed
- [ ] Farragut 5 flagged items — awaiting confirmation
- [ ] Part D orphan drug + MFN flags — not yet applied
- [ ] USP GLOBE/GUARD CSVs — need verification
- [ ] MFN deal status CSV — not yet built
- [ ] Biosimilar tracker CSV — not yet built
- [ ] University outreach — not yet defined
- [ ] Analysis Summary Artifact format — configurable vs fixed?
- [ ] Reference Library maintenance model — who updates, how distributed?

---

## NEXT ACTIONS

**This week:**
- Wait for Stan's update (expected after Monday McDermott AI policy call)
- Casey: do not send anything to Stan or Jackie before update arrives
- IRS name change notification — confirm with CPA
- Bank account rename to Jetware AI LLC
- Shawn: finish M5 P1 multi-dataset UNION/normalization — prerequisite for demo
- Shawn: run Tutorial #4 end-to-end, time it, find rough spots

**When Stan's update arrives:**
- If policy cleared → send privacy architecture document, schedule demo
- If policy questions raised → respond with targeted materials matched to questions
- Draft Farragut proposal immediately (do not wait further)

**Before Farragut demo meeting:**
- Casey completes immersion program
- Casey dry run Tutorial #4 — Shawn plays Stan
- Draft Casey subcontractor agreement
- USPTO trademark filings

**Analytical:**
- Run SOW stress test in AW
- Verify USP GLOBE/GUARD and orphan drug CSVs
- Apply orphan drug + MFN flags to Part D list

---

## HOW TO USE THIS FILE

Start BD session: paste this file, say "JetWare AI BD context loaded."
End BD session: note key decisions. Claude Code wrap script updates CONTEXT.md automatically.
This file updated manually at end of BD sessions. Full technical detail: see CONTEXT.md.
