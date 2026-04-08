# JetWare AI — Business Development Context File
# Paste this at the start of any Claude.ai BD session.
# Say "JetWare AI BD context loaded" to proceed.
# Updated at the end of every BD session and by Claude Code wrap script.
# For full technical product detail see CONTEXT_AW.md.

---

## COMPANY

**Company name:** JetWare AI (Jetware AI LLC — California LLC, rename from LifeModeler Services LLC COMPLETE 2026-03-31)
**Product:** Analytics Workbench (AW) — local-first AI analytics desktop application
**Stage:** Pre-revenue. Brand launched. LinkedIn launched 2026-04-07. First customer engagement in progress.
**Developer:** Shawn (product owner, sole developer, 100% owner)
**BD lead:** Casey (Shawn's son, CS degree, embedded at Farragut/McDermott)

---

## PRODUCT SUMMARY

Analytics Workbench is a Windows desktop application for analysts who work with
sensitive data. It imports CSV, Excel, TSV, and Parquet files locally, surfaces
insights automatically, and answers natural language questions via AI-generated
SQL — without sending data to any cloud server.

**Current version:** v1.20.0
**Test suite:** 1,079 automated tests, all passing. AI SQL accuracy: 100% (20/20).

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
- Example Workflows — 8 built-in tutorials across 5 domains
- JetWare AI logo — branding in header and Welcome card

**Key selling points by audience:**
- Consultants: "No NDA violations — data never leaves your machine"
- Law firms: "No BAA required — no client data touches our systems"
- Healthcare/government: "Full air-gap option available — zero external API calls"
- PE diligence firms: "Replayable workflows — same methodology, new target, one click"
- Enterprise IT: "1,079 automated tests — 100% AI SQL accuracy verified"

**Known demo limitation:** Schema normalization JOIN queries cannot be reliably
generated from natural language. Pre-written SQL is required for that step.
Frame this as "your analyst saves this once and reruns it" — honest and
sets up the Parameterized Sessions story naturally.

**API KEY STATUS — COMPLETE:**
Customer API key management is built and shipped. On first launch AW prompts
for the customer's OpenAI API key. Key is encrypted and stored locally at
%APPDATA%\JetWareAI\config.enc. Persists across sessions. Updateable via
Settings panel. Developer key fully removed from codebase. JetWare AI is
no longer in the data path at all.

**AI touchpoint audit — COMPLETE:**
Full prompt-level audit conducted. Confirmed transmission inventory by feature:
- SQL generation / query suggestions: schema, stats, up to 5 sample rows,
  up to 10 categorical values per column, natural language question
- Result Narrative / Explain: question, column names, row count, up to 5-10
  result rows (query output, not raw source data)
- Insight cards: schema and aggregate stats only — no sample rows (cleanest feature)
- Column aliases, analysis sequences, workflow suggestions: column names only

**Privacy Mode toggle — IN DEVELOPMENT:**
Settings toggle that restricts all AI transmissions to schema and aggregate
statistics only when enabled. Strips sample rows, categorical values, and
result rows from all AI prompts. Persists in encrypted local settings alongside
API key. Claude Code implementation prompt drafted and passed to dev project.
This feature must be built and verified before evaluation package ships.

---

## PRIVACY ARCHITECTURE — THREE TIERS

| Tier | Customer | Privacy Level | What it means |
|------|----------|---------------|---------------|
| 1 | Consultants/freelancers | Schema-only AI | AI sees column names + stats only. Data stays local. |
| 2 | Mid-market finance/ops | Result Passport | Analyst collaboration without raw data leaving. |
| 3 | Healthcare/government/law firms | Local AI mode | Zero external API calls. Full air-gap. Auditable. |

Farragut sits between Tier 1 and Tier 3 — law firm context requires Tier 3 assurance
but may function day-to-day at Tier 1. AI Mode Switch (coming in M5) gives them both.

**Privacy statement for customer conversations:**
"Your data files never leave your machine. When AI features are used, the system
sends limited structural information — column names, statistics, and in some features
a small number of sample values — to generate SQL. No complete records or bulk data
are ever transmitted. For highly sensitive environments, Privacy Mode restricts all
AI transmissions to column names and statistics only — no sample values of any kind."

**Privacy Mode framing (for Tier 3 / law firm conversations):**
Standard mode: schema, stats, up to 5 sample rows for improved SQL accuracy.
Privacy Mode: schema and stats only. Zero sample values or result rows transmitted.
Recommended for attorney-client privilege and air-gap environments.

**No BAA required** — this is a procurement unlock that must be front and center
with healthcare/law firm buyers.

---

## PRICING STRUCTURE

- Tier 1 (consultants/freelancers): $300–800 one-time + 15–20% annual maintenance
- Tier 2 (mid-market finance/ops): $1,000–2,500 per seat + maintenance
- Tier 3 (healthcare/government): $2,000–5,000 per seat + consulting engagement
- Consulting/onboarding: $150–300/hr or fixed project fee

**Farragut/McDermott proposal (not yet drafted — hold until NDA signed):**
- 5–10 seat team license at $3,000–4,500/seat
- $500–800/seat annual maintenance
- $5,000–10,000 onboarding engagement
- Ongoing reference table library maintenance (recurring revenue)

Customer supplies their own OpenAI API key — no data intermediary.

---

## GO-TO-MARKET POSITION

**Primary positioning:**
"Analytics Workbench is the only AI analytics tool that imports your files, surfaces
insights automatically, lets you enrich your data without exporting it — and never
sends a single row to the cloud."

**Tagline:** "AI analytics for data that stays put."

**Core differentiator:** Privacy architecture — three tiers, each honest and verifiable.
**Quality differentiator:** 1,079 automated tests including AI SQL accuracy verification.

**Key buyer vocabulary:**
- "Shadow AI" — the problem AW eliminates
- "Local-first" / "private AI" — architecture descriptors buyers now use
- "Air-gapped analysis" — Tier 3 / law firm language
- "No BAA required" — procurement unlock for healthcare law firm context
- "Schema-only AI mode" — precise technical claim, not marketing
- "Your own API key" — customer controls their OpenAI relationship directly

**Target sequence:** Consultants/freelancers → Mid-market finance/ops → Healthcare/government
**Anchor customer:** Farragut/McDermott+Consulting — law firm analytics arm doing PE diligence.
**University beachhead:** Planned.

---

## ACTIVE PIPELINE

**Farragut/McDermott+Consulting (PRIMARY)**
- What they are: McDermott+Consulting, analytics arm of McDermott Will & Emery law firm
- What they do: Healthcare policy consulting, PE diligence on healthcare acquisitions
- Key contacts: Jackie Williams (primary relationship, business-side), Stan (technical gatekeeper)
- Casey's role: Subcontractor delivering SOW work. Full-time hire budgeted for May 2026.
  Jackie has discussed AI department career trajectory for Casey at McDermott.

**Current Farragut status (as of 2026-04-07):**
- Casey had lunch with Shawn. Full-time hire at Farragut confirmed for May onboarding.
- Wednesday call = Casey only, five use cases discussion. Shawn not invited.
- Casey does NOT want to mention JetWare or flag Shawn's follow-up on the call.
  His read of the room takes priority — respect it completely.
- Shawn will debrief with Casey after Wednesday call before deciding Thursday approach.
- Thursday outreach to Jackie will be calibrated based on Wednesday debrief signal:
  which use cases had most urgency, how Casey's standing looks, how fast Farragut
  is moving. Email draft ready but framing may adjust based on debrief.
- Jackie has accepted Shawn's LinkedIn connection request.
- Do NOT reference Wednesday's call in Thursday email if Casey gave no heads-up.
  Email must stand alone as founder reaching out to establish formal relationship.

**Five Farragut use cases identified (from Stan email):**
- CIM extraction — Document Workbench territory
- CMS rule/note repository + searchable query — Document Workbench territory
- Searchable PE ownership database with org chart output — AW + new output capability
- Cash pay entities analysis (MedSpas etc.) — AW today
- IDRE data analysis (CMS inpatient dataset) — AW today

**Farragut commercial separation (CRITICAL):**
Casey's consulting/employment track and the AW software license track are two
completely separate commercial relationships. Never conflate. Casey is the
embedded analyst. Shawn owns the proposal and contract. Do not let the
full-time employment conversation block or bundle the software sale.

**Casey's Farragut agreement (reviewed 2026-04-07):**
Key risks identified: Section 8b non-compete (during agreement — JetWare is a
tool vendor not a consulting competitor, distinction matters); Section 8d
client non-solicitation 24 months post-termination (Casey cannot be involved
in selling to Farragut's PE firm clients). Section 3 work product — AW is clean,
not created under SOW, not paid for by Farragut. Get CA employment attorney
review before Casey starts full-time.

**NDA Thursday email — READY BUT FRAMING TBD:**
Two versions depending on Wednesday debrief outcome:

Version A (if Casey gave Jackie heads-up — unlikely given his current position):
Subject: Analytics Workbench — Next Steps
"Jackie, Casey mentioned you'd had a good conversation Wednesday, and I wanted
to follow up directly. I understand the right next step before any further
product discussions is to establish a formal NDA relationship between JetWare AI
and Farragut. That makes complete sense, and I'd like to move it forward on
your timeline. Who is the right person on your end to engage — legal, or
someone on the business side? Best, Shawn McGuan, Founder, JetWare AI"

Version B (standalone — no Casey reference):
Subject: Analytics Workbench — Establishing a Formal Relationship
"Jackie, I wanted to reach out directly as we think about next steps between
JetWare AI and Farragut. I understand the right foundation before any product
discussions is a formal NDA relationship. That makes complete sense given your
environment, and I'd like to move it forward on your timeline. Who is the right
person on your end to engage — legal, or someone on the business side?
Best, Shawn McGuan, Founder, JetWare AI"

NOTE: Do NOT attach JetWare's NDA. Let Farragut send their form.
SEND CONDITION: After Wednesday debrief with Casey confirms timing is right.

**Healthcare operations team (SECONDARY)**
- Tier 3 prospect. Meeting prep materials complete. Date not confirmed.

---

## CASEY — ROLE AND STATUS

Casey is Shawn's son. CS degree. Has connections at Farragut from prior work.
Full-time hire at Farragut budgeted for May 2026 with AI department trajectory.

**Dark period protocol (now through NDA signing):**
- Do not initiate contact with Casey about Farragut work during this window
- One debrief call after Wednesday is appropriate and expected
- Casey needs to show up to Farragut as their analyst, not Shawn's reporter
- If Casey calls with a technical AW question, answer it — don't initiate

**Casey still needs to:**
- Complete 5-module AW Immersion Program
- Give Jackie one-sentence heads-up on Wednesday that Shawn will reach out
- Be briefed on API key build — customers must supply their own OpenAI API key
- Casey subcontractor agreement — needed before he starts full-time at Farragut

**Materials sent to Casey:**
- Casey Immersion Guide (5 modules)
- Casey Motivation Email
- Casey Conversation Guide
- AW Onboarding Guide
- Farragut Demo Script

---

## CLAUDE CODE CONTEXT INJECTION

**Instructions:** Add the following section to CLAUDE.md in both the Analytics Workbench
repo and the Document Workbench repo. Commit it. Claude Code reads CLAUDE.md
automatically at session start — no manual pasting required.

Do not include pricing, confidential terms, or anything that would be sensitive
if the repo became public. The text below is clean for that purpose.

---

**Text to add at the bottom of CLAUDE.md in both repos:**

```
## Business Context
See CONTEXT_AW.md for current product and business state.

## Farragut Engagement — Key Commercial Constraints

1. Farragut (McDermott+Consulting) = primary prospect, healthcare PE diligence,
   law firm environment, attorney-client privilege applies to all client data.
2. Casey McGuan = Farragut subcontractor transitioning to full-time employee May 2026.
   Casey = internal resource. JetWare = external vendor. Roles must stay clean.
3. NDA is the only commercial unlock. Nothing ships to Farragut until NDA signed.
4. Five use cases: CIM extraction + CMS repository = Document Workbench (built, demo-ready pending real CMS PDFs).
   Cash pay + IDRE = AW today. PE ownership database = AW partial.
5. IP boundary: JetWare owns software and platform improvements. Farragut owns
   workflows, data, and internal methodology. Platform evolves broadly.
   Workflows stay customer-specific.
6. Never imply co-development with Casey. Never frame JetWare as consultant.
   Primary = software license. Secondary = onboarding. Avoid SOW-based ownership risk.
7. Moat is workflow + execution + customer proximity — not technology.
8. Speed framing: "shorten the loop between analysis and implementation" /
   "velocity with control" — NOT "we move fast."
9. Pedigree: 40 years regulated software, LifeMOD exit to Smith & Nephew 2012,
   1,079 automated tests. JetWare timing is deliberate playbook, not fly-by-night.
10. End state: Farragut = anchor customer. Casey = embedded signal loop.
    JetWare = scalable platform vendor.
```

**Update this section when:** NDA signed, proposal in progress, Casey fully onboarded,
or any material change to the Farragut engagement structure.

**Current Situation:**
- Farragut moving into formal procurement mode
- Casey transitioning to full-time internal role (May)
- JetWare currently external with no formal agreement — NDA required
- Casey = internal resource. JetWare = external vendor. Must stay clean.

**1. Separation of Roles — CRITICAL**
- Casey operates independently inside Farragut
- JetWare engages as standalone software vendor
- No implied co-development or subcontractor relationship

**2. Product vs Consulting — NON-NEGOTIABLE**
- Primary: Software license (Analytics Workbench)
- Secondary: Workflow enablement (onboarding)
- Avoid: custom development framing, consulting dependency, SOW-based ownership risk

**3. IP Boundary — CRITICAL FOR SCALABILITY**
- JetWare owns: software, platform improvements
- Farragut owns: data, workflows, internal methodologies
- Platform evolves across customers. Workflows remain customer-specific.

**4. Workflow Moat vs Technology Moat**
- No longer relying on deep technical moat (LifeModeler model)
- New moat: workflow + execution + proximity to use case
- Positioning: shared platform, proprietary application

**5. Speed Framing — REFRAMED**
Do NOT say: "we move fast"
Instead use:
- "We shorten the loop between analysis and implementation"
- "Velocity with control"
- "Reduced time-to-insight without increasing risk"

**Key Messaging Frameworks:**

A. Platform vs Workflow
- Platform improves across customers
- Customer advantage = how tool is used internally

B. Competitor Question Handling
If asked "Are you building this to sell to competitors?":
- Yes — platform evolves
- No — workflows and data never shared
- Framing: "Capability transfers, methodology does not"

C. Pedigree Positioning
Use background to reinforce:
- Experience in regulated environments (FDA software)
- Discipline around IP and data boundaries
- Credibility in high-risk systems

D. Speed + Control Narrative
- Closed-loop development: Casey → embedded user signal → Shawn → product
- Outcome: faster iteration without compromising governance

**Immediate Tactical Plan:**
1. No JetWare mention in Wednesday call — clean separation
2. Thursday: direct email to Jackie initiating NDA as formal counterparty
3. NDA = critical gate — unlocks product discussions, demos, workflow conversations
4. Post-NDA: re-enter as formal vendor → demo → evaluation → pricing

**Business Model:**
1. Software license (per seat)
2. Onboarding / workflow enablement
3. Optional recurring reference library

**Key Risks to Avoid:**
- Being pulled into Casey's SOW
- Implying co-development
- Allowing IP ambiguity
- Over-emphasizing speed without control
- Acting like a consultant instead of a product company

**End State Goal:**
- Farragut = anchor customer
- Casey = embedded signal loop
- JetWare = scalable platform vendor

---

## COMPETITIVE MOAT AND IP STRATEGY

**The LifeMOD comparison — what carries over and what doesn't:**

LifeMOD had a technical moat — simulation algorithms that were hard to replicate.
AW's moat is different. Local-first architecture is a design choice, not a patent.
Vibe coding and commoditized AI APIs mean the technical barrier is lower. The real
moat is customer proximity, regulated-industry credibility, and execution speed.

**The core tension with anchor customers:**

Farragut will drive the roadmap — CIM extraction, CMS repository, org chart output.
Those become features. JetWare then sells those features to Marwood Group, BRG,
Avalere, and other Farragut competitors. Farragut's investment in shaping the product
benefits their direct competitors. This is a known enterprise software tension and
must be addressed in the agreement structure.

**The clean contractual distinction:**

Software ships broadly — cannot agree to withhold features from the general market.
Workflow templates and reference libraries built to a specific customer's methodology
are a consulting deliverable — can be kept proprietary to that customer for a defined
period (12 months recommended).

The pitch to Farragut: "The software ships to everyone. The workflows and reference
libraries we build together for your specific use cases are yours. We can formalize
that in the agreement."

**The adapted LifeMOD pitch for Farragut:**

Not about technology moat — about workflow moat. Farragut's analysts are shaping the
tool. Their workflows are being encoded into replayable templates. They will be running
analytical sequences in May that competitors won't figure out until Q4. They're not
buying a feature — they're buying a six-month head start on how to use it at
Farragut's level of sophistication. The tool eventually commoditizes. The institutional
knowledge of how to use it doesn't.

**Farragut pitch language — methodology moat (use post-NDA, not in NDA email):**

"Analytics Workbench is designed as a shared platform that continues to improve
across customers, but the real advantage comes from how each organization applies
it. The core software evolves broadly, while the workflows, analytical approaches,
and reference frameworks developed within your team remain specific to Farragut.
That means you're not just adopting a tool — you're encoding your own methodology
into repeatable workflows that your analysts can execute consistently. In practice,
that creates a lead: your team is shaping how the platform is used in your domain
and operationalizing those capabilities ahead of the market, rather than simply
consuming a standardized tool. The analytical workflows and reference libraries
your team builds remain yours."

Use this in: first substantive call with Jackie post-NDA, proposal opening section.
Do NOT use in: NDA email, any communication before NDA is signed.

**Objection: "JetWare AI started at the same time as Casey's engagement — is this a real company?"**

The concern behind this question is not really about the founding date. It's about
whether JetWare AI will be around in two years to support a multi-seat deployment
at a major law firm. Address that concern directly.

Do NOT volunteer this defensively. If it comes up, answer it directly and move on.

Prepared response:
"I've been building software for regulated industries for forty years. The local-first
AI problem wasn't something I stumbled into — it's the same problem I've been watching
since the cloud analytics wave started. Casey's work at Farragut gave me the most
demanding real-world test case I could ask for. That's exactly how I built LifeMOD —
starting with the customers who had the hardest problems. The timing isn't a
coincidence. It's the playbook."

Supporting proof points if pressed:
- 40-year track record building regulated software
- LifeMOD sold to Smith & Nephew 2012 — not a first venture
- 1,079 automated tests — product built to enterprise standards from day one
- Privacy architecture document prepared for McDermott general counsel review
- Local-first architecture is a deliberate design choice, not a limitation

**Status:** v0.10.2 — essentially demo-ready. Full stack operational.
**Tests:** 180 passing.
**One-pager:** Complete (produced 2026-04-07). Navy/amber design, four sections.

**What is built and verified:**
- Full PDF ingestion pipeline — PyMuPDF, paragraph-aware chunking, LanceDB vector store
- Cloud mode (OpenAI gpt-4o-mini + text-embedding-3-small) — tested with real PDFs
- Local mode (Ollama phi-3-mini + nomic-embed-text) — tested end-to-end
- PyInstaller .exe packaging — verified, 404MB, runs standalone
- CMS regulatory library v1 — 5 document categories indexed (synthetic docs currently)
- Frontend UI — matches AW sidebar structure exactly, dark theme, shared branding
- Shared config.enc with AW — same API key, same settings, same APPDATA path
- All 9 FastAPI endpoints wired and responding

**Decisions confirmed:**
- Vector store: LanceDB
- Local model: phi-3-mini (answers), nomic-embed-text (embeddings)
- App: Option A — separate .exe (DocumentWorkbench.exe)
- Shared config.enc with AW at APPDATA\JetWareAI\

**Two remaining actions before demo-ready:**
1. Replace synthetic CMS library docs with real CMS PDFs — content work, not build work
2. Rebuild .exe with v0.10.2 code — last build was v0.8.0

**What this means for Farragut:**
Four of five use cases now demonstrable:
- CIM extraction — drop in PDF, ask questions, get cited answers. Working now.
- CMS regulatory repository — library built, indexed, queryable. Swap real docs and ready.
- Cash pay entities analysis — AW today.
- IDRE data analysis — AW today.
- PE ownership database — AW partial, new output capability needed.

**Suite framing:**
- Analytics Workbench — structured data, available now
- Document Workbench — unstructured documents, available now (pending real CMS PDFs)
- Research Workbench — external sourcing, roadmap

---

## COMPASS/FARRAGUT DELIVERABLE STATUS

**Delivered:**
- Part B / GLOBE memo — 57 confirmed candidates, 14 sole orphan drugs. March 18, 2026.
- Part D / GUARD memo — 304 preliminary candidates, $125.9B spending. March 18, 2026.

**Pending:**
- Farragut confirmation on 5 flagged items
- Orphan drug + MFN flags not yet applied to Part D list

---

## REFERENCE TABLE LIBRARY

**Shipped:** IRA negotiated drugs (35 drugs, Rounds 1-3)
**In draft (need verification):** USP GLOBE/GUARD categories, orphan drug status
**Not yet built:** Biosimilar tracker, MFN deal status, State Medicaid schema map

Maintained reference library = recurring maintenance contract revenue.

---

## LEGAL DOCUMENTS STATUS

**JetWare AI SLA (Analytics Workbench):** Drafted. Clean on data/privacy, no BAA,
liability cap, IP retention. One gap: Data and Privacy section language must precisely
match the AI touchpoint audit results once complete.

**Casey/Farragut Master Agreement:** Reviewed. Key risks documented above. Get CA
employment attorney review before Casey starts full-time.

**Casey/JetWare subcontractor agreement:** Not yet drafted. Needed before contract.

---

## COMPANY FORMATION AND ADMIN

**Entity:** Jetware AI LLC (California)
**Ownership:** 100% Shawn (co-member Sheila Penke — Operating Agreement update needed)
**Status:** CA LLC rename from LifeModeler Services LLC — COMPLETE 2026-03-31

**Completed:**
- CA LLC rename — DONE
- CPA engaged: Paul (met 2026-04-02) — DONE
- EIN — same, no change needed

**Tax strategy (confirmed with Paul 2026-04-02):**
Stay LLC while small. Convert to C Corp if revenue scales — unlocks R&D tax credit
when hiring. "Big beautiful bill" may allow 3-year retroactive C Corp tax advantage
— Paul to clarify. Paul checking CA $800 annual LLC fee compliance.

**Still pending:**
- Start QuickBooks
- Get company credit card
- Get business bank account
- IRS name change notification — CPA (Paul) to handle
- USPTO trademark filings — Class 9 and Class 42 (~$500 total)
- Casey subcontractor agreement — needed before contract
- IP assignment — AW to Jetware AI LLC
- Proposal template — needed before first contract
- Operating Agreement update — signatures from Shawn and Sheila Penke

---

## BRAND AND WEB PRESENCE

**Identity:**
- Logo: Navy/amber — new swoosh version approved. Third build source location
  still overwriting asset in app — unresolved.
- Tagline: "AI analytics for data that stays put."
- Color palette: Navy (#0C1527) + electric green (#00C878) [memory note: 
  context file shows dark navy #11192c + blue #3b9fe8 — confirm which is current]

**Assets complete:**
- Square dark logo (LinkedIn company page, dark backgrounds)
- White background logo (documents, email signature)
- LinkedIn banner (1584x396)
- Landing page (jetwareai.com)
- Email signature (all 4 icons standardized to 24x24, April 2026)
- Document Workbench one-pager (produced 2026-04-07)
- AW Evaluation Setup Guide v1.20 — FINAL (ready for evaluation package)
- AW Privacy Architecture document — FINAL (ready for McDermott general counsel)
  NOTE: Both documents gate on Privacy Mode toggle being built and verified
  before evaluation package ships to Farragut.

**Web presence:**
- jetwareai.com — live on GitHub Pages
  - Repo: github.com/smcguan/jetwareai-site — local clone at C:\dev\JetWareAI-site
  - Contact Us form: Formspree endpoint xlgolljk — confirmed working
  - OG image: 1200x627, logo + tagline
  - Page load time: 4ms (confirmed via Cloudflare)
  - Cloudflare Web Analytics: beacon already installed and running
- LinkedIn company page — live (linkedin.com/company/jetware-ai)
- LinkedIn personal profile (linkedin.com/in/shawnmcguan) — fully updated

**LinkedIn status (as of 2026-04-07):**
- Personal profile: notifications ON
- Launch post: PUBLISHED 2026-04-07
- ~50 personal outreach emails sent to 600-person contact list pre-launch
- Jackie Williams (Farragut) has accepted Shawn's LinkedIn connection
- Content strategy: see LinkedIn Content Strategy section below

---

## LINKEDIN CONTENT STRATEGY

**Core principle:** Every post reinforces credibility, problem awareness, or product proof.
Shawn is a technical founder with 40-year track record — not a content creator.
Post accordingly.

**Three content categories:**

1. PROBLEM POSTS (most frequent — post these most)
   No product mention. Articulate the pain buyers feel. These get shared.
   Jackie reads one and forwards it to colleagues.
   Examples:
   - Why analysts in healthcare and law are the last to get AI tools
   - The NDA problem: when client data can't go near the cloud
   - What firms are actually losing by leaving their analysts out of AI

2. CREDIBILITY AND PERSPECTIVE POSTS (unfair advantage — use it)
   Nobody else has FDA-regulated software background + successful exit + AI second act.
   Examples:
   - Lessons from the LifeMOD exit applied to AI products
   - What building software for surgeons taught me about building for analysts
   - Why local-first architecture is having a moment

3. PRODUCT PROOF POSTS (use sparingly — earn the right first)
   One real customer story or metric is worth ten feature announcements.
   Hold until there is something real to show.

**Cadence:** One post per week. Tuesday or Wednesday morning Pacific.
Never Monday or Friday.

**Format:** Short paragraphs. No walls of text. No bullet-point listicles.
First two lines must earn the read. Write the way Shawn talks — direct and credible.

**Hashtag strategy:**
Use 3-5 hashtags per post, placed at the very bottom, CamelCase format.
Never edit a published post to add hashtags — resets algorithmic momentum.
Mix one broad tag with two or three specific tags per post.

Standard hashtag set — pick 3-4 per post based on topic:
- #LocalFirst — architecture descriptor, own this one, use on every post
- #DataPrivacy — broad, high follow volume, directly relevant
- #HealthcareAnalytics — niche, exactly the right buyer audience
- #PrivateEquity — reaches PE diligence audience
- #AIAnalytics — broad AI tag with professional context
- #RegulatedIndustries — small but highly targeted, exactly the right buyer

Drop #Analytics alone — too broad, too crowded. Use #HealthcareAnalytics instead.

Example combinations by post type:
- NDA / privacy problem posts: #DataPrivacy #LocalFirst #HealthcareAnalytics
- LifeMOD / credibility posts: #AIAnalytics #LocalFirst #RegulatedIndustries
- PE diligence posts: #PrivateEquity #HealthcareAnalytics #DataPrivacy
- Product proof posts: #AIAnalytics #LocalFirst #HealthcareAnalytics

**Next five posts (in order):**
1. Launch post — PUBLISHED 2026-04-07
2. The NDA problem post — write this week
3. LifeMOD to JetWare AI lessons — second act story
4. What PE diligence analysts actually do with sensitive data — buyer problem awareness
5. Why "no BAA required" matters more than people realize — healthcare/law firm audience

**LinkedIn rules during Farragut dark period:**
- Do NOT message Jackie on LinkedIn — email is the right channel for NDA conversation
- DO post knowing Jackie is reading — content reinforces credibility ambient
- Posts about local-first architecture and law firm data privacy are working
  for you in the background while the formal relationship catches up

---

## LAST SESSION LOG (BD-RELEVANT)

[2026-04-08] [BD] — Farragut breakthrough. Wednesday call outcome: AW fully on
  the table. Jackie and Stan excited. Stan taking to committee to determine how
  to bring in. Casey to focus work on AW internally. Sandbox evaluation and
  analyst demos planned. Casey surprised by level of enthusiasm. Four of five
  use cases demonstrable immediately. Do NOT send email to Jackie or Stan until
  Stan gets back to Casey — Casey's direction, respect it completely.
  Document Workbench context file loaded — v0.10.2, 180 tests, full stack
  operational, cloud and local modes verified, .exe packaged. Four of five
  Farragut use cases now demonstrable. Two remaining actions: replace synthetic
  CMS docs with real PDFs, rebuild .exe. CONTEXT_BUSINESS.md updated.

[2026-04-07] [BD] — Session 3. Lunch with Casey. Full-time Farragut hire confirmed
  for May. Casey does not want to mention JetWare on Wednesday call — his read of
  the room takes priority. Thursday Jackie outreach approach to be calibrated after
  Wednesday debrief. Two NDA email versions drafted (with/without Casey reference).
  Strategic discussion on competitive moat: LifeMOD technical moat does not carry
  over directly — AW moat is customer proximity and execution speed. Clean IP
  distinction identified: software ships broadly, workflow templates and reference
  libraries built to customer methodology stay proprietary for defined period.
  Adapted LifeMOD pitch documented: workflow moat, not technology moat.
  CONTEXT_BUSINESS.md updated with moat/IP strategy section.

[2026-04-07] [BD] — Session 2. Customer API key build confirmed complete —
  encrypted local storage, first-launch prompt, Settings panel, developer key
  removed. AI touchpoint audit completed — full transmission inventory documented
  by feature. Privacy Mode toggle scoped and Claude Code prompt drafted. Two
  evaluation package documents finalized: AW Evaluation Setup Guide v1.20 and
  AW Privacy Architecture Final — both ready pending Privacy Mode toggle build
  and verification. Privacy statement updated to reflect accurate audit findings.
  Evaluation package ready to ship same day NDA lands, pending Privacy Mode toggle.

[2026-04-07] [BD] — LinkedIn launched. ~50 personal outreach emails sent.
  Launch post published. Notifications ON. Jackie Williams accepted LinkedIn
  connection. Casey pre-Wednesday call with Jackie: full-time hire confirmed
  for May, AI department trajectory discussed. Bad news: NDA required before
  AW exposure, Shawn disinvited from Wednesday call. Wednesday = Casey SOW
  cases only. NDA Thursday email drafted and ready to send post-Wednesday debrief.
  Casey Farragut agreement reviewed — risks documented. JetWare SLA reviewed —
  clean with one gap (AI touchpoint audit must complete). Document Workbench
  one-pager produced. API key issue confirmed — still on Shawn's personal key,
  Track 1 priority, Claude Code prompt drafted. Shawn's current OpenAI key to
  be revoked today. Cloudflare Web Analytics confirmed running (beacon already
  installed). LinkedIn content strategy defined — three categories, weekly cadence,
  five posts queued. AI touchpoint audit started but incomplete — four features
  still unconfirmed (insight cards, result narrative, sanity check, explain).

[2026-04-06] [BD] — AW context loaded. v1.20.0 confirmed. Tutorial #4 step-through
  attempted — schema normalization natural language confirmed unreliable. Logo swap
  in progress. Email signature fixed. Website repo cloned to C:\dev\JetWareAI-site.

[2026-04-02] [BD] — Met with CPA Paul. Tax strategy confirmed. QuickBooks, bank
  account, credit card action items. Contact form confirmed working. OG image rebuilt.
  Local git repo set up.

[2026-04-01] [BD] — jetwareai.com migrated to GitHub Pages. Formspree contact form added.

[2026-03-31] [BD] — Company launch day. CA LLC rename confirmed. Brand identity locked.
  jetwareai.com live. LinkedIn pages complete.

[2026-03-29] [BD] — Wrap workflow established. SOW stress test ready. v1.19.0.

[2026-03-25] [BD+CODE] — Feature testing. CONTEXT_BUSINESS.md created.

[2026-03-24] [BD] — Casey materials built.

---

## OPEN DECISIONS

**Commercial:**
- [ ] NDA with Farragut — Thursday email to Jackie initiates this
- [ ] Farragut proposal — hold until NDA signed
- [ ] Farragut pricing — $3k–4.5k/seat, confirm before proposal
- [ ] Casey subcontractor agreement — needed before he starts full-time at Farragut
- [ ] Casey/Farragut agreement — CA employment attorney review needed
- [ ] IP assignment — AW to Jetware AI LLC

**Product:**
- [x] Customer API key build — COMPLETE
- [x] Revoke personal OpenAI key — COMPLETE
- [x] AI touchpoint audit — COMPLETE
- [ ] Privacy Mode toggle — IN DEVELOPMENT (Claude Code prompt drafted)
  Gate: must be built and prompt-level verified before evaluation package ships
- [ ] IT intake document — finalize once Privacy Mode toggle is verified
- [x] Document Workbench foundation — COMPLETE v0.10.2, 180 tests, demo-ready
- [ ] Document Workbench — replace synthetic CMS docs with real PDFs
- [ ] Document Workbench — rebuild .exe with v0.10.2 code
- [ ] Logo swap — resolve third build source overwriting asset
- [ ] Demo dataset — clean representative healthcare PE dataset for Farragut demo

**Admin:**
- [ ] Start QuickBooks
- [ ] Get company credit card
- [ ] Get business bank account
- [ ] IRS name change notification — Paul (CPA) to handle
- [ ] Paul to clarify "big beautiful bill" 3-year C Corp retroactive provision
- [ ] USPTO trademark filings — Class 9 and Class 42 (~$500 total)
- [ ] Operating Agreement update — Shawn + Sheila Penke signatures needed
- [ ] Proposal template — needed before first contract

**Analytical:**
- [ ] Farragut 5 flagged items — awaiting confirmation
- [ ] Part D orphan drug + MFN flags — not yet applied
- [ ] USP GLOBE/GUARD CSVs — need verification
- [ ] MFN deal status CSV — not yet built
- [ ] Biosimilar tracker CSV — not yet built
- [ ] SOW stress test — ready to run, not yet completed

**LinkedIn:**
- [ ] NDA problem post — write this week
- [ ] LifeMOD to JetWare AI lessons post
- [ ] PE diligence analyst problem awareness post
- [ ] No BAA required post

---

## NEXT ACTIONS

**Wednesday:**
- Casey call with Jackie — five use cases only, no JetWare mention
- Casey debrief call with Shawn after — listen for use case urgency, timing signals,
  and Casey's read on the room before deciding Thursday approach

**Thursday (after Wednesday debrief):**
- Send NDA email to Jackie — Version A or B depending on debrief outcome
- Hold if debrief signals timing is wrong

**Dark period build priorities:**
1. Privacy Mode toggle (gating item for AW evaluation package)
2. Document Workbench — replace synthetic CMS docs with real PDFs
3. Document Workbench — rebuild .exe with v0.10.2 code
4. Admin: QuickBooks, bank account, credit card

**Weekly:**
- One LinkedIn post — Tuesday or Wednesday morning Pacific
- Next post: NDA problem post

---

## HOW TO USE THIS FILE

Start BD session: paste this file, say "JetWare AI BD context loaded."
Start LinkedIn/content session: paste this file, say "JetWare AI content session loaded."
End BD session: note key decisions. Claude Code wrap script updates automatically.
Full technical detail: see CONTEXT_AW.md.
