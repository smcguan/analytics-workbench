# JetWare AI — Business Development Context File
# Paste this at the start of any Claude.ai BD session.
# Say "JetWare AI BD context loaded" to proceed.
# Updated at the end of every BD session and by Claude Code wrap script.
# For full technical product detail see CONTEXT.md.

---

## COMPANY

**Company name:** JetWare AI (Jetware AI LLC — California LLC, rename from LifeModeler Services LLC COMPLETE 2026-03-31)
**Product:** Analytics Workbench (AW) — local-first AI analytics desktop application
**Stage:** Pre-revenue. Brand launched. First customer engagement in progress.
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

## PRIVACY ARCHITECTURE — THREE TIERS

| Tier | Customer | Privacy Level | What it means |
|------|----------|---------------|---------------|
| 1 | Consultants/freelancers | Schema-only AI | AI sees column names + stats only. Data stays local. |
| 2 | Mid-market finance/ops | Result Passport | Analyst collaboration without raw data leaving. |
| 3 | Healthcare/government/law firms | Local AI mode | Zero external API calls. Full air-gap. Auditable. |

Farragut sits between Tier 1 and Tier 3 — law firm context requires Tier 3 assurance
but may function day-to-day at Tier 1. AI Mode Switch (coming in M5) gives them both.

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
- Status: Positive reception. Requirements email sent, no response yet. Follow-up needed.
- Potential: 5–10 seat team license + onboarding + reference library. Anchor customer.
- Next step: Casey sends follow-up to schedule demo call. Demo = Tutorial #4 live.

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
- Send follow-up email to Jackie and Stan
- Run Tutorial #4 cold dry run with Shawn playing Stan
- Update his LinkedIn to show JetWare AI connection (coordinate with launch)

**Casey's company relationship:** Subcontractor of JetWare AI LLC (agreement not yet drafted)

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

**Identity locked:**
- Logo: Blue gradient JET, thin WARE AI, horizontal swoosh with jet detail
- Tagline: "AI analytics for data that stays put."
- Color palette: Dark navy (#11192c) + blue accent (#3b9fe8)

**Assets complete:**
- Square dark logo (LinkedIn company page, dark backgrounds)
- White background logo (documents, email signature)
- LinkedIn banner (1584x396)
- Landing page (jetwareai.com)
- Email signature
- Infrastructure summary document

**Web presence:**
- jetwareai.com — live on GitHub Pages (migrated from Cloudflare Pages 2026-04-01)
  - Repo: github.com/smcguan/jetwareai-site — local clone at C:\dev\jetwareai-site
  - Contact Us form: Formspree endpoint xlgolljk — email delivery confirmed 2026-04-02
  - OG image: updated 2026-04-02 (1200x627, logo + tagline) — LinkedIn cache clearing
- LinkedIn company page — live (linkedin.com/company/jetware-ai)
- LinkedIn personal profile (linkedin.com/in/shawnmcguan) — fully updated

**LinkedIn status:**
- Company page: live, complete, not yet announced
- Personal profile: updated, notifications OFF — public launch pending Casey coordination
- Launch post: not yet written or published

---

## LAST SESSION LOG (BD-RELEVANT)

[2026-04-02] [BD] — Met with CPA Paul. Tax strategy confirmed: stay LLC, convert to
  C Corp if revenue scales (unlocks R&D tax credit for hiring). Paul checking CA $800
  annual LLC fee compliance. "Big beautiful bill" 3-year retroactive C Corp provision
  — Paul to clarify. Action items: QuickBooks, company credit card, business bank account.
  Contact form tested and confirmed working (Formspree → Gmail). OG image rebuilt at
  1200x627 with real logo — uploaded to repo, LinkedIn cache clearing. Local git repo
  set up at C:\dev\jetwareai-site. CONTEXT_BUSINESS.md updated.

[2026-04-01] [BD] — jetwareai.com migrated from Cloudflare Pages to GitHub Pages
  (github.com/smcguan/jetwareai-site). Formspree contact form added (endpoint xlgolljk)
  — resolves email obfuscation issue that persisted through prior migration. Infrastructure
  summary document created. CONTEXT_BUSINESS.md updated.

[2026-03-31] [BD] — Company launch day. CA LLC rename confirmed complete. CPA engaged.
  Brand identity locked: blue palette, all logo formats. jetwareai.com live on Cloudflare
  Pages (migrated from Worker — email obfuscation issue resolved). LinkedIn company page
  built: banner, logo, tagline, about, specialties. LinkedIn personal profile fully updated:
  headline, about, JetWare AI as current position, featured section, skills, contact info.
  Email signature complete. Notifications OFF — public launch post pending Casey coordination.

[2026-03-29] [BD] — Wrap workflow established. SKILL.md updated. Testing procedure
  Word doc produced. SOW stress test ready for next session. v1.19.0, 1,100 tests.

[2026-03-25] [BD+CODE] — Feature testing. CONTEXT_BUSINESS.md created.
  Column Name Interpreter confirmed working.

[2026-03-24] [BD] — Casey Immersion Guide, motivation email, conversation guide,
  onboarding guide, company formation checklist all built. Wyoming LLC checklist
  complete — filing not started. SOW capability mapping memo built.

---

## OPEN DECISIONS

- [ ] Farragut meeting date — not yet confirmed
- [ ] Farragut pricing — $3k–4.5k/seat, confirm before proposal
- [ ] Casey follow-up email — needs to go out this week
- [ ] Casey immersion program — must complete before meeting
- [ ] LinkedIn public launch post — pending Casey coordination. Notifications currently OFF.
- [ ] LinkedIn OG image — cache clearing, check in ~1 hour
- [ ] Casey subcontractor agreement — needed before contract
- [ ] IP assignment — AW to Jetware AI LLC
- [ ] Proposal template — needed before first contract
- [ ] Start QuickBooks
- [ ] Get company credit card
- [ ] Get business bank account
- [ ] IRS name change notification — Paul (CPA) to handle
- [ ] Paul to clarify "big beautiful bill" 3-year C Corp retroactive provision
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

**Immediate:**
- Check LinkedIn OG image in ~1 hour — should show new logo
- QuickBooks setup
- Business bank account (bring LLC paperwork — Jetware AI LLC rename complete)
- Company credit card (after bank account)

**This week:**
- Casey sends follow-up email to Jackie and Stan
- Casey begins AW Immersion Program
- IRS name change notification — confirm with Paul

**Before Farragut meeting:**
- Casey completes immersion program
- Casey dry run Tutorial #4 — Shawn plays Stan
- Confirm meeting date
- Draft Farragut proposal (hold until post-Stan policy call)
- Draft Casey subcontractor agreement
- USPTO trademark filings

**Analytical:**
- Run SOW stress test in AW
- Verify USP GLOBE/GUARD and orphan drug CSVs
- Apply orphan drug + MFN flags to Part D list

---

## HOW TO USE THIS FILE

Start BD session: paste this file, say "JetWare AI BD context loaded."
End BD session: note key decisions. Claude Code wrap script updates automatically.
Full technical detail: see CONTEXT.md.
