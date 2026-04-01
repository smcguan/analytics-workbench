# Analytics Workbench — Bug Log
# Structured record of all bugs found, root causes, and fixes.
# This file is the source of truth — keep it current after every Claude Code session.
# Upload to the JetWare AI Development project so all chats can access it.
# Say "log this bug" in any project chat to add a new entry.
# Say "synthesize bugs" in any project chat for architectural analysis.

---

## COMPONENT TAGS
DEMO_RUNNER | RESULTS_RENDER | REFERENCE_LIBRARY | AI_FEATURES | EXPORT
SESSION_LOG | IMPORT_PIPELINE | QUERY_ENGINE | UI_GENERAL | PACKAGING

## ROOT CAUSE TAGS
DIVERGENT_PATH | MISSING_WIRE | TYPE_MISMATCH | PATH_RESOLUTION | STATE_MANAGEMENT
MISSING_TEST | CONCURRENCY | SCHEMA_ASSUMPTION | EXTERNAL_BEHAVIOR | SPEC_GAP

---

## SYNTHESIS SUMMARY — Last run: 2026-03-31

**Top risk:** CONCURRENCY pattern is more prevalent than initial analysis showed — BUG-001 was a race condition (not divergent path), and BUG-008 (file lock) is also concurrency. Async fire-and-forget calls in demo runner paths are a recurring risk as new AI features are added.

**Scaling concern:** QUERY_ENGINE (3 bugs) and RESULTS_RENDER (2 bugs) are the most
customer-facing components and currently have no end-to-end test coverage. Edge cases
multiply at 200+ customers.

**Architectural weaknesses:**
- Demo runner isolation (HIGH) — does not share results rendering path with main flow
- Missing test coverage at seams (HIGH) — MISSING_WIRE bugs invisible to unit tests
- External dependency assumptions (MEDIUM) — DuckDB quirks found by accident, not by tests

**Hardening priorities:**
1. Fix BUG-001 structurally — merge demo runner into main results rendering path
2. Build Example Case end-to-end test suite before customer launch
3. Resolve BUG-008 (Windows file lock) — reliability risk for all Windows customers
4. Create DuckDB constraints registry with regression tests for each known quirk

---

## BUG RECORDS

### BUG-001
**Status:** FIXED — v1.19.1
**Found:** 2026-03-31 — Self-test (Claude.ai session)
**Component:** DEMO_RUNNER / RESULTS_RENDER
**Root Cause:** CONCURRENCY (corrected from DIVERGENT_PATH)
**Summary:** Result Narrative missing in Run All demo mode — race condition between _fetchResultNarrative() and the next step clearing the narrative area.
**Detail:** _fetchResultNarrative() was fire-and-forget in demo runner paths. tutorialRunAll() paused only 800ms before advancing to next step, which cleared the narrative area (line 7275). OpenAI call takes 2-4 seconds — narrative response arrived after the area was already cleared. Step Through mode worked fine because user controls pace manually.
**Fix:** Await _fetchResultNarrative() in both demo runner paths (query_run at line 5590, ai_ask at line 5851). Normal runSqlQuery() path left fire-and-forget — no automated next step overwrites it.
**Fix Commit:** 7c034d8 — v1.19.1
**Test Added:** Yes

---

### BUG-002
**Status:** FIXED — v1.5.1
**Found:** 2026-03-18 — Self-test (Claude.ai BD session)
**Component:** REFERENCE_LIBRARY
**Root Cause:** TYPE_MISMATCH
**Summary:** Reference table string columns not normalized on import — JOINs against CMS data failed due to case mismatch.
**Detail:** CMS data uses title case for drug names. Reference library CSVs imported as-is. JOINs silently returned zero matches without LOWER() workaround. Fix: title-case string columns on reference table import.
**Fix Commit:** v1.5.1
**Test Added:** Yes — 3 tests (Bug #7 regression + Bug #8 unit + end-to-end JOIN)

---

### BUG-003
**Status:** FIXED — v1.5.0
**Found:** 2026-03-18 — Self-test (Claude.ai BD session)
**Component:** REFERENCE_LIBRARY
**Root Cause:** MISSING_WIRE
**Summary:** Reference library tables showed in UI but were not registered in DuckDB — queries against them failed silently.
**Detail:** Tables appeared in the library browser but the DuckDB registration step was not being called on load. No error surfaced to the user.
**Fix Commit:** v1.5.0
**Test Added:** Yes

---

### BUG-004
**Status:** FIXED — v1.5.0
**Found:** 2026-03-18 — Self-test (Claude.ai BD session)
**Component:** RESULTS_RENDER
**Root Cause:** MISSING_WIRE
**Summary:** Result Passport display-cap — results truncated in display, full data not accessible.
**Fix Commit:** v1.5.0
**Test Added:** Yes

---

### BUG-005
**Status:** FIXED — v1.5.0
**Found:** 2026-03-18 — Self-test
**Component:** QUERY_ENGINE
**Root Cause:** SCHEMA_ASSUMPTION
**Summary:** Rollup row detection failing — aggregate rows not being identified correctly.
**Fix Commit:** v1.5.0
**Test Added:** Yes

---

### BUG-006
**Status:** FIXED — v1.4.x
**Found:** 2026-03-18 — Self-test
**Component:** QUERY_ENGINE
**Root Cause:** EXTERNAL_BEHAVIOR
**Summary:** NOT LIKE chains — >~25 NOT LIKE conditions cause DuckDB query failure.
**Detail:** DuckDB behavioral limit. Workaround: use Reference Table LEFT JOIN + IS NULL pattern instead of long NOT LIKE chains. Documented as architectural constraint.
**Fix Commit:** v1.4.x
**Test Added:** Yes

---

### BUG-007
**Status:** FIXED — v1.4.x
**Found:** 2026-03-18 — Self-test
**Component:** QUERY_ENGINE
**Root Cause:** EXTERNAL_BEHAVIOR
**Summary:** ORDER BY DESC parser error — ORDER BY as final clause caused parse failure due to LIMIT 200 wrap behavior.
**Detail:** AW wraps all queries with LIMIT 200. If ORDER BY is the final clause, the wrap fails. Fix: ensure ORDER BY precedes LIMIT in wrapped query.
**Fix Commit:** v1.4.x
**Test Added:** Yes

---

### BUG-008
**Status:** PARTIALLY ADDRESSED — medium priority
**Found:** 2026-03-18 — Self-test
**Component:** IMPORT_PIPELINE
**Root Cause:** CONCURRENCY
**Summary:** Windows file lock on Refresh Datasets — file handle not released before refresh attempts to re-read.
**Detail:** Reliability risk for all Windows customers. No test added. Full fix still needed.
**Fix Commit:** Partial — v1.4.x
**Test Added:** No

---

### BUG-009
**Status:** FIXED — v1.4.x
**Found:** 2026-03-18 — Self-test
**Component:** PACKAGING
**Root Cause:** PATH_RESOLUTION
**Summary:** Reference library CSVs not found in packaged build — build reads from dist/data/reference_library/ but files were only in source data/.
**Detail:** No code change needed — files synced to dist path. BUILD_RELEASE.bat updated with explicit library sync step.
**Fix Commit:** v1.4.x
**Test Added:** No — build process fix

---

## HOW TO USE THIS FILE

**In any JetWare AI Development project chat:**
- "Log this bug: [description]" — I'll add a formatted entry and generate a Claude Code handoff prompt
- "Mark BUG-XXX fixed in vX.X.X" — I'll update the status
- "Synthesize bugs" — I'll analyze patterns and surface architectural risks
- "Update BUGS.md" — I'll produce a fresh version of this file to re-upload

**After a Claude Code session:**
- Note which bugs were fixed and what version
- Come back to any project chat, say "mark BUG-XXX fixed in vX.X.X"
- Re-download and re-upload the updated BUGS.md to the project

**Template for new bugs:**
### BUG-XXX
**Status:** OPEN
**Found:** YYYY-MM-DD — [Self-test | Customer | Demo run | Code review]
**Component:** [TAG]
**Root Cause:** [TAG]
**Summary:** One sentence.
**Detail:** More detail if needed.
**Fix:** What was done.
**Fix Commit:** vX.X.X or PENDING
**Test Added:** Yes | No | PENDING

---

## CHANGELOG
- 2026-03-31 — BUG-001 fixed v1.19.1. Root cause corrected to CONCURRENCY. 1,079 tests passing.
- 2026-03-31 — File created. 9 bugs back-populated. Synthesis run added.
