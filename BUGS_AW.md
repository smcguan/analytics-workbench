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

### BUG-010
**Status:** FIXED — v1.20.1
**Found:** 2026-04-06 — Self-test (multi-machine workflow)
**Component:** PACKAGING
**Root Cause:** MISSING_TEST
**Summary:** config.enc (encrypted API key file) not in .gitignore — committed to repo and pulled to other machines, bypassing first-launch setup overlay.
**Detail:** When developer pushed repo from desktop to GitHub, %APPDATA%\JetWareAI\config.enc was not excluded by .gitignore. File was pulled to laptop. AW found the file on first launch and skipped the setup overlay entirely, even though the key was encrypted for a different machine. Customer-facing risk: any customer who clones the repo or receives a build with config.enc bundled will silently inherit a broken key state.
**Fix:** Added `config.enc` to .gitignore. Verified file was not already tracked in git (no git rm --cached needed).
**Fix Commit:** v1.20.1
**Test Added:** Yes — test_key_manager.py validates wrong-machine config is detected and deleted

---

### BUG-011
**Status:** FIXED — v1.20.1
**Found:** 2026-04-06 — Self-test (multi-machine workflow)
**Component:** AI_FEATURES
**Root Cause:** MISSING_WIRE
**Summary:** API key decryption failure on wrong machine is silent — AI features fail without error or setup prompt.
**Detail:** Encryption key is derived from machine-specific values (COMPUTERNAME + USERNAME). When config.enc from machine A is present on machine B, decryption fails. Instead of surfacing an error or triggering the first-launch setup overlay, the failure is swallowed silently. AI features (insights, natural language queries) simply do not work — no error message, no prompt to add a key. User has no way to diagnose the problem.
**Fix:** Both has_key() and get_key() now catch all decryption exceptions, delete the bad config.enc file, log a warning, and return False / raise RuntimeError respectively. This allows the first-launch setup overlay to appear naturally on the next check.
**Fix Commit:** v1.20.1
**Test Added:** Yes — 5 tests in test_key_manager.py::TestCorruptedConfig (corrupted file, wrong machine, get_key cleanup, fresh key after cleanup)

---

### BUG-012
**Status:** FIXED — v1.24.0
**Found:** 2026-04-08 — Self-test (local mode testing)
**Component:** AI_FEATURES
**Root Cause:** DIVERGENT_PATH
**Summary:** suggest_questions endpoint ignores AI mode setting and calls OpenAI directly when local mode is active.
**Detail:** app.log shows `suggest_questions calling OpenAI` while AI mode was set to local. generate_sql correctly routes to Ollama and returns "model not found" errors — confirming local mode is active and working for that endpoint. suggest_questions has a separate code path that bypasses the get_ai_mode() check and calls OpenAI directly regardless of mode. This is a privacy and compliance issue — the feature silently breaks the air-gap guarantee when local mode is selected.
**Fix:** Refactored generate_sql_response() as the single dispatch chokepoint — all 8 AI functions call it, and it checks get_ai_mode() to route to _call_openai() or provider_ollama.generate_response(). No endpoint-level routing needed since every function ultimately calls generate_sql_response(). All endpoints now correctly route through the active provider.
**Fix Commit:** ad42592 — v1.24.0
**Test Added:** Yes — test_ai_mode.py::TestProviderRouting (cloud_calls_openai, local_calls_ollama)

---

## HOW TO USE THIS FILE

**In any JetWare AI Development project chat:**
- "Log this bug: [description]" — I'll add a formatted entry and generate a Claude Code handoff prompt
- "Mark BUG-XXX fixed in vX.X.X" — I'll update the status
- "Synthesize bugs" — I'll analyze patterns and surface architectural risks
- "Update BUGS_AW.md" — I'll produce a fresh version of this file to re-upload

**After a Claude Code session:**
- Note which bugs were fixed and what version
- Come back to any project chat, say "mark BUG-XXX fixed in vX.X.X"
- Re-download and re-upload the updated BUGS_AW.md to the project

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
- 2026-04-08 — BUG-012 fixed v1.24.0. generate_sql_response() refactored as single dispatch chokepoint — all endpoints route correctly in both modes.
- 2026-04-08 — BUG-012 logged. suggest_questions bypasses ai_mode check, calls OpenAI in local mode.
- 2026-04-06 — BUG-010 and BUG-011 fixed v1.20.1. config.enc added to .gitignore; corrupted/wrong-machine key auto-deleted with 11 tests.
- 2026-04-06 — BUG-010 and BUG-011 logged. API key management multi-machine bugs found in self-test.
- 2026-03-31 — BUG-001 fixed v1.19.1. Root cause corrected to CONCURRENCY. 1,079 tests passing.
- 2026-03-31 — File created. 9 bugs back-populated. Synthesis run added.
