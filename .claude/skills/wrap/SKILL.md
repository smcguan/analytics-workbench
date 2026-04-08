---
name: wrap
description: End-of-session wrap — tests, version bump, docs update, commit, push
disable-model-invocation: true
---

End-of-session wrap procedure. Run all steps in order WITHOUT stopping to ask
questions. Make all decisions autonomously using the rules below. Only stop if
tests fail — everything else has a defined rule.

**1. Run full test suite**
Run `PYTHONPATH=backend .venv/Scripts/python -m pytest tests/ -q` and confirm all pass.
Also run `PYTHONPATH=backend .venv/Scripts/python tests/run_all.py` and
`PYTHONPATH=backend .venv/Scripts/python tests/test_accuracy.py --no-ai`.
If any fail, stop and fix before proceeding. Record the total test count.
If tests pass, continue immediately without reporting interim results.

**2. Increment version**
Read the current version from `backend/app/version.py`.
Rule: if new features were added this session, bump minor (e.g. 1.21.0 → 1.22.0).
Rule: if only fixes or polish, bump patch (e.g. 1.21.0 → 1.21.1).
When in doubt, use patch. Do not ask — just apply the rule and move on.

**3. Review git diff since last wrap**
Run `git log --oneline` to see all commits this session.
Use this to build an accurate summary — do not rely on memory alone.
Do not report this step — just use it to inform the CONTEXT_AW.md update.

**4. Update BUGS_AW.md — inline confirmation**
Rule: mark every bug fixed this session as FIXED with version and commit.
Rule: do not add bugs that are still open.
Rule: do not add new bugs discovered this session unless they were also fixed.
If nothing changed, skip without comment.

**5. Update CONTEXT_AW.md — full file review**
Append a session log entry under Last Session Log:
Format: [DATE] [CODE] — what was built, what changed, what's next.
Update test count. Mark completed Next Actions with strikethrough + version.
Resolve any Open Decisions settled this session.
Update component/feature statuses if they changed.
Do not ask what to write — summarize from git log and what was built.

**6. Update CLAUDE.md — full file review**
Add new API endpoints, updated repo structure, new coding patterns.
Do not just append — read the relevant sections and update them in place.
If nothing changed that affects CLAUDE.md, skip without comment.

**7. Update Reference Guide (in frontend/index.html)**
If user-facing features changed, update the Reference Guide content in the
referenceGuideView section of index.html.
Rule: if no user-facing features changed, skip without comment.
Rule: if features changed, update affected sections only — do not rewrite the whole guide.

**8. Update Welcome screen (in frontend/index.html)**
Rule: if new features were added, update the feature highlights list.
Rule: if no new features, skip without comment.

**9. Update RECORD.md**
Add a row to the Version History table.
Append a wrap record under "Wrap Records" (most recent at top).
Format: version number, date, 2-3 lines on what was built/changed/fixed.

**10. Check for uncommitted changes**
Run `git status`. Stage all relevant changed files.
Do not ask which files to stage — stage everything modified this session.

**11. Commit with descriptive message**
Commit message format: "vX.X.X — [one line summary of what was built]"
Do not ask for confirmation — just commit.

**12. Push to remote**
Run `git push` immediately after commit. Do not ask for confirmation.

**13. Report wrap complete**
Output the wrap summary table followed immediately by the session handoff block.

Wrap summary table:
| Step | Status |
|------|--------|
| Tests | N passed (pytest + run_all + accuracy) |
| Version | vX.X.X |
| BUGS_AW.md | No changes / N bugs updated |
| CONTEXT_AW.md | Updated |
| RECORD.md | Updated |
| Commit | [hash] |
| Push | Done |

Then output the session handoff block — copy this exact format:

=== SESSION HANDOFF — AW vX.X.X — [DATE] ===

WHAT WAS BUILT:
[2-4 lines — specific components, files, and features built this session]

WHAT'S WORKING:
[2-4 lines — what is fully functional and tested right now]

WHAT'S NOT DONE YET:
[bullet list of remaining M5 items not yet built]

ACTIVE BUGS:
[list open bugs with BUG-XXX reference, or "None"]

NEXT SESSION:
[1-3 lines — exactly what to build next and why]
=================================================

The handoff block is for pasting directly into Claude.ai to orient the planning
assistant without uploading any files. Write it to be self-contained and precise —
no vague summaries, specific component names and status.
