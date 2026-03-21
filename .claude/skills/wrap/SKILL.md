---
name: wrap
description: End-of-session wrap — tests, version bump, docs update, commit, push
disable-model-invocation: true
---

End-of-session wrap procedure. Run all steps in order:

**1. Run full test suite**
Run `PYTHONPATH=backend .venv/Scripts/python -m pytest tests/ -q` and confirm all pass.
If any fail, stop and fix before proceeding. Record the test count.

**2. Increment version**
Bump the patch version in `backend/app/version.py` (e.g. 1.9.0 → 1.9.1).
Use minor bump if new features were added. Use patch for fixes only.

**3. Review git diff since last wrap**
Run `git log --oneline` to see all commits this session. Use this to build
an accurate summary of what was built, changed, and fixed — do not rely on
memory alone.

**4. Update CONTEXT.md — full file review**
- Append a session log entry under Last Session Log:
  Format: [DATE] [CODE] — what was built, what changed, what's next.
- Update test count to match step 1 results.
- Mark completed items in Next Actions (strikethrough + version).
- Resolve any Open Decisions that were settled this session.
- Update component/feature statuses if they changed.
- Update bug tracker entries if bugs were fixed or found.

**5. Update CLAUDE.md — full file review**
- Add new API endpoints to the endpoints list.
- Add new bugs to the resolved or active bugs section.
- Update repository structure if new directories were added.
- Update dataset storage structure if new storage paths exist.
- Update coding conventions if new patterns were established.
- Do NOT just append — read the relevant sections and update them.

**6. Update Reference Guide (in frontend/index.html)**
- If any user-facing features changed this session, update the Reference
  Guide content in the `referenceGuideView` section of index.html.
- Check: sidebar layout, import flow, sessions/snapshots, privacy model,
  example cases, troubleshooting — update any section that was affected.
- The Reference Guide is the in-app documentation. It must stay accurate.

**7. Review and update Welcome screen (in frontend/index.html)**
- Read the `welcomeDefaultCard` and `welcomeResumeCard` sections of index.html.
- Update the feature highlights list if new features were added this session.
- Update any version references or capability descriptions that changed.
- The Welcome screen is the first thing a new user sees — keep it current.

**8. Update RECORD.md**
- Add a row to the Version History table.
- Append a 3-line wrap record under "Wrap Records" (most recent at top):
  Format: version number, date, and what was built/changed/fixed.

**9. Check for uncommitted changes**
Run `git status` and warn if there are unstaged changes that would be
left behind. Stage all relevant changed files.

**10. Commit with descriptive message**
Stage all updated files and commit. Message should summarize the session.

**11. Push to remote**
Run `git push` after successful commit.
