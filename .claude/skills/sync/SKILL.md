---
name: sync
description: Start-of-session sync — pull, test, read docs, orient. The opposite of /wrap.
disable-model-invocation: true
---

Start-of-session sync procedure. Run when picking up work on a new machine
or fresh terminal. This is read-only — it pulls code and reads files but
does not modify anything.

**1. Pull from remote**
Run `git pull` to get the latest code.

**2. Check environment**
Verify `.venv/Scripts/python` exists. If not, warn:
"Virtual environment not found. Run: python -m venv .venv && .venv/Scripts/pip install -r requirements.txt"

**3. Run full test suite**
Run `PYTHONPATH=backend .venv/Scripts/python -m pytest tests/ -q` and report the result.
If any tests fail, flag them prominently — do not proceed to summarize until the
analyst decides whether to fix or continue.

**4. Check version**
Read `backend/app/version.py` and report the current version.

**5. Git status**
Run `git status -s` and `git log --oneline -5` to show any local changes
and the most recent commits.

**6. Read CONTEXT.md**
Read the full file. Focus on:
- Last Session Log (most recent 3 entries) — what was just done
- Next Actions — what needs to happen next
- Open Decisions — anything pending resolution
- Product State — current milestone status

**7. Read CLAUDE.md**
Read the full file. Focus on:
- Current API endpoints
- Known bugs (active)
- Frozen UI decisions
- Coding conventions and "what not to do"

**8. Read RECORD.md**
Read the Wrap Records section — most recent 3 entries for quick version history.

**9. Summarize**
Output a brief orientation summary:
- Current version
- Test suite status (count + pass/fail)
- Last session: what was built (from session log)
- Top 3 next actions (from CONTEXT.md)
- Any active bugs
- Any uncommitted local changes
