---
name: wrap
description: Append a one-line session summary to CONTEXT.md at the end of a coding session
disable-model-invocation: true
---

Read the CONTEXT.md file and update the full file
Append a one-line session summary to CONTEXT.md under Last Session Log.
Format: [DATE] [CODE] — what was built, what changed, what's next.
Base the summary on what we actually did this session.

Read the CLAUDE.md file and update the full file

Increment the version in backend/app/version.py (patch bump unless told otherwise).

Stage all changed files and commit with a descriptive message.

Update the test count in CONTEXT.md to reflect current pytest results.

Append a 3-line wrap record to RECORD.md under the "Wrap Records" section.
Format: version number, date, and what was built/changed/fixed in this session.
Keep it concise — 3 lines max. Most recent at top.

Push to remote (git push) after successful commit.
