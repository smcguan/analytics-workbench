---
name: eod
description: End of day — wrap, summarize what was accomplished, line up tomorrow's priorities
disable-model-invocation: true
---

End of day procedure. Run all steps in order.

**1. Run the wrap skill first**
Execute all 13 steps of the wrap skill before proceeding.
Do not generate the EOD summary until the wrap is complete and pushed.

**2. Generate EOD summary**
After wrap is confirmed complete, output the following EOD block:

=== END OF DAY — AW vX.X.X — [DATE] ===

ACCOMPLISHED TODAY:
[3-5 bullet points — what was built, fixed, or validated today.
Pull from today's session log entries. Be specific: feature names,
version numbers, test counts, what was validated.]

PRODUCT STATE:
[2-3 lines — where AW stands right now. What's working,
what's demo-ready, what's not built yet.]

FARRAGUT READINESS:
[1-2 lines — is AW ready to demo for Farragut today?
What's the strongest demo path right now?]

TOMORROW — TOP 3 PRIORITIES:
1. [Highest priority item with one sentence of context]
2. [Second priority]
3. [Third priority]

OPEN ITEMS NEEDING DECISION:
[Any decisions that are blocking progress — flag them clearly.
If none, write "None blocking."]
=================================================

Write the EOD block to be self-contained — specific enough that
reading it tomorrow morning immediately orients the next session.
