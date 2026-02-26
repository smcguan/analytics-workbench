AI Dev Playbook – Analytics Workbench (V0)



Purpose

Define structured workflow for using AI tools during development and prevent conversational drift.

Tool Roles

ChatGPT: Debugging, packaging troubleshooting, command generation.

Claude: Full-file rewrites, multi-file refactors, structural cleanup.

Operating Rules

Prefer full-file rewrites. After major changes: build, smoke test, commit. No new dependency without updating packaging.md. Centralize dev vs packaged path logic. Avoid pandas unless necessary.

Standard Request Format

Environment, Goal, Current files, Error, Definition of done.

Debug Packet Template

OS, Python version, Build method, Exact error, dist folder tree, Last 30 lines of build log.

Thread Migration Protocol

Commit and tag repo, generate extraction doc, cross-check against files, start new thread with canonical docs and files.

Milestone Discipline

Each milestone requires acceptance criteria, smoke test, packaging validation, and commit. Avoid unbounded debugging sessions.



