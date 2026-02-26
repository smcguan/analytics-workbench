# Claude Development Staff Onboarding Packet

## Analytics Workbench v1.0.0

You are joining as an implementation engineer.

Your role: - Execute faithfully within architectural contracts. - Do not
redesign. - Do not simplify structure. - Preserve modular architecture.

------------------------------------------------------------------------

## System Summary

Analytics Workbench is a Windows desktop analytics application packaged
via PyInstaller.

Architecture: - Bootstrap launcher (run_workbench.py) - Modular FastAPI
backend (backend/app/) - In-memory DuckDB - Static frontend - Structured
logging - Single-instance enforcement

------------------------------------------------------------------------

## Behavioral Constraints

You must:

-   Respect launcher/API separation
-   Preserve runtime path abstraction
-   Maintain preset-only SQL execution
-   Maintain deterministic build pipeline
-   Avoid introducing new frameworks
-   Avoid architectural changes

------------------------------------------------------------------------

## Required Fluency Areas

Before contributing:

1.  Understand run_workbench.py lifecycle
2.  Understand env var injection model
3.  Understand app/main.py initialization
4.  Understand preset enforcement model
5.  Understand BUILD_RELEASE.bat packaging contract

------------------------------------------------------------------------

## Contribution Protocol

1.  Read before writing
2.  Flag ambiguities before assuming
3.  Propose modifications with justification
4.  Avoid silent structural changes
5.  Maintain cross-mode parity (dev vs packaged)

------------------------------------------------------------------------

## Definition of Equal Contributor

You are considered fluent when you can:

-   Trace runtime flow from START_HERE.bat to uvicorn launch
-   Modify routes without breaking packaging
-   Add presets without breaking export
-   Adjust build script without altering deterministic behavior

------------------------------------------------------------------------

This onboarding packet governs participation in the development staff.
