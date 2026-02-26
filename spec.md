# Analytics Workbench

## Product Specification & Architectural Contract

Version: 1.0.0\
Status: Stable Release (Repository Mirror)\
Scope: Deterministic Windows Desktop Analytics Application

------------------------------------------------------------------------

# 1. Product Identity

Name: Analytics Workbench\
Version: v1.0.0\
Type: Local-first Windows desktop analytics application\
Backend: FastAPI (modular architecture)\
Engine: DuckDB (in-process, in-memory)\
Packaging: PyInstaller --onedir\
Execution: Fully local (no cloud dependencies)

This document mirrors the actual implemented repository architecture of
v1.0.0.

------------------------------------------------------------------------

# 2. Architectural Overview

The system consists of:

1.  Bootstrap launcher (backend/run_workbench.py)
2.  Modular FastAPI backend (backend/app/)
3.  Runtime path abstraction
4.  Structured file logging
5.  Single-instance enforcement
6.  Static frontend
7.  Deterministic Windows packaging

Launcher and API application are intentionally separated.

------------------------------------------------------------------------

# 3. Repository Structure (Actual)

backend/ run_workbench.py requirements.txt Dockerfile app/ main.py
paths.py version.py presets/ routes/

frontend/ data/ BUILD_RELEASE.bat START_HERE.bat AnalyticsWorkbench.spec
README.txt RELEASE_NOTES.md SYSTEM_ARCHITECTURE.md
ARCHITECTURAL_DECISIONS.md DEVELOPMENT_SOP.md OPERATIONS.md .gitignore

build/ and dist/ are artifacts and must not be committed. .venv/ must
not be committed.

------------------------------------------------------------------------

# 4. Execution Modes

## Development Mode

-   START_HERE.bat runs .venv`\Scripts`{=tex}`\python`{=tex}.exe
    backend`\run`{=tex}\_workbench.py
-   Base directory = repo root

## Packaged Mode

-   Runs AnalyticsWorkbench.exe
-   Base directory = folder containing exe
-   No global Python dependency

------------------------------------------------------------------------

# 5. Bootstrap Launcher Contract (run_workbench.py)

Responsibilities:

-   Resolve base directory (frozen-aware)
-   Create exports/, logs/, data/datasets/
-   Inject environment variables:
    -   AW_FRONTEND_DIR
    -   AW_DATA_DIR
    -   AW_DATASETS_DIR
    -   AW_EXPORTS_DIR
-   Configure structured logging (app.log, boot.log)
-   Enforce single-instance port logic
-   Auto-open browser
-   Launch uvicorn with imported FastAPI app

This file is NOT the API definition.

------------------------------------------------------------------------

# 6. FastAPI Application Layer (backend/app/)

main.py: - Creates FastAPI app - Mounts static frontend - Registers
route modules - Initializes in-memory DuckDB

paths.py: - Central runtime path resolver using injected env vars

presets/: - Modular preset SQL definitions - Preset-only query model (no
arbitrary SQL)

routes/: - Modular route organization

------------------------------------------------------------------------

# 7. Data Layer

-   Single DuckDB in-memory connection
-   Parquet registration via read_parquet()
-   No persistence
-   No background workers

------------------------------------------------------------------------

# 8. Frontend

-   Pure static HTML
-   No template engines
-   API-driven interactions only

------------------------------------------------------------------------

# 9. Development Environment

Target: Windows 11\
Python: 3.14.x\
Virtual environment: .venv

Pinned dependencies in backend/requirements.txt\
Regenerate with:

.venv`\Scripts`{=tex}`\python`{=tex}.exe -m pip freeze \>
backend`\requirements`{=tex}.txt

------------------------------------------------------------------------

# 10. Deterministic Build

BUILD_RELEASE.bat must:

1.  Ensure .venv
2.  Install pinned requirements
3.  Verify DuckDB import
4.  Clean build/ and dist/
5.  Run PyInstaller --onedir
6.  Copy frontend + data
7.  Produce runnable folder

------------------------------------------------------------------------

# 11. Deliverable Output

dist/AnalyticsWorkbench/ AnalyticsWorkbench.exe \_internal/ frontend/
data/ exports/ logs/ START_HERE.bat README.txt RELEASE_NOTES.md

------------------------------------------------------------------------

# 12. Definition of Done

-   Launcher resolves paths correctly
-   Logging functional
-   Preset queries execute
-   Export works
-   Single-instance enforcement works
-   Build reproducible on clean Windows 11
