Analytics Workbench – Architecture (V0)



One-Sentence Product Thesis

A self-contained Windows analytics application that allows users to register local Parquet datasets, run preset SQL analyses using DuckDB, and export results to Excel — without requiring Python to be installed.

Strategic Thesis

Analytics Workbench is designed as a customer silo desktop analytics tool: a packaged FastAPI + DuckDB application distributed as a Windows folder deliverable. It enables lightweight data exploration of large Parquet datasets using a simple UI and SQL presets, prioritizing local performance, minimal dependencies, and reproducible packaging.

Repository Layout

backend/

&nbsp; run\_workbench.py

&nbsp; app/

&nbsp;   main.py

&nbsp;   presets/



frontend/

&nbsp; index.html

&nbsp; logo.png



data/

&nbsp; datasets/

&nbsp;   demo/

&nbsp;     sample.parquet



BUILD\_RELEASE.bat



docs/

&nbsp; architecture.md

&nbsp; packaging.md

&nbsp; ai-dev-playbook.md



Deliverable Layout (Post-Build)

dist/AnalyticsWorkbench/

&nbsp; AnalyticsWorkbench.exe

&nbsp; START\_HERE.bat

&nbsp; README.txt

&nbsp; frontend/

&nbsp; data/

&nbsp;   datasets/

&nbsp;     demo/

&nbsp; exports/

&nbsp; \_internal/



Runtime Path Strategy

Dev Mode: BASE\_DIR derived from \_\_file\_\_; frontend from frontend/; datasets from data/datasets; exports to exports/.

Packaged Mode: BASE\_DIR = Path(sys.executable).parent; all runtime paths derived from EXE folder; never write to \_internal/.

Backend Endpoints

/ui/, /api/version, /api/datasets, /api/presets, /api/run, /api/export, /api/datasets/scan, /api/datasets/register, /api/dialog/folder

Dataset Model

Reference Mode: data/datasets/<name>/\_reference.txt containing absolute path.

Copy Mode: Parquet files stored inside data/datasets/<name>/.

Export Strategy

Preferred: DuckDB COPY ... TO 'file.xlsx' (FORMAT XLSX, HEADER TRUE). Avoid pandas/numpy dependency.

Invariants

Frontend folder must exist next to EXE. Exports folder must be writable. No writes to \_internal/. Deliverable must run on clean Windows machine without Python installed.

 



