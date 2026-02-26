\# Analytics Workbench – System Architecture



\## 1. Product Purpose



Analytics Workbench is a local Windows desktop analytics tool.



It allows users to:

\- Register Parquet datasets (reference or copy mode)

\- Execute predefined SQL queries (presets)

\- Export results to Excel (via DuckDB COPY)

\- Run entirely locally (no cloud dependency)



It is packaged using PyInstaller (`--onedir`) for distribution to machines without Python installed.



---



\## 2. High-Level Architecture



Frontend (static HTML/JS)

&nbsp;       ↓

FastAPI backend (Python)

&nbsp;       ↓

DuckDB (embedded analytics engine)

&nbsp;       ↓

Local Parquet files



Everything runs locally on the user's machine.



No server infrastructure.

No external database.

No external services.



---



\## 3. Repository Structure



backend/

run\_workbench.py → entrypoint

app/

main.py → FastAPI app

presets/ → SQL preset queries



frontend/

index.html → UI



data/

datasets/

demo/ → sample dataset only



BUILD\_RELEASE.bat

START\_HERE.bat

DOCTOR.bat

OPERATIONS.md





---



\## 4. Runtime Path Strategy



The application resolves paths differently depending on context:



\### Dev Mode

\- BASE\_DIR = repo root (derived from \_\_file\_\_)



\### Packaged Mode

\- BASE\_DIR = Path(sys.executable).parent



Important rule:

Never write to `sys.\_MEIPASS`.

All writable paths resolve relative to BASE\_DIR.



---



\## 5. Dataset Architecture



Datasets live under:  



data/datasets/<dataset\_name>/





Two modes:



\### Reference Mode (preferred)



\_reference.txt



Contains absolute path to Parquet file.



\### Copy Mode

Parquet files are copied into dataset folder.



Resolution logic:

\- If `\_reference.txt` exists → use referenced path

\- Else → use local `\*.parquet`



---



\## 6. Export Architecture



Exports do NOT use pandas.



Instead:



1\. Create temporary view:



CREATE TEMP VIEW v AS <query>



2\. Export:



COPY v TO 'file.xlsx' (FORMAT XLSX, HEADER TRUE)





Reason:

Avoid pandas/numpy packaging complexity.



---



\## 7. Packaging Model



PyInstaller:

\- Mode: `--onedir`

\- Rationale:

&nbsp;- Avoid temp extraction issues

&nbsp;- Avoid antivirus friction

&nbsp;- Simplify runtime path logic



Deliverable structure:



AnalyticsWorkbench/

AnalyticsWorkbench.exe

frontend/

data/

exports/

logs/

\_internal/





START\_HERE.bat:

\- Launches EXE if packaged

\- Launches dev server if in repo



---



\## 8. Build Philosophy



\- Build from clean virtual environment

\- Dependencies locked in `backend/requirements.txt`

\- No build artifacts committed

\- Version tagged before release



---



\## 9. Known Architectural Constraints



\- Windows-only target

\- Local file system access required

\- DuckDB version must support XLSX export

\- PyInstaller packaging sensitive to hidden imports



---



\## 10. Design Goals



\- Deterministic builds

\- Zero external dependencies

\- Reproducible dev environments

\- Clear separation of source vs artifacts

\- Safe dataset handling



---



This system is intentionally simple, local-first, and packaging-stable.













