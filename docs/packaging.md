Analytics Workbench – Packaging \& Release (Windows) (V0)



Packaging Goal

Deliver a self-contained Windows folder requiring no Python installation, runnable via START\_HERE.bat, with working UI, dataset registration, and Excel export.

Build Mode

PyInstaller --onedir (avoid onefile extraction issues).

Build Prerequisites

Windows 11, Python 3.13, fastapi, uvicorn, duckdb installed.

Build Command

Use BUILD\_RELEASE.bat with appropriate PyInstaller flags for backend/app, frontend, and demo dataset.

Deliverable Must Contain

AnalyticsWorkbench.exe

START\_HERE.bat

README.txt

frontend/

data/datasets/demo/

exports/

\_internal/



Smoke Test Procedure

1\. Delete dist. 2. Run BUILD\_RELEASE.bat. 3. Copy folder to Desktop. 4. Run START\_HERE.bat. 5. Open UI. 6. Register dataset. 7. Run preset. 8. Export Excel and confirm in exports/.

Common Failures \& Fixes

404 /api/dialog/folder → Missing endpoint.

Frontend not loading → frontend folder not copied.

numpy missing → Remove pandas or bundle numpy.

Exports missing → Ensure mkdir on startup.

dist locked → Kill running processes before build.

 



