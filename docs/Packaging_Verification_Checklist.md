Oh really Analytics Workbench v1.0.0

Packaging Verification Checklist

Run this checklist after every build and before any release. Work top to
bottom. A failure at any step should be investigated before continuing.

1 — Dev Mode Verification

Run: START\_HERE.bat from repo root. Do not use the packaged exe for this
section.

☐ App launches without errors START\_HERE.bat completes and browser opens
automatically to http://127.0.0.1:8000/ui/

☐ UI loads and displays version info Top bar shows app name and version.
No blank or error screen.

☐ logs/boot.log is created and written Open logs/boot.log — confirm a
‘=== START ===’ entry with current timestamp.

☐ logs/app.log is created and written Open logs/app.log — confirm
uvicorn startup entries are present.

☐ /api/version returns correct paths Open
http://127.0.0.1:8000/api/version in browser. Confirm base\_dir,
datasets\_dir, exports\_dir all point to repo root locations.

2 — Dataset Registration

A sample .parquet file is required. Use the demo dataset in
data/datasets/demo/ or any .parquet file on disk.

☐ Folder browse dialog opens Click Browse button in the UI. A native
Windows folder picker dialog should appear. ⚠ OPEN RISK: Not yet
verified in packaged mode. If it fails, fix is known.

☐ Folder scan finds parquet files Enter a folder path containing
.parquet files. Click Scan. File list populates with name, size, and row
count.

☐ Dataset registers in Reference mode Select a file, name the dataset,
choose Reference, click Register. Confirm status shows ‘registered’.

☐ Dataset appears in dataset dropdown After registration, the dataset
dropdown in the Run panel should include the new dataset.

☐ \_reference.txt created on disk Navigate to data/datasets//. Confirm
\_reference.txt exists and contains the correct file path.

3 — Preset Query Execution

☐ Presets load in dropdown Open /api/presets in browser. Confirm two
presets are listed: hcpcs\_summary and hcpcs\_over\_threshold.

☐ HCPCS Summary preset runs Select a registered dataset, select ‘HCPCS
Summary’, click Run. Results table populates. Row count and elapsed time
shown.

☐ HCPCS Over Threshold preset runs Select ‘HCPCS Over Threshold’, run
with default threshold. Results return only rows above $100,000,000.

☐ Custom threshold applies correctly Enter a custom threshold value
(e.g. 50000000). Results should change to reflect the new filter.

☐ Preview capped at 200 rows Run a large dataset. Status pill should
show full row count but table shows up to 200 preview rows only.

4 — Export Download

☐ Export triggers file download After running a query, click Export.
Browser should prompt a file download or auto-download.

☐ XLSX file is valid Open the downloaded file in Excel. Confirm columns
and data match the preview table.

☐ File appears in exports/ folder Check the exports/ directory. A
timestamped .xlsx (or .csv fallback) file should be present.

ℹ If XLSX download fails silently, check exports/ for a .csv file —
DuckDB XLSX extension may be unavailable. This is the expected fallback
behavior.

5 — Packaged Mode Verification

Run BUILD\_RELEASE.bat first. Then launch
dist/AnalyticsWorkbench/AnalyticsWorkbench.exe. Do not use
START\_HERE.bat for this section.

☐ Build completes without errors BUILD\_RELEASE.bat runs to completion.
Final output: ‘BUILD COMPLETE’ with dist/AnalyticsWorkbench/ directory
listing.

☐ dist/ folder structure is correct Confirm: AnalyticsWorkbench.exe,
\_internal/, frontend/index.html, data/, exports/, logs/ are all present.

☐ App launches from exe Double-click AnalyticsWorkbench.exe. Browser
opens to /ui/ automatically. No console window appears.

☐ UI loads correctly from packaged frontend All UI elements render. No
broken images or missing styles.

☐ Version endpoint reflects correct paths Hit /api/version — confirm
paths point to the dist/AnalyticsWorkbench/ folder, not the repo root.

☐ Query executes in packaged mode Register a dataset and run a preset.
Results return. DuckDB is functioning inside the bundle.

☐ Export works in packaged mode Export a result. File downloads and
appears in the packaged exports/ folder.

☐ Folder browse works in packaged mode Click Browse. Native folder
dialog should appear. ⚠ OPEN RISK: Tkinter not yet verified in packaged
build. If this fails, add –collect-all tkinter to BUILD\_RELEASE.bat
PyInstaller call.

☐ logs/ files written in packaged mode After launch, check
dist/AnalyticsWorkbench/logs/. Both boot.log and app.log should exist
and contain entries.

6 — Shutdown

☐ Quit button shuts down server Click Quit in the UI. Server process
should terminate. Browser tab goes idle or blank.

☐ App can be relaunched immediately After quitting, relaunch the app. It
should start on port 8000 without conflict.

☐ Single-instance kill works on re-launch Launch app, leave it running,
launch again. Second launch should take over cleanly.

Reference Notes

• Authoritative build script: BUILD\_RELEASE.bat only. All other .bat
variants are deprecated.

• Dev mode base directory: repo root. Packaged base directory: folder
containing the .exe.

• Do not commit .venv/, build/, dist/, or exports/ to the repository.

• A failure in Section 5 that does not appear in Section 1 is a
packaging problem, not a code problem.



systems works from build to run, folder dialog works

