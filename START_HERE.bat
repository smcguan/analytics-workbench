@echo off
setlocal EnableExtensions

REM Load environment variables from .env
for /f "delims=" %%x in (.env) do set %%x

REM Always run from the folder this BAT lives in
set "ROOT=%~dp0"
pushd "%ROOT%"

REM 1) If we're in a built deliverable folder, the EXE should be next to this BAT
if exist "%ROOT%AnalyticsWorkbench.exe" (
  start "" "%ROOT%AnalyticsWorkbench.exe"
  popd
  exit /b 0
)

REM 2) If we're in repo root, the EXE will be under dist\
if exist "%ROOT%dist\AnalyticsWorkbench\AnalyticsWorkbench.exe" (
  start "" "%ROOT%dist\AnalyticsWorkbench\AnalyticsWorkbench.exe"
  popd
  exit /b 0
)

REM 3) Dev mode: run venv Python directly (new window, non-blocking)
if exist "%ROOT%.venv\Scripts\python.exe" (
  start "Analytics Workbench (dev)" "%ROOT%.venv\Scripts\python.exe" "%ROOT%backend\run_workbench.py"
  popd
  exit /b 0
)

echo ERROR: Could not find built EXE or venv Python.
echo Expected one of:
echo   %ROOT%AnalyticsWorkbench.exe
echo   %ROOT%dist\AnalyticsWorkbench\AnalyticsWorkbench.exe
echo   %ROOT%.venv\Scripts\python.exe
pause
popd
exit /b 1