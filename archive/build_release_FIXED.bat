@echo off
setlocal EnableExtensions EnableDelayedExpansion
title Build Analytics Workbench (Deterministic)

REM ============================================================
REM Analytics Workbench - Deterministic PyInstaller build (onedir)
REM Goals:
REM  - Build ONLY the EXE/runtime with PyInstaller (no fragile --add-data)
REM  - Always produce customer-ready folder layout next to the EXE:
REM      dist\AnalyticsWorkbench\
REM        AnalyticsWorkbench.exe
REM        _internal\
REM        frontend\ (copied from repo)
REM        data\datasets\demo\ (copied from repo)
REM        exports\ (created)
REM        logs\ (created)
REM        START_HERE.bat
REM        README.txt
REM ============================================================

set "APP_NAME=AnalyticsWorkbench"
set "ENTRY_SCRIPT=backend\run_workbench.py"
set "PORT=8008"

REM Repo root (folder containing this .bat)
set "REPO_ROOT=%~dp0"
set "RELEASEDIR=%REPO_ROOT%dist\%APP_NAME%"

echo =====================================
echo Building %APP_NAME%...
echo Repo: %REPO_ROOT%
echo =====================================

cd /d "%REPO_ROOT%"

REM ------------------------------------------------------------
REM 0) Kill anything likely to lock output folders
REM ------------------------------------------------------------
taskkill /F /IM "%APP_NAME%.exe" >nul 2>nul
taskkill /F /IM "run_workbench.exe" >nul 2>nul
taskkill /F /IM "uvicorn.exe" >nul 2>nul
REM Uncomment only if you are NOT running other Python jobs:
REM taskkill /F /IM "python.exe" >nul 2>nul
timeout /t 2 >nul

REM ------------------------------------------------------------
REM 1) Clean artifacts (and stale specs)
REM ------------------------------------------------------------
if exist "%REPO_ROOT%build" rmdir /s /q "%REPO_ROOT%build" >nul 2>nul
if exist "%REPO_ROOT%dist"  rmdir /s /q "%REPO_ROOT%dist"  >nul 2>nul
if exist "%REPO_ROOT%buildspec" rmdir /s /q "%REPO_ROOT%buildspec" >nul 2>nul
del /q "%REPO_ROOT%*.spec" >nul 2>nul

REM ------------------------------------------------------------
REM 2) Preflight checks (fail fast with clear messages)
REM ------------------------------------------------------------
if not exist "frontend\index.html" (
  echo ERROR: Missing frontend\index.html (repo frontend folder not found).
  pause
  exit /b 1
)

if not exist "frontend\logo.ico" (
  echo ERROR: Missing frontend\logo.ico (place your final icon there).
  pause
  exit /b 1
)

if not exist "data\datasets\demo" (
  echo ERROR: Missing data\datasets\demo (expected demo dataset folder).
  pause
  exit /b 1
)

py -c "import duckdb; print(duckdb.__version__)"
IF ERRORLEVEL 1 (
  pause
  exit /b 1
)
  pause
)

REM ------------------------------------------------------------
REM 3) Build with PyInstaller (ONE invocation)
REM    NOTE: We intentionally do NOT use --add-data for frontend/data
REM          because it is fragile in .bat line continuation contexts.
REM          We copy those folders explicitly after build.
REM ------------------------------------------------------------
echo.
echo Running PyInstaller...
echo.

py -m PyInstaller --noconfirm --clean --onedir --noconsole ^
  --name "%APP_NAME%" ^
  --icon "frontend\logo.ico" ^
  --distpath "dist" ^
  --workpath "build" ^
  --paths "backend" ^
  --hidden-import "app.main" ^
  --collect-submodules "app" ^
  --hidden-import "duckdb" ^
  --collect-submodules "duckdb" ^
  --collect-data "duckdb" ^
  --collect-binaries "duckdb" ^
  "%ENTRY_SCRIPT%"

IF ERRORLEVEL 1 (
  echo.
  echo =====================================
  echo Build FAILED (PyInstaller).
  echo =====================================
  pause
  exit /b 1
)

REM ------------------------------------------------------------
REM 4) Make the deliverable layout deterministic
REM ------------------------------------------------------------
echo.
echo Staging deliverable layout...
echo.

if not exist "%RELEASEDIR%\%APP_NAME%.exe" (
  echo ERROR: Expected EXE missing: "%RELEASEDIR%\%APP_NAME%.exe"
  pause
  exit /b 1
)

REM Copy frontend next to EXE
robocopy "%REPO_ROOT%frontend" "%RELEASEDIR%\frontend" /MIR /R:2 /W:1 /NFL /NDL /NJH /NJS /NP >nul
if errorlevel 8 (
  echo ERROR: robocopy failed copying frontend to release folder.
  pause
  exit /b 1
)

REM Copy demo dataset next to EXE
robocopy "%REPO_ROOT%data\datasets\demo" "%RELEASEDIR%\data\datasets\demo" /MIR /R:2 /W:1 /NFL /NDL /NJH /NJS /NP >nul
if errorlevel 8 (
  echo ERROR: robocopy failed copying demo dataset to release folder.
  pause
  exit /b 1
)

REM Ensure writable folders exist
mkdir "%RELEASEDIR%\exports" >nul 2>nul
mkdir "%RELEASEDIR%\logs" >nul 2>nul

REM ------------------------------------------------------------
REM 5) Write START_HERE.bat
REM ------------------------------------------------------------
(
  echo @echo off
  echo setlocal
  echo title Analytics Workbench
  echo cd /d "%%~dp0"
  echo.
  echo =====================================
  echo Analytics Workbench Launcher
  echo Folder: %%cd%%
  echo =====================================
  echo.
  echo Starting...
  echo.
  echo start "" "%%cd%%\%APP_NAME%.exe"
  echo.
  echo echo If the UI doesn't open automatically, open:
  echo echo   http://127.0.0.1:%PORT%/ui/
  echo.
) > "%RELEASEDIR%\START_HERE.bat"

REM ------------------------------------------------------------
REM 6) Write README.txt
REM ------------------------------------------------------------
set "READMEFILE=%RELEASEDIR%\README.txt"
del /f /q "%READMEFILE%" >nul 2>nul

>> "%READMEFILE%" echo Analytics Workbench
>> "%READMEFILE%" echo ==================
>> "%READMEFILE%" echo.
>> "%READMEFILE%" echo Start:
>> "%READMEFILE%" echo   Double-click START_HERE.bat
>> "%READMEFILE%" echo.
>> "%READMEFILE%" echo UI:
>> "%READMEFILE%" echo   http://127.0.0.1:%PORT%/ui/
>> "%READMEFILE%" echo.
>> "%READMEFILE%" echo Demo dataset:
>> "%READMEFILE%" echo   data\datasets\demo\sample.parquet
>> "%READMEFILE%" echo.
>> "%READMEFILE%" echo Exports:
>> "%READMEFILE%" echo   exports\
>> "%READMEFILE%" echo.
>> "%READMEFILE%" echo Notes:
>> "%READMEFILE%" echo - Keep the entire folder together (EXE + _internal + frontend + data^).
>> "%READMEFILE%" echo - If SmartScreen blocks: More info ^> Run anyway.

echo.
echo =====================================
echo Build complete.
echo Deliverable located at:
echo   %RELEASEDIR%
echo =====================================
echo.
pause
