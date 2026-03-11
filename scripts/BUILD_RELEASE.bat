@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM Always run from repo root (folder this BAT lives in)
cd /d "%~dp0"

set "APP_NAME=AnalyticsWorkbench"
set "ENTRY_SCRIPT=backend\run_workbench.py"
set "DIST_DIR=dist\%APP_NAME%"

echo =====================================================
echo Building %APP_NAME%...
echo Repo: %cd%
echo =====================================================

taskkill /IM AnalyticsWorkbench.exe /F >nul 2>&1

REM --- 1) Ensure venv exists ---
if not exist ".venv\Scripts\python.exe" (
  echo Creating virtual environment...
  py -m venv .venv
  if errorlevel 1 (
    echo ERROR: Failed to create virtual environment.
    pause
    exit /b 1
  )
)

set "PY=%cd%\.venv\Scripts\python.exe"

REM --- 2) Install dependencies (locked) ---
echo Installing requirements...
"%PY%" -m pip install --upgrade pip >nul
"%PY%" -m pip install -r backend\requirements.txt
if errorlevel 1 (
  echo ERROR: Failed installing requirements.
  pause
  exit /b 1
)

REM --- 3) Verify duckdb import ---
"%PY%" -c "import duckdb; print('duckdb OK:', duckdb.__version__)"
if errorlevel 1 (
  echo ERROR: duckdb not importable in venv.
  pause
  exit /b 1
)

REM --- 4) Clean previous build ---
echo Cleaning previous build...
if exist build rmdir /s /q build
if exist dist  rmdir /s /q dist

REM --- 5) Run PyInstaller (onedir) ---
echo Running PyInstaller...
"%PY%" -m PyInstaller --noconfirm --clean --onedir --noconsole ^
  --name "%APP_NAME%" ^
  --paths "backend" ^
  --add-data "backend\app;app" ^
  --hidden-import "app.main" ^
  --collect-submodules "app" ^
  --hidden-import "duckdb" ^
  --collect-submodules "duckdb" ^
  --collect-data "duckdb" ^
  --collect-binaries "duckdb" ^
  "%ENTRY_SCRIPT%"

if errorlevel 1 (
  echo ERROR: PyInstaller failed.
  pause
  exit /b 1
)

REM --- 6) Stage customer-facing files into dist folder ---
echo.
echo Staging deliverable assets into %DIST_DIR% ...

if not exist "%DIST_DIR%" (
  echo ERROR: Dist folder not found: %DIST_DIR%
  pause
  exit /b 1
)

REM Ensure folders exist
if not exist "%DIST_DIR%\frontend" mkdir "%DIST_DIR%\frontend"
if not exist "%DIST_DIR%\data\datasets\demo" mkdir "%DIST_DIR%\data\datasets\demo"
if not exist "%DIST_DIR%\exports" mkdir "%DIST_DIR%\exports"

REM Copy frontend (must contain index.html)
if not exist "frontend\index.html" (
  echo ERROR: frontend\index.html missing in repo.
  pause
  exit /b 1
)
xcopy /E /I /Y "frontend\*" "%DIST_DIR%\frontend\" >nul

REM Copy demo dataset (optional but recommended)
if exist "data\datasets\demo\" (
  xcopy /E /I /Y "data\datasets\demo\*" "%DIST_DIR%\data\datasets\demo\" >nul
)

REM Copy top-level launch + docs if present
if exist "START_HERE.bat" copy /Y "START_HERE.bat" "%DIST_DIR%\" >nul
if exist "README.txt"     copy /Y "README.txt"     "%DIST_DIR%\" >nul
if exist "RELEASE_NOTES.md" copy /Y "RELEASE_NOTES.md" "%DIST_DIR%\" >nul

echo.
echo =====================================================
echo BUILD COMPLETE
echo Output folder: %DIST_DIR%\
echo =====================================================
dir "%DIST_DIR%"
echo.
pause

