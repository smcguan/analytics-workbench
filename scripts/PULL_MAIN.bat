@echo off
setlocal

echo.
echo =====================================
echo        PULL ANALYTICS WORKBENCH
echo =====================================
echo.

for %%I in ("%~dp0..") do set "REPO=%%~fI"
cd /d "%REPO%"

if errorlevel 1 (
    echo ERROR: Could not access repo directory.
    pause
    exit /b 1
)

git rev-parse --is-inside-work-tree >nul 2>&1
if errorlevel 1 (
    echo ERROR: Parent directory is not a git repository.
    pause
    exit /b 1
)

for /f "delims=" %%i in ('git branch --show-current') do set "CURRENT_BRANCH=%%i"

if /I not "%CURRENT_BRANCH%"=="main" (
    echo ERROR: You are on branch "%CURRENT_BRANCH%".
    echo Switch to main before pulling.
    pause
    exit /b 1
)

echo Repo root:
git rev-parse --show-toplevel
echo Branch: %CURRENT_BRANCH%
echo.

echo Local HEAD before fetch:
git rev-parse HEAD
echo.

echo Fetching origin/main...
git fetch origin main
if errorlevel 1 (
    echo ERROR: Fetch failed.
    pause
    exit /b 1
)

echo.
echo Remote origin/main after fetch:
git rev-parse origin/main
echo.

echo Pulling origin/main...
git pull --ff-only origin main
if errorlevel 1 (
    echo ERROR: Pull failed.
    pause
    exit /b 1
)

echo.
echo Local HEAD after pull:
git rev-parse HEAD
echo.

echo Recent commits:
git log --oneline -3
echo.
pause
endlocal