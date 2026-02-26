@echo off
setlocal EnableExtensions

REM Go to repo root (folder this script lives in)
cd /d "%~dp0"

REM Sanity: must be inside a git repo
git rev-parse --is-inside-work-tree >nul 2>&1
if errorlevel 1 (
  echo ERROR: Not inside a git repository.
  pause
  exit /b 1
)

REM Ensure we are on dev
for /f "delims=" %%b in ('git branch --show-current') do set "BRANCH=%%b"
if /i not "%BRANCH%"=="dev" (
  echo Switching to dev branch...
  git checkout dev
  if errorlevel 1 (
    echo ERROR: Could not checkout dev.
    pause
    exit /b 1
  )
)

REM Capture current HEAD to detect changes
for /f "delims=" %%h in ('git rev-parse HEAD') do set "OLDHEAD=%%h"

echo Pulling latest from origin/dev...
git pull
if errorlevel 1 (
  echo ERROR: Pull failed. Resolve issues, then retry.
  pause
  exit /b 1
)

for /f "delims=" %%h in ('git rev-parse HEAD') do set "NEWHEAD=%%h"

echo.
echo Status:
git status

REM If HEAD changed, remind about deps
if /i not "%OLDHEAD%"=="%NEWHEAD%" (
  echo.
  echo Repo updated.
  if exist "backend\requirements.txt" (
    echo If dependencies changed, run:
    echo   .\.venv\Scripts\python.exe -m pip install -r backend\requirements.txt
  )
) else (
  echo.
  echo Already up to date.
)

pause