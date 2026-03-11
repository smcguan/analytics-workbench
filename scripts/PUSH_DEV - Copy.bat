@echo off
setlocal EnableExtensions EnableDelayedExpansion

echo.
echo Running in directory:
echo %cd%
echo.


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

REM Pull first to reduce conflicts
echo Pulling latest from origin/dev...
git pull
if errorlevel 1 (
  echo ERROR: git pull failed. Resolve issues, then retry.
  pause
  exit /b 1
)

REM Show status
echo.
echo Current status:
git status

REM ====== NEW #1: Confirm before staging ======
echo.
echo About to stage ALL changes (git add -A).
choice /c YN /m "Proceed?"
if errorlevel 2 (
  echo.
  echo Aborted. Nothing staged or committed.
  pause
  exit /b 0
)

REM Stage everything (respects .gitignore)
echo.
echo Staging changes...
git add -A

REM If nothing staged, exit cleanly
git diff --cached --quiet
if not errorlevel 1 (
  echo.
  echo Nothing to commit.
  pause
  exit /b 0
)

REM ====== NEW #2: Timestamped commit message ======
REM Build timestamp like 2026-02-24 13:05:09 (uses WMIC for locale-independent format)
for /f %%i in ('wmic os get localdatetime ^| find "."') do set "LDT=%%i"
set "STAMP=!LDT:~0,4!-!LDT:~4,2!-!LDT:~6,2! !LDT:~8,2!:!LDT:~10,2!:!LDT:~12,2!"

echo.
set "MSG="
set /p MSG=Commit message: 
if "%MSG%"=="" (
  echo ERROR: Commit message cannot be empty.
  pause
  exit /b 1
)

set "FULLMSG=%MSG% [%STAMP%]"

REM Commit + push
git commit -m "%FULLMSG%"
if errorlevel 1 (
  echo ERROR: Commit failed.
  pause
  exit /b 1
)

echo.
echo Pushing to origin/dev...
git push
if errorlevel 1 (
  echo ERROR: Push failed.
  pause
  exit /b 1
)

echo.
echo Done. dev is pushed and synced.
pause
