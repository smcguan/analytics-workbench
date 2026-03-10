@echo off
setlocal EnableExtensions

echo.
echo Running in directory:
echo %cd%
echo.

cd /d "%~dp0"

git rev-parse --is-inside-work-tree >nul 2>&1
if errorlevel 1 (
  echo ERROR: Not inside a git repository.
  pause
  exit /b 1
)

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

echo.
echo WARNING: This will make your local dev branch exactly match origin/dev.
echo All uncommitted local changes will be lost.
set /p confirm=Proceed with force sync? [Y/N]:

if /i not "%confirm%"=="Y" (
  echo Aborted.
  pause
  exit /b
)

echo.
echo Fetching latest from origin...
git fetch origin
if errorlevel 1 (
  echo ERROR: Fetch failed.
  pause
  exit /b 1
)

echo.
echo Resetting local dev to origin/dev...
git reset --hard origin/dev
if errorlevel 1 (
  echo ERROR: Reset failed.
  pause
  exit /b 1
)

echo.
echo Removing untracked files and folders...
git clean -fd
if errorlevel 1 (
  echo ERROR: Clean failed.
  pause
  exit /b 1
)

echo.
echo Sync complete.
git status
pause