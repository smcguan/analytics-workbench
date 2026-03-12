@echo off
setlocal

echo.
echo =====================================
echo        PUSH ANALYTICS WORKBENCH
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
    echo Switch to main before pushing.
    pause
    exit /b 1
)

echo Repo root:
git rev-parse --show-toplevel
echo Branch: %CURRENT_BRANCH%
echo.

echo Current HEAD before commit:
git log --oneline -1
echo.

echo Pulling latest origin/main first...
git pull --ff-only origin main
if errorlevel 1 (
    echo.
    echo ERROR: Pull failed.
    pause
    exit /b 1
)

echo.
echo Current status:
git status --short
echo.

set /p MSG=Enter commit message: 

if "%MSG%"=="" (
    echo ERROR: Commit message cannot be empty.
    pause
    exit /b 1
)

echo.
echo Staging changes...
git add .

echo.
echo Status after staging:
git status --short
echo.

git diff --cached --quiet
if not errorlevel 1 goto HAS_CHANGES

echo No staged changes to commit.
pause
exit /b 0

:HAS_CHANGES
echo Creating commit...
git commit -m "%MSG%"
if errorlevel 1 (
    echo.
    echo ERROR: Commit failed.
    pause
    exit /b 1
)

echo.
echo New HEAD after commit:
git log --oneline -1
echo.

echo Pushing to origin/main...
git push origin main
if errorlevel 1 (
    echo.
    echo ERROR: Push failed.
    pause
    exit /b 1
)

echo.
echo Verifying remote tracking state...
git fetch origin main >nul 2>&1

echo Local HEAD:
git rev-parse HEAD
echo Remote origin/main:
git rev-parse origin/main
echo.

echo Push successful.
pause
endlocal