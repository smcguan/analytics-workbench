@echo off
setlocal EnableExtensions EnableDelayedExpansion

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

REM ------------------------------------------------------------
REM Check for local modifications before attempting pull
REM ------------------------------------------------------------
echo Checking for local changes...

git diff --quiet
set "UNSTAGED=%errorlevel%"

git diff --cached --quiet
set "STAGED=%errorlevel%"

if %UNSTAGED%==1 (
    echo.
    echo WARNING: You have unstaged local changes:
    git diff --name-status
    echo.
)

if %STAGED%==1 (
    echo.
    echo WARNING: You have staged but uncommitted changes:
    git diff --cached --name-status
    echo.
)

if %UNSTAGED%==1 (
    echo These local changes will block the pull.
    echo.
    echo Options:
    echo   [S] Stash your changes, pull, then restore them afterward
    echo   [D] Discard your changes permanently and pull
    echo   [A] Abort - let me handle this manually
    echo.
    choice /C SDA /M "Choose an option (S=Stash, D=Discard, A=Abort)"

    if errorlevel 3 (
        echo.
        echo Aborted. No changes were made.
        echo Run one of the following manually before pulling:
        echo   git stash
        echo   git restore .
        pause
        exit /b 1
    )

    if errorlevel 2 (
        echo.
        echo Discarding all local changes...
        git restore .
        if errorlevel 1 (
            echo ERROR: Could not discard changes.
            pause
            exit /b 1
        )
        echo Done. Local changes discarded.
        echo.
    )

    if errorlevel 1 (
        echo.
        echo Stashing local changes...
        git stash push -m "PULL_MAIN auto-stash before pull"
        if errorlevel 1 (
            echo ERROR: git stash failed.
            pause
            exit /b 1
        )
        echo Done. Changes stashed.
        echo.
        set "STASHED=1"
    )
)

REM ------------------------------------------------------------
REM Proceed with fetch and pull
REM ------------------------------------------------------------
for /f "delims=" %%H in ('git rev-parse HEAD') do set "HEAD_BEFORE=%%H"
echo Local HEAD before pull: %HEAD_BEFORE%
echo.

echo Fetching origin/main...
git fetch origin main
if errorlevel 1 (
    echo ERROR: Fetch failed.
    pause
    exit /b 1
)

for /f "delims=" %%R in ('git rev-parse origin/main') do set "REMOTE_HEAD=%%R"
echo Remote origin/main: %REMOTE_HEAD%
echo.

if "%HEAD_BEFORE%"=="%REMOTE_HEAD%" (
    echo =====================================
    echo  Already up to date. No files changed.
    echo =====================================
    echo.
    goto :SKIP_PULL
)

echo Pulling origin/main...
git pull --ff-only origin main
if errorlevel 1 (
    echo ERROR: Pull failed.
    pause
    exit /b 1
)

for /f "delims=" %%H in ('git rev-parse HEAD') do set "HEAD_AFTER=%%H"
echo.
echo =====================================
echo  Pull complete. Files updated.
echo =====================================
echo Before: %HEAD_BEFORE%
echo After:  %HEAD_AFTER%
echo.
echo Files changed:
git diff --name-only %HEAD_BEFORE% %HEAD_AFTER%
echo.

:SKIP_PULL

REM ------------------------------------------------------------
REM Restore stash if we stashed earlier
REM ------------------------------------------------------------
if defined STASHED (
    echo Restoring your stashed changes...
    git stash pop
    if errorlevel 1 (
        echo.
        echo WARNING: Stash restore had conflicts.
        echo Your changes are still saved in the stash.
        echo Run "git stash pop" manually and resolve any conflicts.
        echo.
    ) else (
        echo Done. Your local changes have been restored on top of the pull.
        echo.
    )
)

REM ------------------------------------------------------------
REM Warn about any untracked files (e.g. index - Copy.html)
REM ------------------------------------------------------------
for /f "delims=" %%u in ('git ls-files --others --exclude-standard') do (
    set "HAS_UNTRACKED=1"
)
if defined HAS_UNTRACKED (
    echo.
    echo NOTE: Untracked files found in your repo folder:
    git ls-files --others --exclude-standard
    echo These are not affecting Git but may be clutter worth cleaning up.
    echo.
)

echo Recent commits:
git log --oneline -3
echo.
pause
endlocal
