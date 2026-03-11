@echo off
setlocal

REM ==========================================================
REM Analytics Workbench Build Script
REM ==========================================================

echo ==========================================================
echo Building AnalyticsWorkbench...
echo ==========================================================

REM Move to repo root (script lives in /scripts)
cd /d "%~dp0\.."

echo Repo: %cd%
echo.

REM ==========================================================
REM Verify backend requirements file exists
REM ==========================================================

if not exist backend\requirements.txt (
    echo ERROR: backend\requirements.txt not found.
    echo Expected location: backend\requirements.txt
    pause
    exit /b 1
)

REM ==========================================================
REM Install requirements
REM ==========================================================

echo Installing requirements...
pip install -r backend\requirements.txt

if errorlevel 1 (
    echo ERROR: Failed installing requirements.
    pause
    exit /b 1
)

echo.
echo Requirements installed successfully.
echo.

REM ==========================================================
REM Build executable
REM ==========================================================

echo Building executable...

pyinstaller AnalyticsWorkbench.spec

if errorlevel 1 (
    echo ERROR: Build failed.
    pause
    exit /b 1
)

REM Move START_HERE.bat to the executable directory
if exist START_HERE.bat (
    echo Moving START_HERE.bat to dist\AnalyticsWorkbench...
    copy /Y START_HERE.bat dist\AnalyticsWorkbench\
)

echo.
echo ==========================================================
echo Build complete.
echo Output located in:
echo dist\AnalyticsWorkbench
echo ==========================================================

pause
endlocal