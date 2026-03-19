@echo off
setlocal

REM ==========================================================
REM Sync reference library CSVs from source to packaged build
REM Use this after adding new CSVs to data\reference_library\
REM without doing a full rebuild.
REM ==========================================================

cd /d "%~dp0\.."

if not exist data\reference_library (
    echo ERROR: Source reference library not found at data\reference_library
    pause
    exit /b 1
)

if not exist dist\AnalyticsWorkbench\data\reference_library (
    mkdir dist\AnalyticsWorkbench\data\reference_library
)

echo Syncing reference library CSVs...
xcopy data\reference_library\*.csv dist\AnalyticsWorkbench\data\reference_library /Y

if exist data\reference_library\_library.json (
    copy /Y data\reference_library\_library.json dist\AnalyticsWorkbench\data\reference_library\
)

echo.
echo Sync complete. New CSVs will appear in AW on next library browser open.
pause
endlocal
