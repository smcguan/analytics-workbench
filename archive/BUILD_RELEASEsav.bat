@echo off
setlocal enabledelayedexpansion
title Build Analytics Workbench

set "APP_NAME=AnalyticsWorkbench"
set "ENTRY_SCRIPT=backend\run_workbench.py"

echo =====================================
echo Building %APP_NAME%...
echo =====================================

cd /d "%~dp0"

REM ------------------------------------------------------------
REM 0) Kill anything likely to lock output folders
REM ------------------------------------------------------------
taskkill /F /IM %APP_NAME%.exe >nul 2>nul
taskkill /F /IM uvicorn.exe >nul 2>nul
taskkill /F /IM python.exe >nul 2>nul
timeout /t 2 >nul

REM ------------------------------------------------------------
REM 1) Clean build artifacts + stale spec
REM ------------------------------------------------------------
taskkill /F /IM %APP_NAME%.exe >nul 2>nul
taskkill /F /IM run_workbench.exe >nul 2>nul
taskkill /F /IM python.exe >nul 2>nul
timeout /t 2 >nul

if exist build rmdir /s /q build
if exist dist rmdir /s /q dist
if exist buildspec rmdir /s /q buildspec
del /q *.spec >nul 2>nul

REM ------------------------------------------------------------
REM 2) Preflight: confirm duckdb importable in builder python
REM ------------------------------------------------------------
py -c "import duckdb; print('duckdb:', duckdb.__version__)" || (
  echo ERROR: duckdb not importable in this Python. Run: py -m pip install duckdb
  exit /b 1
)

REM ------------------------------------------------------------
REM 3) Build with PyInstaller (onedir + noconsole)
REM ------------------------------------------------------------
echo.
echo Running PyInstaller...
echo.

py -m PyInstaller --noconfirm --onedir --noconsole ^
  --name "%APP_NAME%" ^
  --icon "frontend\logo.ico" ^
  --distpath "%TEMP%\aw_dist" ^
  --workpath "%TEMP%\aw_build" ^
  --paths "backend" ^
  --add-data "backend\app;app" ^
  --hidden-import "app.main" ^
  --collect-submodules "app" ^
  --hidden-import "duckdb" ^
  --collect-submodules "duckdb" ^
  --collect-data "duckdb" ^
  --collect-binaries "duckdb" ^
  --add-data "frontend;frontend" ^
  --add-data "data\datasets\demo;data\datasets\demo" ^
  "%ENTRY_SCRIPT%"


IF ERRORLEVEL 1 (
  echo.
  echo =====================================
  echo Build FAILED.
  echo =====================================
  pause
  exit /b 1
)

set "RELEASEDIR=%TEMP%\aw_dist\%APP_NAME%"

REM ------------------------------------------------------------
REM 4) Normalize deliverable layout (frontend/data next to EXE)
REM ------------------------------------------------------------
echo.
echo Post-build packaging fixes...
echo.

REM frontend: if PyInstaller put it under _internal, copy it out
if not exist "%RELEASEDIR%\frontend\index.html" (
  if exist "%RELEASEDIR%\_internal\frontend\index.html" (
    rmdir /s /q "%RELEASEDIR%\frontend" >nul 2>nul
    xcopy "%RELEASEDIR%\_internal\frontend" "%RELEASEDIR%\frontend" /E /I /Y >nul
  ) else (
    echo ERROR: frontend/index.html missing. Check repo frontend folder.
    pause
    exit /b 1
  )
)

REM data: ensure demo dataset exists next to EXE
if not exist "%RELEASEDIR%\data\datasets\demo" (
  if exist "%RELEASEDIR%\_internal\data\datasets\demo" (
    mkdir "%RELEASEDIR%\data\datasets" >nul 2>nul
    xcopy "%RELEASEDIR%\_internal\data\datasets\demo" "%RELEASEDIR%\data\datasets\demo" /E /I /Y >nul
  ) else (
    mkdir "%RELEASEDIR%\data\datasets" >nul 2>nul
    if exist "data\datasets\demo" (
      xcopy "data\datasets\demo" "%RELEASEDIR%\data\datasets\demo" /E /I /Y >nul
    )
  )
)

REM Ensure exports + logs exist
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
>> "%READMEFILE%" echo   http://127.0.0.1:8000/ui/
>> "%READMEFILE%" echo.
>> "%READMEFILE%" echo Demo dataset:
>> "%READMEFILE%" echo   data\datasets\demo\sample.parquet
>> "%READMEFILE%" echo.
>> "%READMEFILE%" echo Exports:
>> "%READMEFILE%" echo   exports\
>> "%READMEFILE%" echo.
>> "%READMEFILE%" echo Logs:
>> "%READMEFILE%" echo   logs\boot.log
>> "%READMEFILE%" echo   logs\app.log
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
