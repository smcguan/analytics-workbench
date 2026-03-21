@echo off
setlocal

echo ==========================================================
echo Building AnalyticsWorkbench...
echo ==========================================================

REM Move to repo root (script lives in /scripts)
cd /d "%~dp0\.."

echo Repo: %cd%
echo.

echo.
echo Checking for running AnalyticsWorkbench...

tasklist /FI "IMAGENAME eq AnalyticsWorkbench.exe" 2>NUL | find /I /N "AnalyticsWorkbench.exe" >NUL

if "%ERRORLEVEL%"=="0" (
    echo AnalyticsWorkbench is running. Stopping it...
    taskkill /F /IM AnalyticsWorkbench.exe >NUL 2>&1
    timeout /t 2 >NUL
    echo Process stopped.
) else (
    echo AnalyticsWorkbench is not running.
)

echo.

REM ==========================================================
REM Install backend requirements
REM ==========================================================

if not exist backend\requirements.txt (
    echo ERROR: backend\requirements.txt not found.
    pause
    exit /b 1
)

echo Installing requirements...
pip install -r backend\requirements.txt

if errorlevel 1 (
    echo ERROR: Failed installing requirements.
    pause
    exit /b 1
)

echo Installing packaging-sensitive runtime dependencies...
pip install pyarrow python-multipart

if errorlevel 1 (
    echo ERROR: Failed installing pyarrow/python-multipart.
    pause
    exit /b 1
)

echo.
echo Requirements installed successfully.
echo.

REM ==========================================================
REM Verify spec file exists
REM ==========================================================

if not exist AnalyticsWorkbench.spec (
    echo ERROR: AnalyticsWorkbench.spec not found.
    pause
    exit /b 1
)

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
echo Build successful.
echo.

REM ==========================================================
REM Copy runtime assets
REM ==========================================================

echo Copying frontend...

if exist dist\AnalyticsWorkbench\frontend (
    rmdir /S /Q dist\AnalyticsWorkbench\frontend
)

xcopy frontend dist\AnalyticsWorkbench\frontend /E /I /Y

echo Copying data...

if exist dist\AnalyticsWorkbench\data (
    rmdir /S /Q dist\AnalyticsWorkbench\data
)

if exist data (
    xcopy data dist\AnalyticsWorkbench\data /E /I /Y
)

echo Copying START_HERE.bat...

if exist START_HERE.bat (
    copy /Y START_HERE.bat dist\AnalyticsWorkbench\
)
REM ==========================================================
REM Copy environment file to release folder
REM ==========================================================

echo Copying environment configuration...

if exist .env (
    echo Using local .env
    copy /Y .env dist\AnalyticsWorkbench\.env
) else (
    echo No .env found. Copying .env.example instead.
    copy /Y .env.example dist\AnalyticsWorkbench\.env
)
echo.
echo ==========================================================
echo Build complete.
echo Release folder:
echo dist\AnalyticsWorkbench
echo ==========================================================

pause
endlocal