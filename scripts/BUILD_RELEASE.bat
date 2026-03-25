@echo off
setlocal

echo ==========================================================
echo Building AnalyticsWorkbench...
echo ==========================================================

REM Move to repo root (script lives in /scripts)
cd /d "%~dp0\.."

echo Repo: %cd%
echo.

REM ==========================================================
REM Kill all processes that could hold file locks
REM ==========================================================

echo Killing any running AW or Python processes...
taskkill /F /IM AnalyticsWorkbench.exe 2>NUL
taskkill /F /IM python.exe 2>NUL
timeout /t 2 /nobreak >NUL
echo Processes cleared.
echo.

REM ==========================================================
REM Clean build state — prevents stale file corruption
REM ==========================================================

echo Cleaning previous build artifacts...

if exist dist (
    rmdir /S /Q dist
    echo dist\ removed.
)

if exist build (
    rmdir /S /Q build
    echo build\ removed.
)

echo Clean state confirmed.
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
pyinstaller AnalyticsWorkbench.spec --noconfirm

if errorlevel 1 (
    echo ERROR: PyInstaller build failed.
    pause
    exit /b 1
)

echo.
echo Build successful.
echo.

REM ==========================================================
REM Verify critical DLLs in build output
REM ==========================================================

echo Verifying build output...

FOR /F "tokens=*" %%i IN ('python -c "import sys; print(f'python{sys.version_info.major}{sys.version_info.minor}.dll')"') DO SET PYTHON_DLL=%%i
IF NOT EXIST "dist\AnalyticsWorkbench\_internal\%PYTHON_DLL%" (
    echo.
    echo !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    echo ERROR: %PYTHON_DLL% MISSING from build output
    echo Build failed — do not distribute
    echo !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    pause
    exit /b 1
)
echo   %PYTHON_DLL% .... OK

if not exist "dist\AnalyticsWorkbench\_internal\python3.dll" (
    echo.
    echo ERROR: python3.dll MISSING from build output
    pause
    exit /b 1
)
echo   python3.dll ...... OK

if not exist "dist\AnalyticsWorkbench\AnalyticsWorkbench.exe" (
    echo.
    echo ERROR: AnalyticsWorkbench.exe MISSING from build output
    pause
    exit /b 1
)
echo   AnalyticsWorkbench.exe .... OK

echo All critical files verified.
echo.

REM ==========================================================
REM Copy runtime assets
REM ==========================================================

echo Copying frontend...
xcopy frontend dist\AnalyticsWorkbench\frontend /E /I /Y

echo Copying data (example cases + reference library only)...

REM Create empty runtime directories the app expects
mkdir dist\AnalyticsWorkbench\data\datasets
mkdir dist\AnalyticsWorkbench\data\references
mkdir dist\AnalyticsWorkbench\data\sessions

REM Copy example cases (tutorials + sample data)
if exist data\example_cases (
    xcopy data\example_cases dist\AnalyticsWorkbench\data\example_cases /E /I /Y
    echo Example cases copied.
)

REM Copy reference library (pre-built CSVs)
if exist data\reference_library (
    xcopy data\reference_library dist\AnalyticsWorkbench\data\reference_library /E /I /Y
    echo Reference library copied.
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
echo BUILD COMPLETE — VERIFIED
echo Release folder: dist\AnalyticsWorkbench
echo ==========================================================

pause
endlocal
