@echo off
setlocal EnableExtensions

echo =========================================
echo Analytics Workbench - Environment Doctor
echo =========================================
echo.

echo.
echo Running in directory:
echo %cd%
echo.

echo Checking Python...
where python
python --version
echo.

echo Checking pip...
python -m pip --version
echo.

echo Checking virtual environment...
if exist ".venv\Scripts\python.exe" (
    echo VENV FOUND
    ".venv\Scripts\python.exe" --version
) else (
    echo VENV NOT FOUND
)
echo.

echo Checking key packages...
if exist ".venv\Scripts\python.exe" (
    ".venv\Scripts\python.exe" -m pip show fastapi >nul 2>&1 && echo fastapi OK || echo fastapi MISSING
    ".venv\Scripts\python.exe" -m pip show uvicorn >nul 2>&1 && echo uvicorn OK || echo uvicorn MISSING
    ".venv\Scripts\python.exe" -m pip show duckdb >nul 2>&1 && echo duckdb OK || echo duckdb MISSING
)
echo.

echo Checking Git branch...
git branch -vv
echo.

echo Checking working tree...
git status
echo.

echo Checking for large tracked artifacts...
git ls-files | findstr /i ".parquet .zip .exe .db .duckdb"
echo.

echo =========================================
echo Doctor check complete.
echo =========================================
pause