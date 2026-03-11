@echo off
setlocal

REM Start Analytics Workbench (packaged EXE)
start "" "%~dp0AnalyticsWorkbench.exe"

REM Give it a moment to boot
timeout /t 2 /nobreak >nul

REM Open the UI
start "" http://localhost:8000/ui/

endlocal
