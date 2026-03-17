@echo off
REM ============================================================
REM  Analytics Workbench — Dev Session Launcher
REM  Double-click this file to start a full dev session:
REM    Window 1: FastAPI backend server
REM    Window 2: Claude Code in the project directory
REM ============================================================

set PROJECT=C:\dev\analytics-workbench


REM --- Start Claude Code in its own window ---
start "Claude Code" cmd /k "cd /d %PROJECT% && claude"
