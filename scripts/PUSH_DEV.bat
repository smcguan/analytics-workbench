@echo off
cd /d %~dp0

echo Running in directory:
cd

echo.
echo Pulling latest from origin/dev...
git pull origin dev

echo.
echo Current status:
git status

echo.
echo About to stage ALL changes (git add -A).
set /p confirm=Proceed? [Y/N]:

if /I not "%confirm%"=="Y" exit /b

echo.
echo Staging changes...
git add -A

echo.
set /p msg=Enter commit message:

if "%msg%"=="" (
echo ERROR: Commit message cannot be empty.
pause
exit /b
)

echo.
echo Committing...
git commit -m "%msg%"

echo.
echo Pushing to origin/dev...
git push origin dev

echo.
echo Done.
pause
