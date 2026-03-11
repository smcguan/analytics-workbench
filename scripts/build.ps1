# build.ps1 - Analytics Workbench release builder (no Docker)
# Usage:
#   Set-ExecutionPolicy -Scope Process Bypass
#   .\build.ps1
#
# Produces:
#   release\AnalyticsWorkbench-v<version>.zip
#   release\AnalyticsWorkbench-v<version>\  (staging folder)

$ErrorActionPreference = "Stop"

# ----------------------------
# Config
# ----------------------------
$AppName   = "AnalyticsWorkbench"
$Root      = $PSScriptRoot
$Backend   = Join-Path $Root "backend"
$Frontend  = Join-Path $Root "frontend"
$DataDir   = Join-Path $Root "data"
$ReleaseRoot = Join-Path $Root "release"

# Optional version env var support (falls back to 0.1.0)
$Version = $env:APP_VERSION
if ([string]::IsNullOrWhiteSpace($Version)) { $Version = "0.1.0" }

# Build/Stage paths
$BuildDir  = Join-Path $Root "build"
$DistDir   = Join-Path $Root "dist"
$StageDir  = Join-Path $ReleaseRoot "$AppName-v$Version"
$ZipPath   = Join-Path $ReleaseRoot "$AppName-v$Version.zip"

Write-Host "=== Analytics Workbench Build ==="
Write-Host "Root:     $Root"
Write-Host "Version:  $Version"
Write-Host "StageDir: $StageDir"
Write-Host "Zip:      $ZipPath"
Write-Host ""

# ----------------------------
# Pre-flight checks
# ----------------------------
if (-not (Test-Path $Backend))  { throw "Missing folder: backend\" }
if (-not (Test-Path $Frontend)) { throw "Missing folder: frontend\" }
if (-not (Test-Path $DataDir))  { Write-Warning "Missing folder: data\ (will still build, but you won't ship demo data)" }

$MainPy = Join-Path $Backend "main.py"
if (-not (Test-Path $MainPy)) { throw "Missing backend\main.py at: $MainPy" }

# Ensure release root exists
New-Item -ItemType Directory -Force -Path $ReleaseRoot | Out-Null

# ----------------------------
# Clean build artifacts
# ----------------------------
Write-Host "Cleaning prior artifacts..."
if (Test-Path $BuildDir) { Remove-Item -Recurse -Force $BuildDir }
if (Test-Path $DistDir)  { Remove-Item -Recurse -Force $DistDir }
if (Test-Path $StageDir) { Remove-Item -Recurse -Force $StageDir }
if (Test-Path $ZipPath)  { Remove-Item -Force $ZipPath }

# ----------------------------
# Build EXE (PyInstaller)
# ----------------------------
Write-Host "Building EXE with PyInstaller..."
Push-Location $Backend
try {
  # Use python -m to avoid PATH issues ("pyinstaller not recognized")
  python -m PyInstaller `
    --noconfirm `
    --clean `
    --onedir `
    --name $AppName `
    main.py
}
finally {
  Pop-Location
}

# Confirm PyInstaller output exists
$BuiltAppDir = Join-Path $DistDir $AppName
if (-not (Test-Path $BuiltAppDir)) {
  throw "PyInstaller did not produce expected folder: $BuiltAppDir"
}

# ----------------------------
# Stage release folder
# ----------------------------
Write-Host "Staging release folder..."
New-Item -ItemType Directory -Force -Path $StageDir | Out-Null

# Copy PyInstaller output contents into stage root
Copy-Item -Path (Join-Path $BuiltAppDir "*") -Destination $StageDir -Recurse -Force

# Copy frontend into stage root
Copy-Item -Path $Frontend -Destination (Join-Path $StageDir "frontend") -Recurse -Force

# Copy data into stage root (demo parquet + README.txt, etc.)
if (Test-Path $DataDir) {
  Copy-Item -Path $DataDir -Destination (Join-Path $StageDir "data") -Recurse -Force
}

# Ensure exports folder exists
New-Item -ItemType Directory -Force -Path (Join-Path $StageDir "exports") | Out-Null

# ----------------------------
# Create run.bat in stage root
# ----------------------------
$RunBat = Join-Path $StageDir "run.bat"
@"
@echo off
setlocal
cd /d "%~dp0"
start "" http://127.0.0.1:8000/ui/
"%~dp0$AppName.exe"
endlocal
"@ | Out-File -Encoding ascii $RunBat

# ----------------------------
# Create README.md in stage root
# ----------------------------
$Readme = Join-Path $StageDir "README.md"
@"
# Analytics Workbench v$Version

## Start
1. Double-click **run.bat** (recommended), or run **$AppName.exe**
2. Your browser should open to: http://127.0.0.1:8000/ui/

## Add your data (Parquet)
Drop Parquet files into:

  data\\datasets\\<dataset_name>\\*.parquet

Example:

  data\\datasets\\doge\\BIG.parquet

Refresh the UI and the dataset will appear in the **Dataset** dropdown.

## Demo dataset
A small sample dataset may be included at:

  data\\datasets\\demo\\sample.parquet

## Exports
Excel exports appear in:

  exports\\

## Notes
- This is a local, single-machine demo app.
- If port 8000 is already in use, close the other app using it and try again.
"@ | Out-File -Encoding utf8 $Readme

# ----------------------------
# Zip it
# ----------------------------
Write-Host "Creating zip package..."
Compress-Archive -Path $StageDir -DestinationPath $ZipPath -Force

Write-Host ""
Write-Host "✅ Build complete!"
Write-Host "Stage folder: $StageDir"
Write-Host "Zip package:  $ZipPath"
