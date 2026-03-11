Write-Host ""
Write-Host "=== Analytics Workbench Bootstrap ==="
Write-Host ""

# -------------------------------------
# Helper: Yes / No Prompt
# -------------------------------------

function Ask-YesNo {
    param(
        [string]$Prompt,
        [bool]$DefaultYes = $true
    )

    while ($true) {

        if ($DefaultYes) { $suffix = " [Y/n]" }
        else { $suffix = " [y/N]" }

        $answer = Read-Host "$Prompt$suffix"

        if ([string]::IsNullOrWhiteSpace($answer)) {
            return $DefaultYes
        }

        switch ($answer.Trim().ToLower()) {
            "y" { return $true }
            "yes" { return $true }
            "n" { return $false }
            "no" { return $false }
            default { Write-Host "Please enter Y or N." }
        }
    }
}

# -------------------------------------
# Move to project root
# -------------------------------------

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Resolve-Path (Join-Path $scriptDir "..")

Set-Location $projectRoot

Write-Host "Project root: $projectRoot"
Write-Host ""

# -------------------------------------
# Check Python
# -------------------------------------

try {
    $pythonVersion = python --version
    Write-Host "Detected Python: $pythonVersion"
}
catch {
    Write-Host "ERROR: Python not installed or not on PATH."
    exit 1
}

Write-Host ""

# -------------------------------------
# Create virtual environment
# -------------------------------------

if (!(Test-Path ".venv")) {

    if (Ask-YesNo "Create Python virtual environment (.venv)?") {

        python -m venv .venv

        if ($LASTEXITCODE -ne 0) {
            Write-Host "ERROR creating venv"
            exit 1
        }
    }
}
else {

    Write-Host ".venv already exists."

    if (Ask-YesNo "Recreate .venv from scratch?" $false) {

        Remove-Item ".venv" -Recurse -Force
        python -m venv .venv
    }
}

Write-Host ""

# -------------------------------------
# Activate environment
# -------------------------------------

Write-Host "Activating virtual environment..."

& .\.venv\Scripts\Activate.ps1

if ($LASTEXITCODE -ne 0) {

    Write-Host "PowerShell blocked script execution."
    Write-Host "Run this once and try again:"
    Write-Host ""
    Write-Host "Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass"
    exit 1
}

Write-Host ""

# -------------------------------------
# Upgrade pip
# -------------------------------------

if (Ask-YesNo "Upgrade pip?") {

    python -m pip install --upgrade pip
}

Write-Host ""

# -------------------------------------
# Install dependencies
# -------------------------------------

if (Test-Path "requirements.txt") {

    if (Ask-YesNo "Install dependencies?") {

        pip install -r backend\requirements.txt

        if ($LASTEXITCODE -ne 0) {
            Write-Host "Dependency install failed."
            exit 1
        }
    }
}

Write-Host ""

# -------------------------------------
# .env setup
# -------------------------------------

function Set-EnvValue {

    param(
        [string]$FilePath,
        [string]$Key,
        [string]$Value
    )

    $content = Get-Content $FilePath -Raw -ErrorAction SilentlyContinue

    if ($null -eq $content) { $content = "" }

    $pattern = "(?m)^$Key=.*$"

    if ($content -match $pattern) {
        $content = [regex]::Replace($content,$pattern,"$Key=$Value")
    }
    else {
        $content += "`n$Key=$Value"
    }

    Set-Content $FilePath $content
}

# create .env

if (!(Test-Path ".env")) {

    Write-Host ".env not found."

    if (Test-Path ".env.example") {

        Copy-Item ".env.example" ".env"

        Write-Host ".env created from .env.example"
    }
    else {

        New-Item ".env" -ItemType File | Out-Null

        Write-Host ".env created."
    }
}
else {

    Write-Host ".env already exists."
}

Write-Host ""

# -------------------------------------
# Prompt for OpenAI key
# -------------------------------------

$envContent = Get-Content ".env" -Raw

if ($envContent -match "OPENAI_API_KEY=your_openai_key_here") {

    if (Ask-YesNo "Enter your OpenAI API key now?") {

        $key = Read-Host "Paste your OpenAI API key"

        if (![string]::IsNullOrWhiteSpace($key)) {

            Set-EnvValue ".env" "OPENAI_API_KEY" $key

            Write-Host "OpenAI key saved to local .env"
        }
    }
}

Write-Host ""

# -------------------------------------
# Optional build
# -------------------------------------

if (Test-Path "BUILD_RELEASE.bat") {

    if (Ask-YesNo "Run release build? (PyInstaller)" $false) {

        cmd /c BUILD_RELEASE.bat
    }
}

Write-Host ""

# -------------------------------------
# Optional start
# -------------------------------------

if (Ask-YesNo "Start backend server now?") {

    python backend/app/main.py
}

Write-Host ""
Write-Host "Bootstrap complete."
Write-Host ""
Write-Host "Next time you can simply run:"
Write-Host ""
Write-Host ".\.venv\Scripts\activate"
Write-Host "python backend/app/main.py"
Write-Host ""