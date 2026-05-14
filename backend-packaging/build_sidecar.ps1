# Build the FastAPI sidecar with PyInstaller and place it under
# src-tauri/binaries/ with the Rust target-triple suffix Tauri expects.
#
# Usage (from repo root):
#   powershell -ExecutionPolicy Bypass -File backend-packaging\build_sidecar.ps1

$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

# Prefer the project venv when no environment is already active.
if (-not $env:VIRTUAL_ENV -and (Test-Path ".venv\Scripts\Activate.ps1")) {
    & ".venv\Scripts\Activate.ps1"
}

if (-not (Get-Command pyinstaller -ErrorAction SilentlyContinue)) {
    Write-Error "pyinstaller not found. Install with: pip install -r requirements-build.txt"
}

# Hard-fail if uvicorn isn't importable here.
& python -c "import uvicorn, fastapi" 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Error "uvicorn/fastapi not importable. Activate your venv and run: pip install -r requirements.txt"
}

$triple = (& rustc -vV | Select-String '^host:').ToString() -replace '^host:\s*', ''
if (-not $triple) { Write-Error "rustc not found; cannot determine target triple" }

pyinstaller --noconfirm --distpath dist --workpath build/pyinstaller `
    backend-packaging/district_mapper.spec

New-Item -ItemType Directory -Force -Path "src-tauri/binaries" | Out-Null
$src = "dist/district-mapper-backend.exe"
$dst = "src-tauri/binaries/district-mapper-backend-$triple.exe"
Copy-Item -Force $src $dst
Write-Host "Wrote $dst"
