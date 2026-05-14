# Build the FastAPI sidecar with PyInstaller and place it under
# src-tauri/binaries/ with the Rust target-triple suffix Tauri expects.
#
# Usage (from repo root):
#   powershell -ExecutionPolicy Bypass -File backend-packaging\build_sidecar.ps1

$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

if (-not (Get-Command pyinstaller -ErrorAction SilentlyContinue)) {
    Write-Error "pyinstaller not found. Install with: pip install pyinstaller"
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
