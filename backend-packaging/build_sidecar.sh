#!/usr/bin/env bash
# Build the FastAPI sidecar with PyInstaller and place it under
# src-tauri/binaries/ with the Rust target-triple suffix Tauri expects.
#
# Usage:
#   backend-packaging/build_sidecar.sh
#
# Run from the repo root. Requires pyinstaller in the active venv.

set -euo pipefail

cd "$(dirname "$0")/.."

# Prefer the project venv so PyInstaller analyzes the same dependency tree
# the app actually uses. Falls through to the active environment when no
# .venv exists (e.g. CI installs deps into the runner's Python directly).
if [[ -z "${VIRTUAL_ENV:-}" && -x ".venv/bin/python" ]]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate
fi

if ! command -v pyinstaller >/dev/null 2>&1; then
  echo "pyinstaller not found. Install with: pip install -r requirements-build.txt" >&2
  exit 1
fi

# Hard-fail if uvicorn isn't importable here — otherwise PyInstaller silently
# produces a binary that crashes with ModuleNotFoundError at runtime.
if ! python -c "import uvicorn, fastapi" >/dev/null 2>&1; then
  echo "uvicorn/fastapi not importable in $(which python)." >&2
  echo "Activate your venv and run: pip install -r requirements.txt" >&2
  exit 1
fi

TRIPLE="${TARGET_TRIPLE:-}"
if [[ -z "${TRIPLE}" ]]; then
  if command -v rustc >/dev/null 2>&1; then
    TRIPLE="$(rustc -vV | sed -n 's/host: //p')"
  fi
fi
if [[ -z "${TRIPLE}" ]]; then
  echo "Set TARGET_TRIPLE or install rustc to determine the Tauri triple." >&2
  echo "Examples: aarch64-apple-darwin, x86_64-apple-darwin, x86_64-pc-windows-msvc" >&2
  exit 1
fi

pyinstaller --noconfirm --distpath dist --workpath build/pyinstaller \
  backend-packaging/district_mapper.spec

mkdir -p src-tauri/binaries
SRC="dist/district-mapper-backend"
DST="src-tauri/binaries/district-mapper-backend-${TRIPLE}"
if [[ -f "${SRC}.exe" ]]; then
  cp "${SRC}.exe" "${DST}.exe"
  echo "Wrote ${DST}.exe"
else
  cp "${SRC}" "${DST}"
  chmod +x "${DST}"
  echo "Wrote ${DST}"
fi
