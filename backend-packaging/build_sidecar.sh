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

if ! command -v pyinstaller >/dev/null 2>&1; then
  echo "pyinstaller not found. Install with: pip install pyinstaller" >&2
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
