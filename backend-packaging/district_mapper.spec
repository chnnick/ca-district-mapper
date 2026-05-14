# -*- mode: python ; coding: utf-8 -*-
"""PyInstaller spec for the FastAPI sidecar shipped inside the Tauri app.

Run from the repo root:

    pyinstaller --noconfirm backend-packaging/district_mapper.spec

The resulting binary lives at dist/district-mapper-backend (or .exe on
Windows). The Tauri build script renames it to include the Rust target
triple Tauri expects under src-tauri/binaries/.
"""

from pathlib import Path

from PyInstaller.utils.hooks import collect_submodules

PROJECT_ROOT = Path(SPECPATH).parent

datas = [
    (str(PROJECT_ROOT / "db" / "migrations"), "db/migrations"),
    (str(PROJECT_ROOT / "config" / "bef_sources.yaml"), "config"),
]

hiddenimports = (
    collect_submodules("uvicorn")
    + collect_submodules("uvicorn.protocols")
    + collect_submodules("uvicorn.lifespan")
    + collect_submodules("uvicorn.loops")
    + [
        "uvicorn.logging",
        "anyio._backends._asyncio",
    ]
)

a = Analysis(
    [str(PROJECT_ROOT / "src" / "api" / "sidecar.py")],
    pathex=[str(PROJECT_ROOT)],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    runtime_hooks=[],
    excludes=["tkinter", "matplotlib", "PIL", "numpy", "pandas"],
    noarchive=False,
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    name="district-mapper-backend",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
