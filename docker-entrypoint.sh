#!/bin/sh
set -e
mkdir -p /app/data/raw /app/data/bef /app/logs /app/reports

# First-run: auto-load BEF data if the database has no loaded versions.
python3 -c "
import sqlite3, os, subprocess, sys
db = '/app/data/district_mapper.db'
needs_load = True
if os.path.exists(db):
    try:
        conn = sqlite3.connect(db)
        count = conn.execute('SELECT COUNT(*) FROM bef_versions').fetchone()[0]
        conn.close()
        needs_load = count == 0
    except Exception:
        pass
if needs_load:
    print('First run: loading BEF district data (this may take a minute)...', flush=True)
    result = subprocess.run([sys.executable, 'scripts/load_bef.py', '--approved-by', 'auto-load'])
    if result.returncode != 0:
        print('Warning: BEF load completed with errors. District lookups may not work until resolved.', file=sys.stderr, flush=True)
" || true

exec uvicorn src.api.app:create_app --factory --host 0.0.0.0 --port 8000
