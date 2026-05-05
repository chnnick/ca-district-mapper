#!/bin/bash
set -e
cd "$(dirname "$0")/.."

echo "This will stop the app and delete all data EXCEPT BEF files."
read -p "Are you sure? (y/N): " confirm
if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
    echo "Aborted."
    exit 0
fi

echo "Stopping app..."
docker compose down

echo "Removing database..."
rm -f data/district_mapper.db

echo "Clearing raw and processed data..."
find data/raw -type f ! -name '.gitkeep' -delete
find data/processed -type f ! -name '.gitkeep' -delete

echo "Clearing logs..."
find logs -type f ! -name '.gitkeep' -delete 2>/dev/null || true

echo "Clearing reports..."
find reports -type f ! -name '.gitkeep' -delete 2>/dev/null || true

echo "Done. BEF files preserved. Run ./launch.sh to start fresh."
