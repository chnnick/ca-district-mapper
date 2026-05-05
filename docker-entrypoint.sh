#!/bin/sh
set -e
mkdir -p /app/data/raw /app/data/bef /app/logs /app/reports
exec uvicorn src.api.app:create_app --factory --host 0.0.0.0 --port 8000
