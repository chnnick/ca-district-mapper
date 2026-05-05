#!/bin/bash
set -e

if ! docker info > /dev/null 2>&1; then
    echo "Docker is not running. Please open Docker Desktop and try again."
    exit 1
fi

echo "Building and starting cal-district-mapper..."
docker compose up --build -d

echo "Waiting for the app to be ready..."
for i in $(seq 1 30); do
    if curl -sf http://localhost:8000/ > /dev/null 2>&1; then
        break
    fi
    sleep 1
done

echo "Opening browser..."
open http://localhost:8000
echo ""
echo "Cal-district-mapper is running at http://localhost:8000"
echo "To stop the app, run: ./stop.sh"
