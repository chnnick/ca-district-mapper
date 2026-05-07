#!/bin/bash
cd "$(dirname "$0")"

echo "California District Mapper"
echo "──────────────────────────────────────"

if ! docker info > /dev/null 2>&1; then
    echo ""
    echo "ERROR: Docker is not running."
    echo ""
    echo "Please:"
    echo "  1. Open Docker Desktop from your Applications folder"
    echo "  2. Wait for it to finish starting (the whale icon in your menu bar stops animating)"
    echo "  3. Double-click this launcher again"
    echo ""
    echo "Don't have Docker Desktop? Download it at:"
    echo "  https://www.docker.com/products/docker-desktop/"
    echo ""
    read -rp "Press Enter to close..."
    exit 1
fi

echo "Pulling latest app image..."
docker compose pull --quiet

echo "Starting app..."
docker compose up -d

echo ""
echo "Waiting for the app to be ready..."
for i in $(seq 1 60); do
    if curl -sf http://localhost:8000/ > /dev/null 2>&1; then
        break
    fi
    if [ "$i" -eq 60 ]; then
        echo "App is taking longer than expected. Try opening http://localhost:8000 manually."
        break
    fi
    sleep 1
done

echo "Opening browser..."
open http://localhost:8000
echo ""
echo "California District Mapper is running at http://localhost:8000"
echo ""
echo "To stop the app, double-click 'Stop California District Mapper.command'"
echo ""
read -rp "Press Enter to close this window..."
