#!/bin/bash
cd "$(dirname "$0")"
echo "Stopping California District Mapper..."
docker compose down
echo "Stopped."
read -rp "Press Enter to close..."
