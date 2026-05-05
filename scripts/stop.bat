@echo off
cd /d "%~dp0.."
docker compose down
echo Cal-district-mapper stopped.
pause
