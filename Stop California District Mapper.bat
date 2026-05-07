@echo off
cd /d "%~dp0"
echo Stopping California District Mapper...
docker compose down
echo Stopped.
pause
