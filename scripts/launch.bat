@echo off
cd /d "%~dp0.."
docker info > nul 2>&1
if errorlevel 1 (
    echo Docker is not running. Please open Docker Desktop and try again.
    pause
    exit /b 1
)

echo Building and starting cal-district-mapper...
docker compose up --build -d

echo Waiting for the app to be ready...
timeout /t 8 /nobreak > nul

start http://localhost:8000
echo.
echo Cal-district-mapper is running at http://localhost:8000
echo To stop the app, run stop.bat
