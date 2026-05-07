@echo off
cd /d "%~dp0"
echo California District Mapper
echo ------------------------------------------

docker info > nul 2>&1
if errorlevel 1 (
    echo.
    echo ERROR: Docker is not running.
    echo.
    echo Please:
    echo   1. Open Docker Desktop from your Start Menu
    echo   2. Wait for it to finish starting ^(the whale icon in your taskbar stops animating^)
    echo   3. Double-click this launcher again
    echo.
    echo Don't have Docker Desktop? Download it at:
    echo   https://www.docker.com/products/docker-desktop/
    echo.
    pause
    exit /b 1
)

echo Pulling latest app image...
docker compose pull --quiet

echo Starting app...
docker compose up -d

echo.
echo Waiting for the app to be ready...
set /a attempts=0
:waitloop
set /a attempts+=1
curl -sf http://localhost:8000/ > nul 2>&1
if not errorlevel 1 goto ready
if %attempts% GEQ 60 goto timeout
timeout /t 1 /nobreak > nul
goto waitloop

:timeout
echo App is taking longer than expected. Try opening http://localhost:8000 manually.
goto open

:ready
echo.

:open
echo Opening browser...
start http://localhost:8000
echo.
echo California District Mapper is running at http://localhost:8000
echo.
echo To stop the app, double-click "Stop California District Mapper.bat"
echo.
pause
