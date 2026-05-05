@echo off
cd /d "%~dp0.."
echo This will stop the app and delete all data EXCEPT BEF files.
set /p confirm="Are you sure? (y/N): "
if /i not "%confirm%"=="y" (
    echo Aborted.
    pause
    exit /b 0
)

echo Stopping app...
docker compose down

echo Removing database...
if exist data\district_mapper.db del /f data\district_mapper.db

echo Clearing raw and processed data...
for /f "delims=" %%f in ('dir /b /a-d data\raw\ 2^>nul') do (
    if /i not "%%f"==".gitkeep" del /f "data\raw\%%f"
)
for /f "delims=" %%f in ('dir /b /a-d data\processed\ 2^>nul') do (
    if /i not "%%f"==".gitkeep" del /f "data\processed\%%f"
)

echo Clearing logs...
for /f "delims=" %%f in ('dir /b /a-d logs\ 2^>nul') do (
    if /i not "%%f"==".gitkeep" del /f "logs\%%f"
)

echo Clearing reports...
for /f "delims=" %%f in ('dir /b /a-d reports\ 2^>nul') do (
    if /i not "%%f"==".gitkeep" del /f "reports\%%f"
)

echo Done. BEF files preserved. Run launch.bat to start fresh.
pause
