@echo off
title Business Data Processor - View Logs

cd /d "%~dp0"

echo ================================================
echo    Business Data Processor - Log Viewer
echo ================================================
echo.

docker version > nul 2>&1
if errorlevel 1 (
    echo [ERROR] Docker Desktop is not running
    pause
    exit /b 1
)

echo Select log option:
echo.
echo [1] Live logs (Ctrl+C to stop)
echo [2] Last 50 lines
echo [3] Last 100 lines
echo [4] Error logs only
echo [0] Exit
echo.
choice /c 12340 /n /m "Select: "

if errorlevel 5 goto end
if errorlevel 4 goto error_logs
if errorlevel 3 goto last_100
if errorlevel 2 goto last_50
if errorlevel 1 goto live

:live
echo.
echo === Live Logs (Ctrl+C to stop) ===
docker-compose logs -f
goto end

:last_50
echo.
echo === Last 50 Lines ===
docker-compose logs --tail=50
pause
goto end

:last_100
echo.
echo === Last 100 Lines ===
docker-compose logs --tail=100
pause
goto end

:error_logs
echo.
echo === Error Logs ===
docker-compose logs --tail=200 | findstr /i "error exception failed"
pause

:end