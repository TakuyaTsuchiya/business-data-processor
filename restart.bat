@echo off
title Business Data Processor - Restarting

cd /d "%~dp0"

echo ================================================
echo    Business Data Processor - Restart
echo ================================================
echo.

echo [1/2] Stopping application...
docker-compose down
timeout /t 2 /nobreak > nul

echo.
echo [2/2] Starting application...
docker-compose up -d --build

if errorlevel 1 (
    echo.
    echo [ERROR] Failed to restart
    pause
    exit /b 1
)

echo.
echo Waiting for startup...
timeout /t 30 /nobreak > nul

echo.
echo [OK] Application restarted!
echo.
echo Opening http://localhost:8501
start http://localhost:8501

pause