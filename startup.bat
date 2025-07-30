@echo off
title Business Data Processor - Starting

cd /d "%~dp0"

echo ================================================
echo    Business Data Processor v2.1.0 - Docker
echo ================================================
echo.

echo [1/5] Checking Docker Desktop...
docker version > nul 2>&1
if errorlevel 1 (
    echo [ERROR] Docker Desktop is not running!
    echo Please start Docker Desktop first.
    pause
    exit /b 1
)
echo [OK] Docker Desktop is running

echo.
echo [2/5] Checking required files...
if not exist "app.py" (
    echo [ERROR] app.py not found!
    echo Please extract ZIP file completely before running.
    pause
    exit /b 1
)
echo [OK] Required files found

echo.
echo [3/5] Creating data folders...
if not exist "data" mkdir data
if not exist "downloads" mkdir downloads
if not exist "logs" mkdir logs
echo [OK] Folders ready

echo.
echo [4/5] Cleaning up old containers...
docker-compose down > nul 2>&1
echo [OK] Cleanup completed

echo.
echo [5/5] Starting application...
echo This may take 2-5 minutes on first run...
echo.

docker-compose up -d --build

if errorlevel 1 (
    echo.
    echo [ERROR] Failed to start application
    echo Please check the error messages above.
    pause
    exit /b 1
)

echo.
echo Waiting for application to start (30 seconds)...
timeout /t 30 /nobreak > nul

echo.
echo ================================================
echo [SUCCESS] Application should be running!
echo ================================================
echo.
echo Open your browser and go to:
echo http://localhost:8501
echo.
echo Opening browser automatically...
start http://localhost:8501

echo.
echo To stop the application, run: shutdown.bat
echo.
pause