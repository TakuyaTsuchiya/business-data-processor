@echo off
title Business Data Processor - Stopping

cd /d "%~dp0"

echo ================================================
echo    Business Data Processor - Shutdown
echo ================================================
echo.

echo Stopping application...

docker-compose down

echo.
echo [OK] Application stopped successfully.
echo.
pause