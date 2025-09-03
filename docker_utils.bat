@echo off
rem ================================================
rem    Business Data Processor - Docker Utilities
rem    All Docker commands in one file
rem ================================================

cd /d "%~dp0"

:menu
cls
echo ================================================
echo    Business Data Processor v2.1.0
echo    Docker Utility Menu
echo ================================================
echo.
echo [1] Start Application
echo [2] Stop Application  
echo [3] Restart Application
echo [4] View Logs
echo [5] Create Distribution Package
echo [0] Exit
echo.
choice /c 123450 /n /m "Select option: "

if errorlevel 6 goto exit
if errorlevel 5 goto create_package
if errorlevel 4 goto logs
if errorlevel 3 goto restart
if errorlevel 2 goto stop
if errorlevel 1 goto start

:start
cls
echo ================================================
echo    Starting Application
echo ================================================
echo.
echo [1/5] Checking Docker Desktop...
docker version > nul 2>&1
if errorlevel 1 (
    echo [ERROR] Docker Desktop is not running!
    echo Please start Docker Desktop first.
    pause
    goto menu
)
echo [OK] Docker Desktop is running

echo.
echo [2/5] Checking required files...
if not exist "app.py" (
    echo [ERROR] app.py not found!
    pause
    goto menu
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
    pause
    goto menu
)

echo.
echo Waiting for application to start (30 seconds)...
timeout /t 30 /nobreak > nul

echo.
echo ================================================
echo [SUCCESS] Application started!
echo ================================================
echo.
echo Opening http://localhost:8501
start http://localhost:8501

pause
goto menu

:stop
cls
echo ================================================
echo    Stopping Application
echo ================================================
echo.
echo Stopping containers...
docker-compose down
echo.
echo [OK] Application stopped successfully.
pause
goto menu

:restart
cls
echo ================================================
echo    Restarting Application
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
    goto menu
)

echo.
echo Waiting for startup...
timeout /t 30 /nobreak > nul

echo.
echo [OK] Application restarted!
echo Opening http://localhost:8501
start http://localhost:8501

pause
goto menu

:logs
cls
echo ================================================
echo    View Logs
echo ================================================
echo.
echo [1] Live logs (Ctrl+C to stop)
echo [2] Last 50 lines
echo [3] Last 100 lines
echo [4] Error logs only
echo [0] Back to menu
echo.
choice /c 12340 /n /m "Select: "

if errorlevel 5 goto menu
if errorlevel 4 goto error_logs
if errorlevel 3 goto last_100
if errorlevel 2 goto last_50
if errorlevel 1 goto live

:live
echo.
echo === Live Logs (Press Ctrl+C to stop) ===
docker-compose logs -f
goto logs

:last_50
echo.
echo === Last 50 Lines ===
docker-compose logs --tail=50
pause
goto logs

:last_100
echo.
echo === Last 100 Lines ===
docker-compose logs --tail=100
pause
goto logs

:error_logs
echo.
echo === Error Logs ===
docker-compose logs --tail=200 | findstr /i "error exception failed"
pause
goto logs

:create_package
cls
echo ================================================
echo    Create Distribution Package
echo ================================================
echo.

set DIST_NAME=business-data-processor-docker-v2.1.0
set DIST_PATH=%cd%\%DIST_NAME%

if exist "%DIST_PATH%" (
    echo Removing existing package folder...
    rmdir /s /q "%DIST_PATH%"
)

echo [1/5] Creating package structure...
mkdir "%DIST_PATH%"
mkdir "%DIST_PATH%\processors"
mkdir "%DIST_PATH%\data"
mkdir "%DIST_PATH%\downloads"
mkdir "%DIST_PATH%\logs"
mkdir "%DIST_PATH%\docs"

echo.
echo [2/5] Copying essential files...

copy "Dockerfile" "%DIST_PATH%\" > nul
copy "docker-compose.yml" "%DIST_PATH%\" > nul
if exist ".dockerignore" copy ".dockerignore" "%DIST_PATH%\" > nul

copy "requirements.txt" "%DIST_PATH%\" > nul
copy "app.py" "%DIST_PATH%\" > nul

rem 統合版のbatファイルをコピー
copy "docker_utils.bat" "%DIST_PATH%\" > nul

copy "README.md" "%DIST_PATH%\" > nul
if exist "IMPORTANT_EXTRACT_FIRST.txt" copy "IMPORTANT_EXTRACT_FIRST.txt" "%DIST_PATH%\" > nul
if exist "Quick_Start_Guide.txt" copy "Quick_Start_Guide.txt" "%DIST_PATH%\" > nul

echo.
echo [3/5] Copying processors...
xcopy /s /e /i /q "processors" "%DIST_PATH%\processors" > nul

echo.
echo [4/5] Copying documentation...
if exist "docs" xcopy /s /e /i /q "docs" "%DIST_PATH%\docs" > nul

echo.
echo [5/5] Creating info file...
echo Business Data Processor v2.1.0 Docker Edition > "%DIST_PATH%\VERSION.txt"
echo Created: %date% %time% >> "%DIST_PATH%\VERSION.txt"
echo. >> "%DIST_PATH%\VERSION.txt"
echo Processors included: >> "%DIST_PATH%\VERSION.txt"
echo - Mirail: Autocall (6 types) + SMS (3 types) >> "%DIST_PATH%\VERSION.txt"
echo - Faith: Autocall (3 types) + SMS (3 types) >> "%DIST_PATH%\VERSION.txt"
echo - Plaza: Autocall (3 types) + SMS (3 types) >> "%DIST_PATH%\VERSION.txt"
echo - Ark: Registration + Late Payment Update >> "%DIST_PATH%\VERSION.txt"
echo - Capco: Registration + Debt Update >> "%DIST_PATH%\VERSION.txt"
echo Total: 26 processors >> "%DIST_PATH%\VERSION.txt"

echo.
echo ================================================
echo [SUCCESS] Package created successfully!
echo ================================================
echo.
echo Location: %DIST_PATH%
echo.

echo Creating ZIP file...
powershell -command "Compress-Archive -Path '%DIST_PATH%' -DestinationPath '%DIST_NAME%.zip' -Force"

if exist "%DIST_NAME%.zip" (
    echo.
    echo [OK] ZIP file created: %DIST_NAME%.zip
    powershell -command "'{0:N1} MB' -f ((Get-Item '%DIST_NAME%.zip').Length / 1MB)"
) else (
    echo [ERROR] ZIP creation failed
)

pause
goto menu

:exit
exit