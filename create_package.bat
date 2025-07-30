@echo off
title Business Data Processor - Create Distribution Package

echo ================================================
echo    Business Data Processor v2.1.0
echo    Distribution Package Creator
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

copy "startup.bat" "%DIST_PATH%\" > nul
copy "shutdown.bat" "%DIST_PATH%\" > nul
copy "restart.bat" "%DIST_PATH%\" > nul
copy "logs.bat" "%DIST_PATH%\" > nul

copy "README.md" "%DIST_PATH%\" > nul
copy "IMPORTANT_EXTRACT_FIRST.txt" "%DIST_PATH%\" > nul
copy "Quick_Start_Guide.txt" "%DIST_PATH%\" > nul

echo.
echo [3/5] Copying processors...
xcopy /s /e /i /q "processors" "%DIST_PATH%\processors" > nul

echo.
echo [4/5] Copying documentation...
if exist "docs\Docker_Desktop_インストールガイド.md" (
    copy "docs\Docker_Desktop_インストールガイド.md" "%DIST_PATH%\docs\" > nul
)
if exist "docs\トラブルシューティング.md" (
    copy "docs\トラブルシューティング.md" "%DIST_PATH%\docs\" > nul
)
if exist "docs\Docker基礎知識.md" (
    copy "docs\Docker基礎知識.md" "%DIST_PATH%\docs\" > nul
)

echo.
echo [5/5] Creating info file...
echo Business Data Processor v2.1.0 Docker Edition > "%DIST_PATH%\VERSION.txt"
echo Created: %date% %time% >> "%DIST_PATH%\VERSION.txt"
echo. >> "%DIST_PATH%\VERSION.txt"
echo Processors included: >> "%DIST_PATH%\VERSION.txt"
echo - Mirail: 6 types >> "%DIST_PATH%\VERSION.txt"
echo - Faith: 3 types >> "%DIST_PATH%\VERSION.txt"
echo - Plaza: 3 types >> "%DIST_PATH%\VERSION.txt"
echo - Ark: 1 type >> "%DIST_PATH%\VERSION.txt"
echo Total: 15 processors >> "%DIST_PATH%\VERSION.txt"

echo.
echo ================================================
echo [SUCCESS] Package created successfully!
echo ================================================
echo.
echo Location: %DIST_PATH%
echo.
echo Next steps:
echo 1. ZIP the %DIST_NAME% folder
echo 2. Share the ZIP file
echo 3. Extract and run startup.bat
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

echo.
pause