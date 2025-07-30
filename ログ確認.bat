@echo off
chcp 65001 > nul
title Business Data Processor - ログ確認

:: 実行ディレクトリをバッチファイルの場所に設定
cd /d "%~dp0"

echo ================================================
echo    Business Data Processor ログ確認ツール
echo ================================================
echo.

:: Docker確認
docker version > nul 2>&1
if errorlevel 1 (
    echo ❌ エラー: Docker Desktop が起動していません
    echo.
    pause
    exit /b 1
)

:: コンテナ状態確認
echo アプリケーションの状態を確認しています...
docker ps | findstr business-data-processor > nul 2>&1
if errorlevel 1 (
    echo.
    echo ⚠️ 警告: アプリケーションが起動していません
    echo.
    echo 過去のログを表示しますか？ (Y/N)
    choice /c YN /n
    if errorlevel 2 exit /b 0
    if errorlevel 1 goto show_all_logs
)

:menu
echo.
echo ログ表示オプションを選択してください:
echo.
echo [1] リアルタイムログ（最新20行から開始）
echo [2] 最新50行のログ
echo [3] 最新100行のログ
echo [4] エラーログのみ表示
echo [5] 本日のログすべて
echo [0] 終了
echo.
choice /c 123450 /n /m "選択してください: "

if errorlevel 6 goto end
if errorlevel 5 goto today_logs
if errorlevel 4 goto error_logs
if errorlevel 3 goto last_100
if errorlevel 2 goto last_50
if errorlevel 1 goto realtime

:realtime
echo.
echo === リアルタイムログ表示 (Ctrl+C で終了) ===
echo.
if exist "docker-compose.tmp.yml" (
    docker-compose -f docker-compose.tmp.yml logs --tail=20 -f
) else (
    docker-compose logs --tail=20 -f
)
goto menu

:last_50
echo.
echo === 最新50行のログ ===
echo.
if exist "docker-compose.tmp.yml" (
    docker-compose -f docker-compose.tmp.yml logs --tail=50
) else (
    docker-compose logs --tail=50
)
echo.
pause
goto menu

:last_100
echo.
echo === 最新100行のログ ===
echo.
if exist "docker-compose.tmp.yml" (
    docker-compose -f docker-compose.tmp.yml logs --tail=100
) else (
    docker-compose logs --tail=100
)
echo.
pause
goto menu

:error_logs
echo.
echo === エラーログのみ表示 ===
echo.
if exist "docker-compose.tmp.yml" (
    docker-compose -f docker-compose.tmp.yml logs --tail=200 | findstr /i "error exception failed critical"
) else (
    docker-compose logs --tail=200 | findstr /i "error exception failed critical"
)
echo.
pause
goto menu

:today_logs
echo.
echo === 本日のログすべて ===
echo.
if exist "docker-compose.tmp.yml" (
    docker-compose -f docker-compose.tmp.yml logs --since 24h
) else (
    docker-compose logs --since 24h
)
echo.
pause
goto menu

:show_all_logs
echo.
echo === 保存されているログ（最新100行）===
echo.
if exist "docker-compose.tmp.yml" (
    docker-compose -f docker-compose.tmp.yml logs --tail=100 business-data-processor
) else (
    docker-compose logs --tail=100 business-data-processor
)
echo.
pause
goto end

:end
echo.
echo ログ確認を終了します
echo.