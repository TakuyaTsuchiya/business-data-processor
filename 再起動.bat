@echo off
chcp 65001 > nul
title Business Data Processor - 再起動中

:: 実行ディレクトリをバッチファイルの場所に設定
cd /d "%~dp0"

echo ================================================
echo    Business Data Processor v2.1.0 再起動ツール
echo ================================================
echo.

:: Docker確認
docker version > nul 2>&1
if errorlevel 1 (
    echo ❌ エラー: Docker Desktop が起動していません
    echo Docker Desktop を起動してから再度実行してください
    echo.
    pause
    exit /b 1
)

:: 停止処理
echo [1/3] アプリケーションを停止しています...
if exist "docker-compose.tmp.yml" (
    docker-compose -f docker-compose.tmp.yml down
) else (
    docker-compose down
)
timeout /t 2 /nobreak > nul

:: 起動処理
echo.
echo [2/3] アプリケーションを再起動しています...
:: Docker Compose設定の動的生成
powershell -Command "$currentPath = (Get-Location).Path; (Get-Content 'docker-compose.yml') -replace 'PLACEHOLDER_DATA_PATH', ($currentPath + '\data') -replace 'PLACEHOLDER_DOWNLOADS_PATH', ($currentPath + '\downloads') -replace 'PLACEHOLDER_LOGS_PATH', ($currentPath + '\logs') | Set-Content 'docker-compose.tmp.yml'"
if not exist "docker-compose.tmp.yml" (
    copy "docker-compose.yml" "docker-compose.tmp.yml" > nul
)

docker-compose -f docker-compose.tmp.yml up -d

if errorlevel 1 (
    echo.
    echo ❌ エラー: 再起動に失敗しました
    echo.
    pause
    exit /b 1
)

:: 起動待機
echo.
echo [3/3] アプリケーションの起動を待っています...
timeout /t 5 /nobreak > nul

:: ヘルスチェック
set /a count=0
:healthcheck
set /a count+=1
if %count% gtr 12 goto timeout

docker exec business-data-processor-business-data-processor-1 curl -f http://localhost:8501/_stcore/health > nul 2>&1 || docker exec business-data-processor curl -f http://localhost:8501/_stcore/health > nul 2>&1
if errorlevel 1 (
    echo 起動中... %count%0秒経過
    timeout /t 5 /nobreak > nul
    goto healthcheck
)

:: 再起動成功
echo.
echo ================================================
echo ✅ Business Data Processor が正常に再起動しました！
echo ================================================
echo.
echo ブラウザで http://localhost:8501 にアクセスしてください
echo.
echo ※ 自動的にブラウザが開きます...
echo ================================================

start http://localhost:8501
pause
exit /b 0

:timeout
echo.
echo ⚠️ 警告: 再起動に時間がかかっています
echo ログを確認してください
echo.
pause