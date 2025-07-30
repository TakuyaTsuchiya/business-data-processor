@echo off
chcp 65001 > nul
title Business Data Processor - 停止中

:: 実行ディレクトリをバッチファイルの場所に設定
cd /d "%~dp0"

echo ================================================
echo    Business Data Processor v2.1.0 停止ツール
echo ================================================
echo.

:: Docker確認
docker version > nul 2>&1
if errorlevel 1 (
    echo ❌ エラー: Docker Desktop が起動していません
    echo アプリケーションは既に停止している可能性があります
    echo.
    pause
    exit /b 1
)

:: コンテナ状態確認
echo [1/3] アプリケーションの状態を確認しています...
docker ps | findstr business-data-processor > nul 2>&1
if errorlevel 1 (
    echo.
    echo ℹ️ Business Data Processor は既に停止しています
    echo.
    pause
    exit /b 0
)

echo ✅ アプリケーションが実行中です

:: 停止処理
echo.
echo [2/3] アプリケーションを停止しています...
if exist "docker-compose.tmp.yml" (
    docker-compose -f docker-compose.tmp.yml down
) else (
    docker-compose down
)

if errorlevel 1 (
    echo.
    echo ❌ エラー: 停止処理に失敗しました
    echo.
    echo 強制停止を試みます...
    docker stop business-data-processor-business-data-processor-1 2>nul || docker stop business-data-processor 2>nul
    docker rm business-data-processor-business-data-processor-1 2>nul || docker rm business-data-processor 2>nul
)

:: 停止確認
echo.
echo [3/3] 停止を確認しています...
timeout /t 2 /nobreak > nul

docker ps | findstr business-data-processor > nul 2>&1
if errorlevel 1 (
    echo.
    echo ================================================
    echo ✅ Business Data Processor が正常に停止しました
    echo ================================================
    echo.
    echo データは以下のフォルダに保存されています:
    echo - アップロードファイル: data\
    echo - ダウンロードファイル: downloads\
    echo - ログファイル: logs\
    echo.
    echo 再度起動する場合は「🚀起動.bat」を実行してください
    echo ================================================
) else (
    echo.
    echo ⚠️ 警告: アプリケーションがまだ実行中の可能性があります
    echo.
    echo 手動で停止する場合は以下のコマンドを実行してください:
    echo docker stop business-data-processor
    echo docker rm business-data-processor
)

echo.
pause