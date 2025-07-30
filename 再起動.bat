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
powershell -Command "$PSDefaultParameterValues['Out-File:Encoding']='utf8'; $currentPath = (Get-Location).Path; (Get-Content 'docker-compose.yml' -Encoding UTF8) -replace 'PLACEHOLDER_DATA_PATH', ($currentPath + '\data') -replace 'PLACEHOLDER_DOWNLOADS_PATH', ($currentPath + '\downloads') -replace 'PLACEHOLDER_LOGS_PATH', ($currentPath + '\logs') | Set-Content 'docker-compose.tmp.yml' -Encoding UTF8"
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

:: ヘルスチェック（最大180秒待機）
set /a count=0
:healthcheck
set /a count+=1
if %count% gtr 36 goto timeout

:: Streamlitの起動確認（curlではなくtelnetで確認）
powershell -Command "try { $result = Test-NetConnection -ComputerName localhost -Port 8501 -WarningAction SilentlyContinue; if ($result.TcpTestSucceeded) { exit 0 } else { exit 1 } } catch { exit 1 }" > nul 2>&1
if errorlevel 1 (
    echo アプリケーション起動中... %count%回目のチェック（5秒間隔）
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
echo ================================================
echo ⚠️ 再起動完了まで3分以上かかっています
echo ================================================
echo.
echo Dockerコンテナは正常に動作中です。
echo Streamlitの初期化に時間がかかっている可能性があります。
echo. 
echo 対処法:
echo 1. 手動でブラウザから http://localhost:8501 にアクセス
echo 2. もう少し待ってから再度アクセス
echo 3. ログを確認: ログ確認.bat を実行
echo.
echo ブラウザを自動で開きます...
start http://localhost:8501
echo.
pause