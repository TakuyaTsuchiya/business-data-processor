@echo off
chcp 65001 > nul
title Business Data Processor - 起動中

echo ================================================
echo    Business Data Processor v2.1.0 起動ツール
echo ================================================
echo.

:: Docker Desktop起動確認
echo [1/4] Docker Desktop の起動を確認しています...
docker version > nul 2>&1
if errorlevel 1 (
    echo.
    echo ❌ エラー: Docker Desktop が起動していません！
    echo.
    echo 対処方法:
    echo 1. Docker Desktop をインストールしてください
    echo    https://www.docker.com/products/docker-desktop
    echo.
    echo 2. インストール済みの場合は Docker Desktop を起動してください
    echo    スタートメニューから「Docker Desktop」を検索して実行
    echo.
    echo 3. Docker Desktop が完全に起動するまで待ってから、
    echo    このバッチファイルを再度実行してください
    echo.
    pause
    exit /b 1
)
echo ✅ Docker Desktop が起動しています

:: 必要なフォルダ作成
echo.
echo [2/4] 必要なフォルダを作成しています...
if not exist "data" mkdir data
if not exist "downloads" mkdir downloads
if not exist "logs" mkdir logs
echo ✅ フォルダの準備が完了しました

:: Docker イメージのビルド・起動
echo.
echo [3/4] アプリケーションを起動しています...
echo 初回起動時は数分かかる場合があります。お待ちください...
echo.

docker-compose up -d --build

if errorlevel 1 (
    echo.
    echo ❌ エラー: アプリケーションの起動に失敗しました
    echo.
    echo 考えられる原因:
    echo - ポート 8501 が既に使用されている
    echo - Docker Desktop のリソースが不足している
    echo - ファイルの配置が正しくない
    echo.
    echo 詳細なエラー情報:
    docker-compose logs --tail=50
    echo.
    pause
    exit /b 1
)

:: 起動待機
echo.
echo [4/4] アプリケーションの起動を待っています...
echo.
timeout /t 5 /nobreak > nul

:: ヘルスチェック（最大60秒待機）
set /a count=0
:healthcheck
set /a count+=1
if %count% gtr 12 goto timeout

docker exec business-data-processor curl -f http://localhost:8501/_stcore/health > nul 2>&1
if errorlevel 1 (
    echo アプリケーション起動中... %count%0秒経過
    timeout /t 5 /nobreak > nul
    goto healthcheck
)

:: 起動成功
echo.
echo ================================================
echo ✅ Business Data Processor が正常に起動しました！
echo ================================================
echo.
echo アクセス方法:
echo ブラウザで以下のURLにアクセスしてください
echo.
echo     http://localhost:8501
echo.
echo ※ 自動的にブラウザが開きます...
echo.
echo 停止方法:
echo 「🛑停止.bat」をダブルクリックしてください
echo ================================================
echo.

:: ブラウザを開く
start http://localhost:8501

:: ログ表示（オプション）
echo ログを確認しますか？ (Y/N)
choice /c YN /n /t 10 /d N
if errorlevel 2 goto end
if errorlevel 1 goto showlogs

:showlogs
echo.
echo === アプリケーションログ ===
docker-compose logs --tail=20 -f
goto end

:timeout
echo.
echo ⚠️ 警告: アプリケーションの起動に時間がかかっています
echo.
echo 手動でブラウザから http://localhost:8501 にアクセスしてみてください
echo ログを確認する場合は「📊ログ確認.bat」を実行してください
echo.
pause
exit /b 2

:end
pause