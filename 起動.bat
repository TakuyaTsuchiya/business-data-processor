@echo off
chcp 65001 > nul
title Business Data Processor - 起動中

:: 実行ディレクトリをバッチファイルの場所に設定
cd /d "%~dp0"

echo ================================================
echo    Business Data Processor v2.1.0 起動ツール
echo ================================================
echo.

:: Docker Desktop起動確認
:: 既存のコンテナ・イメージをクリーンアップ（権限エラー対策）
echo [1/5] 既存のコンテナをクリーンアップしています...
docker stop business-data-processor > nul 2>&1
docker rm business-data-processor > nul 2>&1
echo ✅ クリーンアップが完了しました

echo.
echo [2/5] Docker Desktop の起動を確認しています...
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

:: 必要ファイルの存在確認
echo.
echo [3/6] 必要ファイルの存在を確認しています...
if not exist "app.py" (
    echo ❌ エラー: 必要ファイル（app.py）が見つかりません
    echo.
    echo 正しいフォルダで実行してください。
    echo このバッチファイルは解凍したフォルダ内で実行する必要があります。
    echo.
    pause
    exit /b 1
)
if not exist "docker-compose.yml" (
    echo ❌ エラー: 必要ファイル（docker-compose.yml）が見つかりません
    echo.
    echo 正しいフォルダで実行してください。
    echo.
    pause
    exit /b 1
)
echo ✅ 必要ファイルが確認できました

:: 必要なフォルダ作成
echo.
echo [4/6] 必要なフォルダを作成しています...
if not exist "data" mkdir data
if not exist "downloads" mkdir downloads
if not exist "logs" mkdir logs
echo ✅ フォルダの準備が完了しました

:: Docker Compose設定の動的生成
echo.
echo [5/6] Docker設定を準備しています...
powershell -Command "$PSDefaultParameterValues['Out-File:Encoding']='utf8'; $currentPath = (Get-Location).Path; (Get-Content 'docker-compose.yml' -Encoding UTF8) -replace 'PLACEHOLDER_DATA_PATH', ($currentPath + '\data') -replace 'PLACEHOLDER_DOWNLOADS_PATH', ($currentPath + '\downloads') -replace 'PLACEHOLDER_LOGS_PATH', ($currentPath + '\logs') | Set-Content 'docker-compose.tmp.yml' -Encoding UTF8"
if not exist "docker-compose.tmp.yml" (
    echo ⚠️ 動的設定生成に失敗。標準設定を使用します...
    copy "docker-compose.yml" "docker-compose.tmp.yml" > nul
)
echo ✅ Docker設定の準備が完了しました

:: Docker イメージのビルド・起動
echo.
echo [6/6] アプリケーションを起動しています...
echo 初回起動時は数分かかる場合があります。お待ちください...
echo.

docker-compose -f docker-compose.tmp.yml up -d --build

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
echo アプリケーションの起動を待っています...
echo.
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
docker-compose -f docker-compose.tmp.yml logs --tail=20 -f
goto end

:timeout
echo.
echo ================================================
echo ⚠️ 起動完了まで3分以上かかっています
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
exit /b 2

:end
pause