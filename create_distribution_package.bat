@echo off
chcp 65001 > nul
title Business Data Processor - 配布パッケージ作成

echo ================================================
echo    Business Data Processor v2.1.0
echo    Docker版 配布パッケージ作成ツール
echo ================================================
echo.

:: 配布用フォルダ名
set DIST_NAME=business-data-processor-docker-v2.1.0
set DIST_PATH=%cd%\%DIST_NAME%

:: 既存の配布フォルダを削除
if exist "%DIST_PATH%" (
    echo 既存の配布フォルダを削除しています...
    rmdir /s /q "%DIST_PATH%"
)

:: 配布フォルダ作成
echo [1/5] 配布フォルダを作成しています...
mkdir "%DIST_PATH%"
mkdir "%DIST_PATH%\processors"
mkdir "%DIST_PATH%\data"
mkdir "%DIST_PATH%\downloads"
mkdir "%DIST_PATH%\logs"
mkdir "%DIST_PATH%\docs"

:: 必須ファイルをコピー
echo.
echo [2/5] 必須ファイルをコピーしています...

:: Docker関連
copy "Dockerfile" "%DIST_PATH%\" > nul
copy "docker-compose.yml" "%DIST_PATH%\" > nul
copy ".dockerignore" "%DIST_PATH%\" > nul

:: Python関連
copy "requirements.txt" "%DIST_PATH%\" > nul
copy "app.py" "%DIST_PATH%\" > nul

:: バッチファイル
copy "🚀起動.bat" "%DIST_PATH%\" > nul
copy "🛑停止.bat" "%DIST_PATH%\" > nul
copy "🔄再起動.bat" "%DIST_PATH%\" > nul
copy "📊ログ確認.bat" "%DIST_PATH%\" > nul

:: ドキュメント
copy "📋Docker版使い方.txt" "%DIST_PATH%\" > nul
copy "README_Docker版.md" "%DIST_PATH%\README.md" > nul
copy "CLAUDE.md" "%DIST_PATH%\" > nul
copy "DOCKER移植ガイド.md" "%DIST_PATH%\" > nul

:: プロセッサーをコピー
echo.
echo [3/5] プロセッサーモジュールをコピーしています...
xcopy /s /e /i /q "processors" "%DIST_PATH%\processors" > nul

:: ドキュメントをコピー
echo.
echo [4/5] 詳細ドキュメントをコピーしています...
copy "docs\Docker_Desktop_インストールガイド.md" "%DIST_PATH%\docs\" > nul
copy "docs\トラブルシューティング.md" "%DIST_PATH%\docs\" > nul
copy "docs\Docker基礎知識.md" "%DIST_PATH%\docs\" > nul

:: 不要ファイルの削除
echo.
echo [5/5] 不要ファイルを削除しています...
del /s /q "%DIST_PATH%\*.pyc" 2>nul
del /s /q "%DIST_PATH%\*_test.py" 2>nul
rmdir /s /q "%DIST_PATH%\__pycache__" 2>nul
rmdir /s /q "%DIST_PATH%\processors\__pycache__" 2>nul

:: 配布パッケージ情報ファイル作成
echo Business Data Processor v2.1.0 Docker版 > "%DIST_PATH%\VERSION.txt"
echo 作成日: %date% %time% >> "%DIST_PATH%\VERSION.txt"
echo. >> "%DIST_PATH%\VERSION.txt"
echo 含まれるプロセッサー: >> "%DIST_PATH%\VERSION.txt"
echo - ミライル: 6種類 >> "%DIST_PATH%\VERSION.txt"
echo - フェイス: 3種類 >> "%DIST_PATH%\VERSION.txt"
echo - プラザ: 3種類 >> "%DIST_PATH%\VERSION.txt"
echo - アーク: 1種類 >> "%DIST_PATH%\VERSION.txt"
echo 合計: 15種類 >> "%DIST_PATH%\VERSION.txt"

:: 完了メッセージ
echo.
echo ================================================
echo ✅ 配布パッケージの作成が完了しました！
echo ================================================
echo.
echo 作成場所: %DIST_PATH%
echo.
echo 次の手順:
echo 1. %DIST_NAME% フォルダをZIP圧縮
echo 2. 配布先に送付
echo 3. 受取側で解凍して 🚀起動.bat を実行
echo.
echo ファイル数:
dir /s /b "%DIST_PATH%" | find /c /v "" 
echo.
echo ================================================
echo.

:: ZIP圧縮するか確認
echo ZIP圧縮を実行しますか？ (Y/N)
choice /c YN /n
if errorlevel 2 goto end
if errorlevel 1 goto compress

:compress
echo.
echo ZIP圧縮を実行しています...
powershell -command "Compress-Archive -Path '%DIST_PATH%' -DestinationPath '%DIST_NAME%.zip' -Force"

if exist "%DIST_NAME%.zip" (
    echo.
    echo ✅ ZIP圧縮が完了しました: %DIST_NAME%.zip
    echo ファイルサイズ: 
    powershell -command "(Get-Item '%DIST_NAME%.zip').Length / 1MB" | findstr /r "[0-9]"
    echo MB
) else (
    echo ❌ ZIP圧縮に失敗しました
)

:end
echo.
pause