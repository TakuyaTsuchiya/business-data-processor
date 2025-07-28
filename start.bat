@echo off
echo =============================================
echo   Business Data Processor 起動中...
echo =============================================
echo.

REM 現在のディレクトリに移動
cd /d "%~dp0"

echo [1/3] 依存関係をインストール中...
pip install -r requirements.txt --quiet --disable-pip-version-check

if %errorlevel% neq 0 (
    echo.
    echo エラー: 依存関係のインストールに失敗しました
    echo Python がインストールされているか確認してください
    echo.
    pause
    exit /b 1
)

echo [2/3] アプリケーションを起動中...
echo.
echo ブラウザが自動で開きます...
echo 閉じるには Ctrl+C を押してください
echo.

python -m streamlit run app.py

echo.
echo アプリケーションが終了しました
pause