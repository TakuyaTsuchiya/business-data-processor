@echo off
echo =====================================
echo  Business Data Processor v2.1.0
echo  Docker版統合データ処理システム
echo =====================================
echo.

echo Dockerコンテナを起動中...
echo.

REM データフォルダ作成
if not exist "data" mkdir data
if not exist "downloads" mkdir downloads

REM Docker Compose起動
docker-compose up -d

echo.
echo =====================================
echo  起動完了！
echo =====================================
echo.
echo ブラウザで以下のURLにアクセスしてください:
echo http://localhost:8501
echo.
echo コンテナ停止: docker-compose down
echo ログ確認: docker-compose logs -f
echo.
pause