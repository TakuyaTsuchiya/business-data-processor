#!/bin/bash

echo "=== VPS最新コード反映問題デバッグスクリプト ==="
echo ""

# 1. 現在のコンテナ状態を確認
echo "1. 実行中のコンテナ確認:"
echo "----------------------------------------"
docker ps --format "table {{.ID}}\t{{.Names}}\t{{.Image}}\t{{.Status}}\t{{.CreatedAt}}"
echo ""

# 2. app.pyの最終更新時刻を確認（ホスト側）
echo "2. ホスト側のapp.py最終更新時刻:"
echo "----------------------------------------"
ls -la /home/ubuntu/business-data-processor/app.py
echo ""

# 3. コンテナ内のapp.pyを確認
echo "3. コンテナ内のapp.py最終更新時刻:"
echo "----------------------------------------"
docker exec business-data-processor-app-1 ls -la /app/app.py
echo ""

# 4. コンテナ内のapp.pyの特定部分を確認（バージョン表示部分）
echo "4. コンテナ内のapp.pyバージョン表示部分:"
echo "----------------------------------------"
docker exec business-data-processor-app-1 grep -A 3 "Business Data Processor" /app/app.py | head -10
echo ""

# 5. ホスト側のapp.pyの特定部分を確認
echo "5. ホスト側のapp.pyバージョン表示部分:"
echo "----------------------------------------"
grep -A 3 "Business Data Processor" /home/ubuntu/business-data-processor/app.py | head -10
echo ""

# 6. Dockerボリュームマウント設定を確認
echo "6. コンテナのマウント設定確認:"
echo "----------------------------------------"
docker inspect business-data-processor-app-1 | grep -A 10 "Mounts"
echo ""

# 7. Nginxキャッシュ設定を確認
echo "7. Nginxキャッシュ設定確認:"
echo "----------------------------------------"
docker exec business-data-processor-nginx-1 cat /etc/nginx/nginx.conf | grep -E "proxy_cache|proxy_no_cache|expires"
echo ""

# 8. Streamlitのキャッシュ確認
echo "8. Streamlitキャッシュディレクトリ確認:"
echo "----------------------------------------"
docker exec business-data-processor-app-1 ls -la /app/.streamlit 2>/dev/null || echo "Streamlitキャッシュディレクトリなし"
echo ""

# 9. コンテナのビルド時刻確認
echo "9. コンテナイメージのビルド時刻:"
echo "----------------------------------------"
docker images --format "table {{.Repository}}\t{{.Tag}}\t{{.CreatedAt}}\t{{.Size}}" | grep business-data-processor
echo ""

# 10. git の状態確認
echo "10. Gitリポジトリの状態:"
echo "----------------------------------------"
cd /home/ubuntu/business-data-processor && git status
echo ""
echo "最新コミット:"
git log -1 --oneline
echo ""

# 11. docker-compose.ymlのボリューム設定確認
echo "11. docker-compose.ymlのボリューム設定:"
echo "----------------------------------------"
grep -A 5 "volumes:" /home/ubuntu/business-data-processor/docker-compose.yml
echo ""

# 12. 実際にブラウザで表示されているバージョンと比較するためのURL
echo "12. 確認用URL:"
echo "----------------------------------------"
echo "http://$(curl -s ifconfig.me):8501"
echo ""
echo "※ブラウザで以下を確認してください："
echo "- Ctrl+F5で強制リロード"
echo "- プライベートブラウジングモードで開く"
echo "- 異なるブラウザで開く"
echo ""

# 13. コンテナ再起動の推奨コマンド
echo "13. 問題が続く場合の対処法:"
echo "----------------------------------------"
echo "# 1. コンテナを完全に再作成（キャッシュなし）"
echo "docker-compose down"
echo "docker-compose build --no-cache"
echo "docker-compose up -d"
echo ""
echo "# 2. ボリュームマウントの確認（開発モード）"
echo "# docker-compose.ymlで以下の設定があることを確認:"
echo "# volumes:"
echo "#   - ./:/app"
echo ""
echo "# 3. Streamlitの開発モードでの起動確認"
echo "# CMD設定が以下であることを確認:"
echo "# streamlit run app.py --server.port=8501 --server.address=0.0.0.0"
echo ""