#\!/bin/bash

# VPS接続情報
VPS_HOST="mirail.net"
VPS_USER="${VPS_USER:-ubuntu}"

# カラー定義
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}VPS (mirail.net) へのデプロイを開始します${NC}"

# SSH接続してデプロイコマンドを実行
ssh ${VPS_USER}@${VPS_HOST} << 'REMOTE_SCRIPT'
set -e

cd /home/ubuntu/apps/business-data-processor

echo "現在のディレクトリ: $(pwd)"
echo "Git状態確認..."

# ローカル変更を退避
if [ -n "$(git status --porcelain)" ]; then
    echo "ローカル変更を退避します..."
    git stash push -m "Deploy stash $(date)"
fi

# 最新コードを取得
echo "最新コードを取得中..."
git fetch origin main
git reset --hard origin/main

echo "現在のコミット: $(git log -1 --oneline)"

# Dockerイメージをビルド
echo "Dockerイメージをビルド中..."
cd deploy
docker-compose -f docker-compose.prod.yml build

# Blue-Greenデプロイメント
echo "Blue-Greenデプロイメント実行中..."
./deploy.sh

echo "デプロイ完了！"
REMOTE_SCRIPT

if [ $? -eq 0 ]; then
    echo -e "${GREEN}デプロイが成功しました！${NC}"
    echo "https://mirail.net でアクセスして確認してください"
else
    echo -e "${RED}デプロイに失敗しました${NC}"
    exit 1
fi
