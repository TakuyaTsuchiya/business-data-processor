#!/bin/bash

# Business Data Processor - Blue-Green Deployment Script
# 安全なゼロダウンタイムデプロイを実現

set -e  # エラーで即座に停止

# カラー出力の定義
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# ログ関数
log() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# プロジェクトルートに移動
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "$SCRIPT_DIR/.."

log "Business Data Processor - Blue-Green Deployment Starting..."

# 1. 現在アクティブな環境を確認
log "Checking current active environment..."
CURRENT_ACTIVE=$(docker ps --filter "label=deployment" --format '{{.Labels}}' | grep -E "deployment=(blue|green)" | sed 's/.*deployment=\([^,]*\).*/\1/' | head -1)

if [ -z "$CURRENT_ACTIVE" ]; then
    warning "No active deployment found. Assuming first deployment."
    CURRENT_ACTIVE="green"  # 初回はblueにデプロイ
    NEW_ENV="blue"
else
    log "Current active environment: $CURRENT_ACTIVE"
    if [ "$CURRENT_ACTIVE" = "blue" ]; then
        NEW_ENV="green"
    else
        NEW_ENV="blue"
    fi
fi

log "Will deploy to: $NEW_ENV"

# 2. 新環境のコンテナをビルド
log "Building new environment ($NEW_ENV)..."
cd deploy
docker-compose -f docker-compose.prod.yml build app-$NEW_ENV

# 3. 新環境を起動（まだトラフィックは向いていない）
log "Starting $NEW_ENV environment..."
docker-compose -f docker-compose.prod.yml up -d app-$NEW_ENV

# 4. ヘルスチェック（最大60秒待機）
log "Waiting for $NEW_ENV to be healthy..."
MAX_ATTEMPTS=30
ATTEMPT=0

while [ $ATTEMPT -lt $MAX_ATTEMPTS ]; do
    if docker exec bdp-$NEW_ENV curl -f http://localhost:8501/_stcore/health &>/dev/null; then
        success "$NEW_ENV is healthy!"
        break
    fi
    
    ATTEMPT=$((ATTEMPT + 1))
    if [ $ATTEMPT -eq $MAX_ATTEMPTS ]; then
        error "$NEW_ENV failed health check after 60 seconds"
        log "Rolling back..."
        docker-compose -f docker-compose.prod.yml stop app-$NEW_ENV
        docker-compose -f docker-compose.prod.yml rm -f app-$NEW_ENV
        exit 1
    fi
    
    echo -n "."
    sleep 2
done
echo ""

# 5. 追加の検証（オプション）
log "Running additional validation..."
# Streamlitのメインページが正常に返ってくるか確認
if ! docker exec bdp-$NEW_ENV curl -s http://localhost:8501 | grep -q "streamlit"; then
    error "$NEW_ENV is not serving Streamlit properly"
    log "Rolling back..."
    docker-compose -f docker-compose.prod.yml stop app-$NEW_ENV
    exit 1
fi

# 6. Nginxの設定を更新してトラフィックを切り替え
log "Switching traffic to $NEW_ENV..."

# upstream.confを更新
cat > ../nginx/upstream.conf << EOF
# Business Data Processor - Upstream Configuration
# This file is dynamically updated during Blue-Green deployment
# Last updated: $(date)

upstream app_backend {
    server app-$NEW_ENV:8501 max_fails=3 fail_timeout=30s;
    keepalive 32;
}
EOF

# Nginxをリロード（ダウンタイムなし）
log "Reloading Nginx configuration..."
if docker exec bdp-nginx nginx -t &>/dev/null; then
    docker exec bdp-nginx nginx -s reload
    success "Nginx configuration reloaded successfully"
else
    error "Nginx configuration test failed"
    # 元の設定に戻す
    cat > ../nginx/upstream.conf << EOF
upstream app_backend {
    server app-$CURRENT_ACTIVE:8501 max_fails=3 fail_timeout=30s;
    keepalive 32;
}
EOF
    exit 1
fi

# 7. 新環境が正常に動作していることを確認（トラフィック切り替え後）
log "Verifying deployment..."
sleep 5

# Nginxを通してアクセスできるか確認
if curl -f http://localhost/_stcore/health &>/dev/null; then
    success "New deployment is serving traffic correctly"
else
    error "New deployment is not accessible through Nginx"
    # ロールバック処理をここに追加可能
fi

# 8. 古い環境を停止（オプション：すぐに停止するか、しばらく残すか選択可能）
log "Stopping old environment ($CURRENT_ACTIVE)..."
if [ "$CURRENT_ACTIVE" != "none" ]; then
    # 念のため10秒待機（接続中のセッションが終了するのを待つ）
    sleep 10
    docker-compose -f docker-compose.prod.yml stop app-$CURRENT_ACTIVE
    success "Old environment stopped"
fi

# 9. デプロイメント情報を記録
DEPLOY_INFO="deploy_$(date +%Y%m%d_%H%M%S).log"
cat > "../logs/$DEPLOY_INFO" << EOF
Deployment completed at: $(date)
Previous environment: $CURRENT_ACTIVE
New environment: $NEW_ENV
Git commit: $(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
EOF

success "Blue-Green deployment completed successfully!"
log "Active environment: $NEW_ENV"
log "Deployment log saved to: logs/$DEPLOY_INFO"

# 10. クリーンアップ（オプション）
# 古いイメージを削除してディスクスペースを節約
# docker image prune -f

echo ""
success "🚀 Deployment complete! Your application is now running on $NEW_ENV environment."