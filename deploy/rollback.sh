#!/bin/bash

# Business Data Processor - Rollback Script
# 問題が発生した場合に前の環境に即座に戻す

set -e

# カラー出力の定義
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

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

log "Starting rollback process..."

# 1. 現在のupstream設定を確認
CURRENT_UPSTREAM=$(grep -oP 'server\s+app-\K(blue|green)' nginx/upstream.conf | head -1)
log "Current upstream points to: $CURRENT_UPSTREAM"

# 2. ロールバック先を決定
if [ "$CURRENT_UPSTREAM" = "blue" ]; then
    ROLLBACK_TO="green"
else
    ROLLBACK_TO="blue"
fi

log "Will rollback to: $ROLLBACK_TO"

# 3. ロールバック先の環境が動いているか確認
if ! docker ps | grep -q "bdp-$ROLLBACK_TO"; then
    warning "$ROLLBACK_TO environment is not running. Starting it..."
    cd deploy
    docker-compose -f docker-compose.prod.yml up -d app-$ROLLBACK_TO
    
    # ヘルスチェック
    log "Waiting for $ROLLBACK_TO to be healthy..."
    MAX_ATTEMPTS=30
    ATTEMPT=0
    
    while [ $ATTEMPT -lt $MAX_ATTEMPTS ]; do
        if docker exec bdp-$ROLLBACK_TO curl -f http://localhost:8501/_stcore/health &>/dev/null; then
            success "$ROLLBACK_TO is healthy!"
            break
        fi
        
        ATTEMPT=$((ATTEMPT + 1))
        if [ $ATTEMPT -eq $MAX_ATTEMPTS ]; then
            error "$ROLLBACK_TO failed health check"
            exit 1
        fi
        
        echo -n "."
        sleep 2
    done
    echo ""
fi

# 4. Nginxの設定を更新
log "Updating Nginx configuration..."

cat > nginx/upstream.conf << EOF
# Business Data Processor - Upstream Configuration
# ROLLBACK performed at: $(date)

upstream app_backend {
    server app-$ROLLBACK_TO:8501 max_fails=3 fail_timeout=30s;
    keepalive 32;
}
EOF

# 5. Nginxをリロード
log "Reloading Nginx..."
if docker exec bdp-nginx nginx -t &>/dev/null; then
    docker exec bdp-nginx nginx -s reload
    success "Nginx configuration reloaded"
else
    error "Nginx configuration test failed"
    exit 1
fi

# 6. 検証
sleep 3
if curl -f http://localhost/_stcore/health &>/dev/null; then
    success "Rollback completed successfully!"
else
    error "Service is not responding after rollback"
    exit 1
fi

# 7. 問題のある環境を停止
log "Stopping problematic environment ($CURRENT_UPSTREAM)..."
cd deploy
docker-compose -f docker-compose.prod.yml stop app-$CURRENT_UPSTREAM

# 8. ロールバック情報を記録
ROLLBACK_INFO="rollback_$(date +%Y%m%d_%H%M%S).log"
cat > "../logs/$ROLLBACK_INFO" << EOF
Rollback performed at: $(date)
From environment: $CURRENT_UPSTREAM
To environment: $ROLLBACK_TO
Reason: Manual rollback initiated
EOF

success "🔄 Rollback completed! Now serving from $ROLLBACK_TO environment."
log "Rollback log saved to: logs/$ROLLBACK_INFO"