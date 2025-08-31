#!/bin/bash

# Business Data Processor - 手動デプロイスクリプト
# Blue-Greenが失敗した時の緊急デプロイ用

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

# VPS接続情報
VPS_HOST="${VPS_HOST:-}"
VPS_USERNAME="${VPS_USERNAME:-ubuntu}"
VPS_KEY_PATH="${VPS_KEY_PATH:-~/.ssh/id_rsa}"

# 使用方法
usage() {
    cat << EOF
使用方法:
    ./manual_deploy.sh <VPS_HOST>

例:
    ./manual_deploy.sh your-vps-host.com
    
環境変数:
    VPS_HOST - VPSのホスト名またはIPアドレス
    VPS_USERNAME - SSHユーザー名 (デフォルト: ubuntu)
    VPS_KEY_PATH - SSH秘密鍵のパス (デフォルト: ~/.ssh/id_rsa)
EOF
    exit 1
}

# 引数チェック
if [ -z "$1" ] && [ -z "$VPS_HOST" ]; then
    error "VPSホストが指定されていません"
    usage
fi

VPS_HOST="${1:-$VPS_HOST}"

log "========================================="
log "Business Data Processor 手動デプロイ開始"
log "========================================="
log "対象ホスト: $VPS_HOST"
log "ユーザー: $VPS_USERNAME"

# 1. 現在のコミット情報を取得
CURRENT_COMMIT=$(git rev-parse --short HEAD)
log "現在のコミット: $CURRENT_COMMIT"

# 2. SSH接続テスト
log "SSH接続をテストしています..."
if ssh -o ConnectTimeout=10 -o StrictHostKeyChecking=no -i "$VPS_KEY_PATH" "$VPS_USERNAME@$VPS_HOST" "echo 'SSH接続成功'" > /dev/null 2>&1; then
    success "SSH接続確認OK"
else
    error "SSH接続に失敗しました"
    exit 1
fi

# 3. リモートでデプロイコマンドを実行
log "本番環境でデプロイを開始します..."

ssh -i "$VPS_KEY_PATH" "$VPS_USERNAME@$VPS_HOST" << 'REMOTE_SCRIPT'
set -e

# カラー出力の定義
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

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

# アプリケーションディレクトリに移動
cd /home/ubuntu/apps/business-data-processor

log "現在のディレクトリ: $(pwd)"

# 1. Git状態確認
log "Gitの状態を確認しています..."
if [ -n "$(git status --porcelain)" ]; then
    warning "ローカル変更が検出されました"
    git status --short
    
    # upstream.confの変更を一時退避
    if git status --porcelain | grep -q "nginx/upstream.conf"; then
        log "nginx/upstream.confの変更を退避します..."
        git checkout -- nginx/upstream.conf
    fi
    
    # その他の変更も退避
    git stash push -m "Manual deploy stash at $(date)"
fi

# 2. 最新コードを取得
log "最新のコードを取得しています..."
git fetch origin main
git reset --hard origin/main
success "コード更新完了"

# 3. 現在実行中のコンテナを確認
log "現在のコンテナ状態を確認しています..."
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# 4. シンプルなデプロイ（Blue-Greenを使わない）
log "アプリケーションを再起動しています..."

# docker-compose.ymlを使用（本番用設定があればそれを使用）
if [ -f "docker-compose.prod.yml" ]; then
    COMPOSE_FILE="docker-compose.prod.yml"
elif [ -f "deploy/docker-compose.prod.yml" ]; then
    cd deploy
    COMPOSE_FILE="docker-compose.prod.yml"
else
    COMPOSE_FILE="docker-compose.yml"
fi

log "使用するCompose file: $COMPOSE_FILE"

# 既存のコンテナを停止
log "既存のコンテナを停止しています..."
docker-compose -f $COMPOSE_FILE down || true

# 新しいイメージをビルド
log "新しいイメージをビルドしています..."
docker-compose -f $COMPOSE_FILE build

# コンテナを起動
log "コンテナを起動しています..."
docker-compose -f $COMPOSE_FILE up -d

# 5. ヘルスチェック
log "アプリケーションの起動を待っています..."
MAX_ATTEMPTS=30
ATTEMPT=0

while [ $ATTEMPT -lt $MAX_ATTEMPTS ]; do
    # Streamlitのヘルスチェック
    if docker exec $(docker ps -q -f name=app) curl -f http://localhost:8501/_stcore/health &>/dev/null 2>&1; then
        success "アプリケーションが正常に起動しました！"
        break
    fi
    
    ATTEMPT=$((ATTEMPT + 1))
    if [ $ATTEMPT -eq $MAX_ATTEMPTS ]; then
        error "アプリケーションの起動がタイムアウトしました"
        docker logs $(docker ps -aq -f name=app | head -1) --tail 50
        exit 1
    fi
    
    echo -n "."
    sleep 2
done
echo ""

# 6. 最終確認
log "デプロイ完了確認..."
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# アプリケーションログの最後の数行を表示
log "アプリケーションログ（最新10行）:"
docker logs $(docker ps -q -f name=app) --tail 10

success "デプロイが完了しました！"
log "アプリケーションURL: https://mirail.net"

REMOTE_SCRIPT

# デプロイ結果の確認
if [ $? -eq 0 ]; then
    echo ""
    success "========================================="
    success "手動デプロイが成功しました！"
    success "========================================="
    log "デプロイされたコミット: $CURRENT_COMMIT"
    log "アプリケーションURL: https://mirail.net"
    log ""
    log "動作確認を行ってください："
    log "1. ブラウザで https://mirail.net にアクセス"
    log "2. 各機能が正常に動作することを確認"
else
    echo ""
    error "========================================="
    error "デプロイに失敗しました"
    error "========================================="
    error "上記のエラーメッセージを確認してください"
    exit 1
fi