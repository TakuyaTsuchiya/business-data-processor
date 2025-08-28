#!/bin/bash

# Business Data Processor - Health Check Script
# 定期的に実行して環境の状態を確認

# カラー出力の定義
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# 環境をチェック
check_environment() {
    local env=$1
    local container_name="bdp-$env"
    
    if docker ps | grep -q "$container_name"; then
        if docker exec $container_name curl -f http://localhost:8501/_stcore/health &>/dev/null; then
            echo -e "${GREEN}✓${NC} $env environment is healthy"
            return 0
        else
            echo -e "${RED}✗${NC} $env environment is unhealthy"
            return 1
        fi
    else
        echo -e "${YELLOW}-${NC} $env environment is not running"
        return 2
    fi
}

# Nginxをチェック
check_nginx() {
    if docker ps | grep -q "bdp-nginx"; then
        if docker exec bdp-nginx nginx -t &>/dev/null; then
            echo -e "${GREEN}✓${NC} Nginx is healthy"
            
            # 現在のアップストリーム設定を表示
            CURRENT=$(grep -oP 'server\s+app-\K(blue|green)' nginx/upstream.conf | head -1)
            echo "  Currently routing to: $CURRENT"
            return 0
        else
            echo -e "${RED}✗${NC} Nginx configuration error"
            return 1
        fi
    else
        echo -e "${RED}✗${NC} Nginx is not running"
        return 1
    fi
}

# プロジェクトルートに移動
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "$SCRIPT_DIR/.."

echo "Business Data Processor - System Health Check"
echo "============================================="
echo "Time: $(date)"
echo ""

# 各コンポーネントをチェック
echo "Checking environments:"
check_environment "blue"
BLUE_STATUS=$?

check_environment "green"
GREEN_STATUS=$?

echo ""
echo "Checking proxy:"
check_nginx
NGINX_STATUS=$?

echo ""

# 全体的なステータスを判定
if [ $NGINX_STATUS -eq 0 ] && ([ $BLUE_STATUS -eq 0 ] || [ $GREEN_STATUS -eq 0 ]); then
    echo -e "${GREEN}Overall status: HEALTHY${NC}"
    exit 0
else
    echo -e "${RED}Overall status: UNHEALTHY${NC}"
    exit 1
fi