# Blue-Greenデプロイメント緊急修正ガイド

## 問題の概要

2025年8月30日、Blue-Greenデプロイメントの初回実行で以下の問題が発生：

1. **Git pullエラー**: 本番環境の `nginx/upstream.conf` にローカル変更があるため、マージできない
2. **Nginxコンテナエラー**: `bdp-nginx` コンテナが存在しないか、起動していない

## 緊急修正手順

### 1. SSHで本番環境にログイン

```bash
ssh ubuntu@[VPS_HOST]
cd /home/ubuntu/apps/business-data-processor
```

### 2. Git pullエラーの解決

```bash
# 現在の状態を確認
git status

# ローカル変更を一時退避
git stash

# 最新のコードを取得
git pull origin main

# 必要に応じてstashを戻す（通常は不要）
# git stash pop
```

### 3. Nginxコンテナの確認と起動

```bash
# 現在のコンテナを確認
docker ps -a | grep nginx

# Nginxコンテナが存在しない場合、起動
cd deploy
docker-compose -f docker-compose.prod.yml up -d nginx

# Nginxコンテナの状態確認
docker ps | grep nginx
```

### 4. Blue-Greenデプロイの再実行

```bash
# deployディレクトリで実行
cd /home/ubuntu/apps/business-data-processor/deploy
chmod +x deploy.sh
./deploy.sh
```

### 5. 動作確認

```bash
# ヘルスチェック
curl http://localhost/_stcore/health

# ログ確認
docker logs bdp-nginx
docker logs bdp-green
```

## 今後の対応

### deploy.shの改善点

1. Nginxコンテナの存在確認を追加
2. Git pullエラーのハンドリング
3. より詳細なエラーメッセージ

### GitHub Actionsの改善点

1. Git操作のエラーハンドリング追加
2. 初回デプロイ用の特別な処理

## トラブルシューティング

### Q: Nginxコンテナが起動しない場合

```bash
# Nginx設定の検証
docker run --rm -v $(pwd)/../nginx/nginx.conf:/etc/nginx/nginx.conf:ro nginx:alpine nginx -t

# ログ確認
docker-compose -f docker-compose.prod.yml logs nginx
```

### Q: ポート競合エラーが出る場合

```bash
# 80番ポートを使用しているプロセスを確認
sudo lsof -i :80

# 古いコンテナを停止
docker-compose down
```

### Q: Blue/Green切り替えが機能しない場合

```bash
# 手動でupstream.confを編集
vi ../nginx/upstream.conf

# Nginxをリロード
docker exec bdp-nginx nginx -s reload
```

## 連絡先

問題が解決しない場合は、開発チームに連絡してください。