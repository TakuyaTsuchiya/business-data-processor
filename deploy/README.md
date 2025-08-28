# Blue-Green Deployment Guide

このディレクトリには、Business Data ProcessorのBlue-Greenデプロイメントシステムが含まれています。

## 🎯 概要

Blue-Greenデプロイメントにより、ダウンタイムなしで安全にアプリケーションを更新できます。

## 📁 ファイル構成

```
deploy/
├── docker-compose.prod.yml    # Blue-Green本番環境設定
├── deploy.sh                  # デプロイスクリプト
├── rollback.sh               # ロールバックスクリプト
├── health-check.sh           # ヘルスチェック
├── smoke-test.py            # スモークテスト
└── README.md                # このファイル

nginx/
├── nginx.conf               # Nginx設定
└── upstream.conf           # アップストリーム設定（動的切り替え）

deploy_manager.py           # Claude Code用デプロイ管理ツール
.env.deploy                # VPS接続設定
```

## 🚀 使用方法

### 1. 初期セットアップ

```bash
# Claude Codeから実行
python deploy_manager.py setup
```

### 2. デプロイ実行

```bash
# 方法1: Claude Codeから
python deploy_manager.py deploy

# 方法2: VPS上で直接
./deploy/deploy.sh
```

### 3. ロールバック

```bash
# 問題が発生した場合
python deploy_manager.py rollback
```

### 4. 状態確認

```bash
# デプロイ状況の確認
python deploy_manager.py status

# ログの確認
python deploy_manager.py logs
```

## 🔧 VPSでの初期セットアップ

VPS上で初回のみ実行：

```bash
# プロジェクトのクローン
git clone https://github.com/your-repo/business-data-processor.git
cd business-data-processor

# deployディレクトリに移動
cd deploy

# Docker環境を起動（初回）
docker-compose -f docker-compose.prod.yml up -d

# 動作確認
./health-check.sh
```

## 📊 Blue-Greenの仕組み

1. **Blue環境**: 現在稼働中のバージョン
2. **Green環境**: 新しいバージョンをデプロイする環境
3. **Nginx**: トラフィックをBlueまたはGreenに振り分け

### デプロイフロー

```
1. Green環境に新バージョンをビルド
2. ヘルスチェックで動作確認
3. Nginxの設定を切り替え（1秒以内）
4. Blue環境を停止
```

## 🛡️ 安全機能

- **自動ヘルスチェック**: デプロイ前に新環境の動作を確認
- **即座にロールバック**: 問題発生時は前のバージョンに瞬時に戻る
- **スモークテスト**: 基本的な機能が正常に動作することを確認
- **ログ記録**: デプロイ・ロールバックの履歴を保存

## 📝 設定ファイル

### .env.deploy
```bash
VPS_HOST=your-vps-ip
VPS_USER=ubuntu
VPS_PORT=22
PROJECT_PATH=/home/ubuntu/business-data-processor
```

### SSH鍵の設置
```bash
# ローカルで鍵生成
ssh-keygen -t ed25519 -f ~/.ssh/vps-deploy-key

# VPSに公開鍵を追加
ssh-copy-id -i ~/.ssh/vps-deploy-key.pub user@your-vps
```

## 🔍 トラブルシューティング

### デプロイが失敗する場合
```bash
# 詳細なログを確認
python deploy_manager.py logs

# ヘルスチェックを実行
./deploy/health-check.sh

# 手動でロールバック
./deploy/rollback.sh
```

### Docker関連の問題
```bash
# コンテナの状態確認
docker ps -a

# ログの確認
docker logs bdp-blue
docker logs bdp-green
docker logs bdp-nginx

# コンテナの再起動
docker-compose -f docker-compose.prod.yml restart
```

## 📈 メリット

1. **ゼロダウンタイム**: ユーザーへの影響なし
2. **安全なデプロイ**: 事前テスト後に切り替え
3. **瞬時のロールバック**: 問題発生時は即座に復旧
4. **開発効率向上**: 大胆なリファクタリングも安心

## 🎯 次のステップ

このBlue-Greenデプロイシステムにより、app.pyの大規模リファクタリングも安心して実行できるようになりました！