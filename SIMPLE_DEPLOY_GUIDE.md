# シンプルデプロイガイド

## 概要
Blue-GreenデプロイメントとGitHub Actionsを無効化し、最もシンプルな手動デプロイ方式に変更しました。

## デプロイ手順

### 1. ローカルで開発
```bash
# 開発・テスト
streamlit run app.py

# コミット・プッシュ
git add .
git commit -m "修正内容"
git push origin main
```

### 2. VPSにデプロイ
```bash
# VPSにSSH接続
ssh ubuntu@153.120.4.214

# 最新コードを取得
cd /home/ubuntu/apps/business-data-processor
git pull origin main

# 完了！（自動的に反映されます）
```

## なぜこれで動くのか？

docker-compose.ymlで以下の設定をしているため：
```yaml
volumes:
  - ./:/app:ro  # ホストのコードをコンテナにマウント
```

`git pull`でホスト側のファイルが更新されると、コンテナ内にも即座に反映されます。

## トラブルシューティング

### 反映されない場合
```bash
# コンテナを再起動
docker-compose restart
```

### 大きな変更の場合（依存関係の追加など）
```bash
docker-compose down
docker-compose build --no-cache
docker-compose up -d
```

## 無効化したもの
- Blue-Greenデプロイメント
- GitHub Actions自動デプロイ
- 複雑なデプロイスクリプト

## メリット
- 理解しやすい
- トラブルシューティングが簡単
- 依存関係が少ない
- 手動なので完全にコントロール可能