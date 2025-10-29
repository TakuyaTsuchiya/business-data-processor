# CLAUDE.md

このファイルはClaude Code (claude.ai/code) がこのリポジトリで作業する際のガイドです。

## 🌟 プロジェクト概要

**Business Data Processor** - 不動産管理業務データ処理システム

不動産管理業務における契約データの変換・統合処理を自動化するStreamlit製のWebアプリケーション。

## 📁 主要ディレクトリ構造

```
business-data-processor/
├── app.py                        # エントリポイント（159行）
├── components/                   # UI共通コンポーネント
├── screens/                      # 画面モジュール（Phase 2完了）
│   ├── mirail_autocall.py       # ミライルオートコール
│   ├── faith_autocall.py        # フェイスオートコール
│   ├── plaza_autocall.py        # プラザオートコール
│   ├── sms/                     # SMS系画面
│   ├── registration/            # 新規登録系画面
│   └── others/                  # その他画面
├── processors/                   # データ処理モジュール
│   ├── mirail_autocall/         # ミライル（6種類）
│   ├── faith_autocall/          # フェイス（3種類）
│   ├── plaza_autocall/          # プラザ（3種類）
│   ├── mirail_sms/              # ミライルSMS
│   ├── faith_sms/               # フェイスSMS
│   ├── plaza_sms/               # プラザSMS
│   └── sms_common/              # SMS共通処理
└── templates/                   # テンプレートファイル
```

## 🔧 技術仕様

- **言語**: Python 3.8+
- **フレームワーク**: Streamlit 1.28+
- **データ処理**: pandas 2.0+
- **デプロイ**: 手動git pullフロー

## 📋 重要な注意事項

### CSVファイル処理
- **文字化け対応**: 必ずエンコーディング（cp932 → shift_jis → utf-8-sig）を試して解消する
- **絵文字使用禁止**: CP932エンコーディングエラーの原因となるため

### アーク新規登録テンプレート
- **111列厳守**: 列数・順序・名前を一切変更しない
- **空列対応**: pandasの空列処理問題に注意（仮名前方式を使用）

### SMS共通処理
- **入金予定日フィルター**: フェイス・ミライル・プラザ全てで有効（date_filter: 'before_today'）

## 🚀 開発ルール

1. **新機能追加時**: 適切なscreens/ディレクトリに配置
2. **プロセッサー作成**: processors/配下の適切な場所に配置
3. **app.py変更禁止**: エントリポイントのみ、ビジネスロジックは追加しない
4. **テスト必須**: 機能追加時は必ず動作確認

## 🎯 開発手法

### Exec Plan方式
複雑な機能開発時は**exec plan**方式を採用する：

#### 1. 計画フェーズ
- `plans.md`に詳細設計を作成
- 進捗状況（チェックボックス形式）
- 意思決定ログ（日付と決定内容）
- 驚きと発見（実装中に気づいた重要事項）
- ToDoリスト（細かいタスク管理）

#### 2. レビューフェーズ
- 計画内容を確認・承認
- 必要に応じて修正・調整

#### 3. 実装フェーズ
- TDD（Test-Driven Development）で実装
- `plans.md`を常に最新に保つ（生きた文書）
- テスト監視：長時間失敗が続く場合は軌道修正

#### 4. リリースフェーズ
- 最終テスト
- `plans.md`の完了記録
- PR作成

#### 用語の重要性
- **"exec plan"**: この固有用語を使うことで、必ず`plans.md`を作成・参照する
- 計画書は単なる設計文書ではなく、実装の羅針盤

#### Git戦略
- featureブランチで作業
- 適切な粒度でcommit（各フェーズごと）
- commit message: Conventional Commits準拠

## 📞 サポート

- **開発者**: Takuya Tsuchiya
- **AI支援**: Claude Code (Anthropic)

---
最終更新: 2025-10-29