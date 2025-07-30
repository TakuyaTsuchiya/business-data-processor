# CLAUDE.md

このファイルはClaude Code (claude.ai/code) がこのリポジトリで作業する際のガイドです。

## 🌟 プロジェクト概要

**Business Data Processor v2.1.0-docker** - Docker化統合データ処理システム（権限問題解決済み）

不動産管理業務における契約データの変換・統合処理を自動化するStreamlit製のWebアプリケーション。
複数の業務システム（オートコール、SMS、新規登録）を統合し、非エンジニア向けの直感的なUIを提供。

## 📁 ディレクトリ構造

```
business-data-processor/
├── README.md                     # プロジェクト概要・使用方法
├── CLAUDE.md                     # このファイル - Claude作業指示
├── app.py                        # メインStreamlitアプリケーション
├── Dockerfile                    # Docker化イメージ設定（権限問題解決済み）
├── docker-compose.yml            # Docker Compose設定
├── requirements.txt              # Python依存パッケージ定義
├── Docker使い方.txt              # 非エンジニア向けDocker簡易マニュアル
├── SOW.md                        # 作業範囲明細書（完全版）
├── DOCKER移植ガイド.md           # Docker移植・配布完全ガイド
│
├── 起動.bat                      # Docker版ワンクリック起動（6段階）
├── 停止.bat                      # Docker版安全停止
├── 再起動.bat                    # Docker版再起動（180秒タイムアウト）
├── ログ確認.bat                  # Docker版ログ確認（5種類）
│
├── processors/                   # 業務別データ処理モジュール
│   ├── mirail_autocall/          # 🏢 ミライルオートコール処理
│   │   ├── __init__.py           # モジュール初期化
│   │   ├── contract/             # 契約者オートコール
│   │   │   ├── __init__.py       # モジュール初期化
│   │   │   ├── without10k.py     # 残債除外版（1万円・1万1千円除外）
│   │   │   └── with10k.py        # 残債含む版（全件処理）
│   │   ├── guarantor/            # 保証人オートコール
│   │   │   ├── __init__.py       # モジュール初期化
│   │   │   ├── without10k.py     # 残債除外版（1万円・1万1千円除外）
│   │   │   └── with10k.py        # 残債含む版（全件処理）
│   │   └── emergency_contact/    # 緊急連絡人オートコール
│   │       ├── __init__.py       # モジュール初期化
│   │       ├── without10k.py     # 残債除外版（1万円・1万1千円除外）
│   │       └── with10k.py        # 残債含む版（全件処理）
│   │
│   ├── faith_autocall/           # 📱 フェイスオートコール処理
│   │   ├── __init__.py           # モジュール初期化
│   │   ├── contract/             # 契約者オートコール
│   │   │   ├── __init__.py       # モジュール初期化
│   │   │   └── standard.py       # 標準版（委託先法人ID 1-4）
│   │   ├── guarantor/            # 保証人オートコール
│   │   │   ├── __init__.py       # モジュール初期化
│   │   │   └── standard.py       # 保証人データ処理
│   │   └── emergency_contact/    # 緊急連絡人オートコール
│   │       ├── __init__.py       # モジュール初期化
│   │       └── standard.py       # 緊急連絡人データ処理
│   │
│   ├── plaza_autocall/           # 🏪 プラザオートコール処理
│   │   ├── __init__.py           # モジュール初期化
│   │   ├── main/                 # 契約者オートコール
│   │   │   ├── __init__.py       # モジュール初期化
│   │   │   └── standard.py       # 2ファイル処理（ContractList + Excel報告書）
│   │   ├── guarantor/            # 保証人オートコール（基本構造のみ）
│   │   │   ├── __init__.py       # モジュール初期化
│   │   │   └── standard.py       # 基本構造のみ（未実装）
│   │   └── contact/              # 緊急連絡人オートコール
│   │       ├── __init__.py       # モジュール初期化
│   │       └── standard.py       # 2ファイル処理（ContractList + Excel報告書）
│   │
│   ├── faith_sms/                # 🔔 フェイスSMS処理（準備中）
│   │   ├── __init__.py           # モジュール初期化
│   │   └── standard.py           # 標準版（将来実装予定）
│   │
│   └── ark_registration.py       # 📋 アーク新規登録データ変換
│
├── screenshots/                  # UIテスト用スクリーンショット
│   ├── 01_welcome.png            # ウェルカム画面
│   ├── debug_full_page.png       # デバッグ用フルページ
│   └── debug_sidebar.png         # デバッグ用サイドバー
│
├── shared/                       # 共通ユーティリティ（将来使用）
├── templates/                    # テンプレートファイル（将来使用）
│
└── テスト関連ファイル
    ├── ui_test.py                # Playwright UIテスト（基本版）
    ├── simple_ui_test.py         # 簡易UIテスト
    └── debug_ui_test.py          # デバッグ用UIテスト
```

## 🎯 業務カテゴリ構造

### 📞 オートコール処理
```
processors/
├── mirail_autocall/              # ミライル
│   ├── contract/                 # 契約者
│   │   ├── without10k.py         # 残債1万円・1万1千円除外
│   │   └── with10k.py            # 残債含む全件処理
│   ├── guarantor/                # 保証人
│   │   ├── without10k.py         # 残債1万円・1万1千円除外
│   │   └── with10k.py            # 残債含む全件処理
│   └── emergency_contact/        # 緊急連絡人
│       ├── without10k.py         # 残債1万円・1万1千円除外
│       └── with10k.py            # 残債含む全件処理
├── faith_autocall/               # フェイス
│   ├── contract/                 # 契約者
│   │   └── standard.py           # 委託先法人ID別処理
│   ├── guarantor/                # 保証人
│   │   └── standard.py           # 委託先法人ID別処理
│   └── emergency_contact/        # 緊急連絡人
│       └── standard.py           # 委託先法人ID別処理
└── plaza_autocall/               # プラザ
    ├── main/                     # 契約者（2ファイル処理）
    │   └── standard.py           # ContractList + Excel報告書
    ├── guarantor/                # 保証人（基本構造のみ）
    │   └── standard.py           # 基本構造のみ未実装
    └── contact/                  # 緊急連絡人（2ファイル処理）
        └── standard.py           # ContractList + Excel報告書
```

### 📱 SMS処理
```
processors/
└── faith_sms/                    # フェイスSMS（準備中）
    └── standard.py               # 将来実装
```

### 📋 データ変換処理
```
processors/
└── ark_registration.py          # アーク新規登録（111列統合CSV）
```

## 🔧 技術仕様

### 開発環境
- **言語**: Python 3.8+
- **フレームワーク**: Streamlit 1.28+
- **データ処理**: pandas 2.0+, chardet 5.2+
- **UIテスト**: Playwright 1.40+
- **対応OS**: Windows, macOS, Linux

### 主要機能
- 業務カテゴリ別階層メニューUI
- 自動エンコーディング判定（UTF-8, Shift_JIS, CP932）
- リアルタイム処理ログ表示
- データプレビュー・統計情報表示
- CP932エンコーディングCSV出力

## 📋 開発ルール

### 新機能追加時の手順
1. **業務カテゴリの特定**: オートコール/SMS/データ変換のどれか
2. **適切なディレクトリに配置**: processors/[category]/standard.py
3. **app.pyのUI更新**: メニュー追加・処理画面関数追加
4. **テスト実装**: 各プロセッサーの動作確認
5. **README.md更新**: 新機能の説明追加

### ファイル命名規則
- **プロセッサー**: `[system]_[type].py` (例: mirail_autocall/standard.py)
- **テスト**: `test_[target].py` または `[target]_test.py`
- **出力ファイル**: `MMDD[system]_[description].csv`

### インポート規則
```python
# ✅ 正しいインポート（全プロセッサー対応）
# ミライルオートコール（6種類）
from processors.mirail_autocall.contract.without10k import process_mirail_contract_without10k_data
from processors.mirail_autocall.contract.with10k import process_mirail_contract_with10k_data
from processors.mirail_autocall.guarantor.without10k import process_mirail_guarantor_without10k_data
from processors.mirail_autocall.guarantor.with10k import process_mirail_guarantor_with10k_data
from processors.mirail_autocall.emergency_contact.without10k import process_mirail_emergencycontact_without10k_data
from processors.mirail_autocall.emergency_contact.with10k import process_mirail_emergencycontact_with10k_data

# フェイスオートコール（3種類）
from processors.faith_autocall.contract.standard import process_faith_contract_data
from processors.faith_autocall.guarantor.standard import process_faith_guarantor_data
from processors.faith_autocall.emergency_contact.standard import process_faith_emergencycontact_data

# プラザオートコール（3種類）
from processors.plaza_autocall.main.standard import process_plaza_main_data
from processors.plaza_autocall.guarantor.standard import process_plaza_guarantor_data
from processors.plaza_autocall.contact.standard import process_plaza_contact_data

# その他
from processors.ark_registration import process_ark_data

# ❌ 間違ったインポート（旧構造）
from processors.mirail_autocall.standard import process_mirail_data
from processors.mirail_processor import process_mirail_data
```

## 🧪 テスト・品質保証

### UIテスト
- `simple_ui_test.py`: 基本的なスクリーンショット取得
- `ui_test.py`: Playwright詳細テスト
- `debug_ui_test.py`: デバッグ用テスト

### テスト実行コマンド
```bash
# UIテスト
python simple_ui_test.py

# プロセッサー動作確認（6種類例）
python -c "from processors.mirail_autocall.contract.without10k import process_mirail_contract_without10k_data; print('契約者without10k: OK')"
python -c "from processors.mirail_autocall.guarantor.with10k import process_mirail_guarantor_with10k_data; print('保証人with10k: OK')"
python -c "from processors.mirail_autocall.emergency_contact.without10k import process_mirail_emergencycontact_without10k_data; print('緊急連絡人without10k: OK')"

# アプリ起動
streamlit run app.py
# または
start.bat  # Windows環境
```

## 🔄 デプロイ・バージョン管理

### ブランチ戦略
- `main`: 安定版
- `feature/*`: 新機能開発用
- `hotfix/*`: 緊急修正用

### バージョン管理
- v2.1.0: プラザオートコール3種類実装完了
- v2.0.0: 業務カテゴリ別階層構造
- v1.1.0: ミライル（残債含む）追加
- v1.0.0: 初期統合アプリ

### コミットメッセージ形式
```
feat: [機能追加の説明]
fix: [バグ修正の説明]
refactor: [リファクタリングの説明]
docs: [ドキュメント更新の説明]
```

## 🚨 重要な注意事項

### 既存機能への影響
- **既存プロセッサーの変更時**: 必ず後方互換性を保つ
- **UIメニュー変更時**: 全てのプロセッサーの動作確認必須
- **ファイル移動時**: インポートパスの更新を忘れずに

### セキュリティ・品質
- **ローカル実行前提**: データを外部送信しない
- **エラーハンドリング**: ユーザーフレンドリーなメッセージ
- **ログ出力**: 処理過程の詳細記録

### 将来拡張対応
- プラザ保証人プロセッサーの実装完成が必要（現在基本構造のみ）
- フェイスSMS機能の実装枠を準備済み
- 新しい業務カテゴリ追加時は、この構造に従って拡張

## 📞 サポート・問い合わせ

- **GitHub Issues**: バグ報告・機能要望
- **開発者**: Takuya Tsuchiya
- **AI支援**: Claude Code (Anthropic)

---

**このCLAUDE.mdファイルは、プロジェクトの構造変更時に必ず更新してください。**