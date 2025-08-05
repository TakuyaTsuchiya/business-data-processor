# CLAUDE.md

このファイルはClaude Code (claude.ai/code) がこのリポジトリで作業する際のガイドです。

## 🌟 プロジェクト概要

**Business Data Processor v2.3.0** - 革新的UIシステム実装完了版

不動産管理業務における契約データの変換・統合処理を自動化するStreamlit製のWebアプリケーション。
複数の業務システム（オートコール用CSV加工、SMS送信用CSV加工、新規登録用CSV加工）を統合し、
プルダウンレスの常時表示階層メニューによる革新的で直感的なUIを提供。

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
│   ├── faith_sms/                # 📱 フェイスSMS処理
│   │   ├── __init__.py           # モジュール初期化
│   │   ├── vacated_contract.py   # 退去済み契約者SMS処理（実装済み）
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

#### 🏢 ミライル（6種類）
```
processors/mirail_autocall/
├── contract/                     # 契約者オートコール
│   ├── without10k.py             # 残債除外版
│   │   📋 フィルタ条件: 委託先法人ID(空白&5), 入金予定日(前日以前), 
│   │                  回収ランク(弁護士介入除外), 
│   │                  残債除外(CD=1かつ10k/11k除外), TEL携帯必須
│   └── with10k.py                # 残債含む全件処理版
│       📋 フィルタ条件: 委託先法人ID(空白&5), 入金予定日(前日以前),
│                      回収ランク(弁護士介入除外), 残債フィルタなし, TEL携帯必須
├── guarantor/                    # 保証人オートコール
│   ├── without10k.py             # 残債除外版（TEL携帯.1使用）
│   │   📋 フィルタ条件: 委託先法人ID(空白&5), 入金予定日(前日以前), 
│   │                  回収ランク(弁護士介入除外), 
│   │                  残債除外(CD=1かつ10k/11k除外), 入金予定金額(2,3,5除外), TEL携帯.1必須
│   └── with10k.py                # 残債含む全件処理版（TEL携帯.1使用）
│       📋 フィルタ条件: 委託先法人ID(空白&5), 入金予定日(前日以前),
│                      回収ランク(弁護士介入除外), 残債フィルタなし, 入金予定金額(2,3,5除外), TEL携帯.1必須
└── emergency_contact/            # 緊急連絡人オートコール
    ├── without10k.py             # 残債除外版（TEL携帯.2使用）
    └── with10k.py                # 残債含む全件処理版（TEL携帯.2使用）
```

#### 📱 フェイス（3種類）
```
processors/faith_autocall/
├── contract/standard.py          # 契約者オートコール
├── guarantor/standard.py         # 保証人オートコール  
└── emergency_contact/standard.py # 緊急連絡人オートコール
📋 共通フィルタ条件: 委託先法人ID(1-4), 入金予定日(前日以前),
                   回収ランク(弁護士介入除外), 入金予定金額(2,3,5除外), 残債フィルタなし
```

#### 🏪 プラザ（3種類）
```
processors/plaza_autocall/
├── main/standard.py              # 契約者オートコール（2ファイル処理）
├── guarantor/standard.py         # 保証人オートコール（2ファイル処理）
└── contact/standard.py           # 緊急連絡人オートコール（2ファイル処理）
📋 共通フィルタ条件: 延滞額合計(0,2,3,5円除外), TEL無効除外,
                   回収ランク(督促停止・弁護士介入除外)
📂 必要ファイル: ContractList + Excel報告書
```

### 📱 SMS処理
```
processors/faith_sms/
├── vacated_contract.py           # 退去済み契約者SMS（実装済み）
│   📋 フィルタ条件: 入居ステータス(退去済み), 委託先法人ID(1-4), TEL携帯必須
└── standard.py                   # 標準版（将来実装予定）
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

# フェイスSMS（1種類）
from processors.faith_sms.vacated_contract import process_faith_sms_vacated_contract_data

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
- v2.4.1: ファイルアップロード上限30MB制限実装（セキュリティ・パフォーマンス向上）
- v2.4.0: アーク残債更新機能統合完了（💰 残債の更新用CSV加工カテゴリ追加）
- v2.3.0: 革新的UIシステム実装完了（プルダウンレス常時表示メニュー、固定ヘッダー、コンパクトボタン配置）
- v2.2.0: フェイスSMS退去済み契約者実装完了
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

## 🚀 v2.3.0 革新的UIシステム改善詳細

### 🎯 主要UI改善点

**1. プルダウンレス常時表示メニュー**
- ❌ 従来のプルダウン選択 → ✅ 階層化された常時表示ボタンメニュー
- 全選択肢が一目で確認可能
- クリック一回で直接機能選択

**2. 統一された命名規則**
- `📞 オートコール用CSV加工`
- `📱 SMS送信用CSV加工` 
- `📋 新規登録用CSV加工`
- 技術用語を分かりやすい日本語に統一（without10k → 10,000円を除外するパターン）

**3. 固定ヘッダーシステム**
- スクロール時もタイトルが常時表示
- ナビゲーションの迷いを解消

**4. コンパクトボタン配置**
- ボタン間余白を極小化（0.05rem）
- より多くの選択肢を画面内に表示
- 効率的な空間利用

**5. サイドバー固定**
- 折りたたみ機能を無効化
- 誤操作による隠蔽を防止
- 常に400px幅で安定表示

### 🔧 技術的実装詳細

**CSS最適化**:
```css
/* タイトル固定 */
.main .block-container > div:first-child {
    position: sticky !important;
    top: 0 !important;
    background-color: white !important;
    z-index: 999 !important;
}

/* コンパクトボタン */
.stSidebar .stButton > button {
    margin: 0.1rem !important;
    padding: 0.2rem 0.5rem !important;
}

/* サイドバー固定 */
section[data-testid="stSidebar"] {
    width: 400px !important;
    min-width: 400px !important;
}
```

**階層構造**:
```
📞 オートコール用CSV加工
├── ミライル用オートコール (6パターン)
├── フェイス用オートコール (3パターン) 
└── プラザ用オートコール (3パターン)
📱 SMS送信用CSV加工
└── フェイス_契約者_退去済みSMS用
📋 新規登録用CSV加工
└── アーク新規登録
```

## 🚨 重要な注意事項

### 【最重要】アーク新規登録テンプレートヘッダー厳守ルール
⚠️ **絶対に変更禁止** ⚠️
- **最終アウトプットCSVファイルのヘッダー情報**: 一切変更・編集禁止
- **列の位置・順序**: 絶対に変えない
- **列名**: 1文字たりとも変更しない（全角・半角数字の区別も厳守）
- **列数**: 111列から増減させない

**正確なテンプレートヘッダー（111列）**:
```
引継番号,契約者氏名,契約者カナ,契約者生年月日,契約者TEL自宅,契約者TEL携帯,契約者現住所郵便番号,契約者現住所1,契約者現住所2,契約者現住所3,引継情報,物件名,部屋番号,物件住所郵便番号,物件住所1,物件住所2,物件住所3,入居ステータス,滞納ステータス,受託状況,月額賃料,管理費,共益費,水道代,駐車場代,その他費用1,その他費用2,敷金,礼金,回収口座金融機関CD,回収口座金融機関名,回収口座支店CD,回収口座支店名,回収口座種類,回収口座番号,回収口座名義,契約種類,管理受託日,契約確認日,退去済手数料,入居中滞納手数料,入居中正常手数料,管理前滞納額,更新契約手数料,退去手続き（実費）,初回振替月,保証開始日,クライアントCD,パートナーCD,契約者勤務先名,契約者勤務先カナ,契約者勤務先TEL,勤務先業種,契約者勤務先郵便番号,契約者勤務先住所1,契約者勤務先住所2,契約者勤務先住所3,保証人１氏名,保証人１カナ,保証人１契約者との関係,保証人１生年月日,保証人１郵便番号,保証人１住所1,保証人１住所2,保証人１住所3,保証人１TEL自宅,保証人１TEL携帯,保証人２氏名,保証人２カナ,保証人２契約者との関係,保証人２生年月日,保証人２郵便番号,保証人２住所1,保証人２住所2,保証人２住所3,保証人２TEL自宅,保証人２TEL携帯,緊急連絡人１氏名,緊急連絡人１カナ,緊急連絡人１契約者との関係,緊急連絡人１郵便番号,緊急連絡人１現住所1,緊急連絡人１現住所2,緊急連絡人１現住所3,緊急連絡人１TEL自宅,緊急連絡人１TEL携帯,緊急連絡人２氏名,緊急連絡人２カナ,緊急連絡人２契約者との関係,緊急連絡人２郵便番号,緊急連絡人２現住所1,緊急連絡人２現住所2,緊急連絡人２現住所3,緊急連絡人２TEL自宅,緊急連絡人２TEL携帯,保証入金日,保証入金者,引落銀行CD,引落銀行名,引落支店CD,引落支店名,引落預金種別,引落口座番号,引落口座名義,解約日,管理会社,委託先法人ID,,,,登録フラグ
```

**特に注意すべき点**:
- 「保証人１」「保証人２」「緊急連絡人１」「緊急連絡人２」は全角数字
- 「契約確認日」（「申請者確認日」ではない）
- 空列（108-110番目）も含めて111列きっかり
- 手戻り防止のため、このルールに違反する修正は一切行わない

### CSVファイル処理時の厳守事項
- **文字化け対応**: CSVファイルを読み込んで文字化けが発生した場合、必ずその場で文字エンコーディング（UTF-8、Shift_JIS、CP932など）を試して解消してから作業を進めること
- **推測での補完禁止**: 文字化けしたデータを推測で補完することは絶対に禁止。マッピングエラーの原因となる
- **エンコーディング優先順位**: cp932 → shift_jis → utf-8-sig → utf-8 → iso-2022-jp → euc-jp の順で試行
- **ファイルへの一時出力**: 文字化けが解消できない場合は、一時的にファイルに出力してから読み込み直すなどの対策を取る

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