# ナップ新規登録機能 実装タスクリスト

## フェーズ1: セットアップ

### Task 1.1: ブランチ作成
- [ ] mainブランチから`feature/nap-registration`ブランチを作成
- [ ] ブランチをチェックアウト

### Task 1.2: テストデータ準備
- [ ] ContractListモックデータを作成
  - 委託先法人ID=5のデータを含む
  - 委託先法人ID≠5のデータも含む（フィルタテスト用）
  - 引継番号のサンプルデータ

## フェーズ2: プロセッサー実装（TDD）

### Task 2.1: NapConfig実装
- [ ] テスト: `test_nap_config_output_columns_count`（111列確認）
- [ ] テスト: `test_nap_config_fixed_values`（固定値確認）
- [ ] 実装: `NapConfig`クラス
  - OUTPUT_COLUMNS（111列定義）
  - FIXED_VALUES（固定値辞書）
  - COLUMN_MAPPING（列マッピング）
  - TARGET_CORPORATION_ID
  - EXCEL_SKIPROWS
- [ ] Commit: "feat: add NapConfig class with 111-column definition"

### Task 2.2: FileReader実装
- [ ] テスト: `test_read_excel_file_success`
- [ ] テスト: `test_read_excel_file_skiprows`
- [ ] テスト: `test_read_excel_file_invalid`
- [ ] テスト: `test_read_csv_file_cp932`
- [ ] テスト: `test_read_csv_file_utf8`
- [ ] テスト: `test_read_csv_file_shift_jis`
- [ ] 実装: `FileReader`クラス
  - `read_excel_file()`
  - `read_csv_file()`
- [ ] Commit: "feat: add FileReader with Excel and CSV support"

### Task 2.3: DuplicateChecker実装
- [ ] テスト: `test_filter_contract_list_with_target_id_5`
- [ ] テスト: `test_filter_contract_list_empty_result`
- [ ] テスト: `test_filter_contract_list_missing_column`
- [ ] テスト: `test_check_duplicates_no_duplicates`
- [ ] テスト: `test_check_duplicates_all_duplicates`
- [ ] テスト: `test_check_duplicates_partial_duplicates`
- [ ] 実装: `DuplicateChecker`クラス
  - `filter_contract_list()`
  - `check_duplicates()`
- [ ] Commit: "feat: add DuplicateChecker with corporation ID filtering"

### Task 2.4: DataMapper実装（契約者情報）
- [ ] テスト: `test_create_output_dataframe_structure`
- [ ] テスト: `test_map_contractor_info_basic_fields`
- [ ] テスト: `test_map_contractor_info_missing_fields`
- [ ] 実装: `DataMapper`クラス（パート1）
  - `create_output_dataframe()`
  - `map_contractor_info()`
- [ ] Commit: "feat: add DataMapper with contractor info mapping"

### Task 2.5: DataMapper実装（物件情報）
- [ ] テスト: `test_map_property_info_complete`
- [ ] テスト: `test_map_property_info_missing_address`
- [ ] 実装: `DataMapper`（パート2）
  - `map_property_info()`
- [ ] Commit: "feat: add property info mapping to DataMapper"

### Task 2.6: DataMapper実装（保証人情報）
- [ ] テスト: `test_map_guarantor_info_complete`
- [ ] テスト: `test_map_guarantor_info_partial`
- [ ] テスト: `test_map_guarantor_info_empty`
- [ ] 実装: `DataMapper`（パート3）
  - `map_guarantor_info()`（連帯保証人→保証人１）
- [ ] Commit: "feat: add guarantor info mapping (連帯保証人→保証人１)"

### Task 2.7: DataMapper実装（緊急連絡人情報）
- [ ] テスト: `test_map_emergency_contact_info_complete`
- [ ] テスト: `test_map_emergency_contact_info_partial`
- [ ] 実装: `DataMapper`（パート4）
  - `map_emergency_contact_info()`
- [ ] Commit: "feat: add emergency contact info mapping"

### Task 2.8: DataMapper実装（固定値適用）
- [ ] テスト: `test_apply_fixed_values_all_fields`
- [ ] テスト: `test_apply_fixed_values_management_date`
- [ ] 実装: `DataMapper`（パート5）
  - `apply_fixed_values()`
- [ ] Commit: "feat: add fixed values application"

### Task 2.9: メイン処理関数実装
- [ ] テスト: `test_process_nap_data_end_to_end`
- [ ] テスト: `test_process_nap_data_no_new_records`
- [ ] テスト: `test_process_nap_data_invalid_input`
- [ ] 実装: `process_nap_data()`関数
- [ ] Commit: "feat: add process_nap_data main function"

## フェーズ3: 画面・サービス層実装

### Task 3.1: 画面モジュール実装
- [ ] ファイル作成: `screens/registration/nap.py`
- [ ] 実装: `show_nap_registration()`関数
  - ScreenConfigを使用
  - フィルタ条件表示
  - 2ファイルアップローダー
- [ ] Commit: "feat: add nap registration screen module"

### Task 3.2: サービス層更新
- [ ] 編集: `services/registration.py`
  - `process_nap_data`をインポート
  - `__all__`に追加
- [ ] Commit: "feat: export process_nap_data from service layer"

### Task 3.3: アプリ統合（app.py）
- [ ] 編集: `app.py`
  - `show_nap_registration`をインポート
  - プロセッサーマッピングに追加
- [ ] Commit: "feat: integrate nap registration into app.py"

### Task 3.4: サイドバー統合
- [ ] 編集: `components/sidebar.py`
  - 「その他」→「新規登録」セクションに「ナップ新規登録」ボタン追加
- [ ] Commit: "feat: add nap registration button to sidebar"

## フェーズ4: テスト・動作確認

### Task 4.1: 単体テスト実行
- [ ] 全ユニットテストを実行
- [ ] カバレッジ確認（目標: 80%以上）
- [ ] 失敗したテストを修正

### Task 4.2: 統合テスト
- [ ] 実データでE2Eテスト
  - 入力: `/Users/tchytky/Downloads/ミライル様　11月分依頼データ.xlsx`
  - ContractList: モックデータ
- [ ] 出力CSVの検証
  - 111列確認
  - 固定値確認
  - マッピング確認
- [ ] Commit: "test: add integration test with real data"

### Task 4.3: 手動動作確認
- [ ] Streamlitアプリ起動
- [ ] ナップ新規登録画面を開く
- [ ] ファイルアップロード動作確認
- [ ] 処理実行・結果表示確認
- [ ] CSVダウンロード確認
- [ ] エラーケースの確認

## フェーズ5: ドキュメント・PR作成

### Task 5.1: ドキュメント更新
- [ ] CLAUDE.mdに機能追加を記載
- [ ] README更新（必要であれば）
- [ ] Commit: "docs: update documentation for nap registration"

### Task 5.2: PR作成
- [ ] すべての変更をpush
- [ ] PR作成: `feature/nap-registration` → `main`
- [ ] PR説明文を記載
  - 機能概要
  - 実装内容
  - テスト結果
  - スクリーンショット（可能であれば）
- [ ] レビュアー指定（/reviewコマンド用）

### Task 5.3: サブエージェントレビュー
- [ ] `/review`コマンドでレビュー依頼
- [ ] レビュー結果を確認
- [ ] 指摘事項に対応
- [ ] 対応後、再度レビュー依頼（必要に応じて）

## コミット方針

### コミットメッセージフォーマット
```
<type>: <subject>

<body>（オプション）
```

### Type
- `feat`: 新機能
- `test`: テスト追加・修正
- `fix`: バグ修正
- `refactor`: リファクタリング
- `docs`: ドキュメント更新
- `chore`: その他

### コミット粒度
- 1つのテスト＋実装 = 1コミット（TDD）
- 大きな機能は複数コミットに分割
- 動作する状態でコミット

## チェックリスト

### コード品質
- [ ] 型ヒントを適切に使用
- [ ] docstringを記載
- [ ] エラーハンドリングが適切
- [ ] ログ出力が適切
- [ ] マジックナンバーなし（定数化）

### テスト品質
- [ ] すべてのpublicメソッドにテストがある
- [ ] 正常系・異常系の両方をテスト
- [ ] エッジケースのテスト

### 統合品質
- [ ] 既存機能への影響がない
- [ ] コーディングスタイルが統一されている
- [ ] CLAUDE.mdのルールに従っている

## 見積もり

- フェーズ1: 0.5時間
- フェーズ2: 4-6時間（TDD込み）
- フェーズ3: 1-2時間
- フェーズ4: 1-2時間
- フェーズ5: 0.5-1時間

**合計**: 7-11.5時間
