# ナップ新規登録 緊急連絡人１現住所3修正

## 要件定義

### 背景
ナップ新規登録のCSV出力において、CF列（緊急連絡人１現住所3）に「緊急連絡人住所アパート等」が入っていない。

### 現状
- CF列（緊急連絡人１現住所3）: `緊急連絡人住所３`のみ（アパート等が取得できていない）
- 原因: 「緊急連絡人住所アパート等」列が使用されていない

### 要件
- CF列（緊急連絡人１現住所3）: `緊急連絡人住所３　緊急連絡人住所アパート等`（全角空白で結合）
- 例: `マンションB　301号室`

### 受け入れ条件
- [ ] 緊急連絡人１現住所3に緊急連絡人住所３と緊急連絡人住所アパート等が全角空白で結合されて出力される
- [ ] 両方に値がある場合のみ全角空白を挿入
- [ ] 片方が空の場合は余計な空白が入らない
- [ ] 既存の他の列に影響がない

---

## 設計

### 変更対象ファイル
- `processors/nap_registration.py`
- `tests/test_nap_registration.py`

### 変更箇所
`processors/nap_registration.py` の `map_emergency_contact_info` メソッド（698-699行目付近）

#### Before
```python
if "緊急連絡人住所３" in excel_df.columns:
    output_df["緊急連絡人１現住所3"] = excel_df["緊急連絡人住所３"]
```

#### After
```python
# 緊急連絡人１現住所3 = 緊急連絡人住所３ + 全角空白 + 緊急連絡人住所アパート等
addr3 = excel_df["緊急連絡人住所３"].fillna("") if "緊急連絡人住所３" in excel_df.columns else pd.Series([""] * len(excel_df))
apt = excel_df["緊急連絡人住所アパート等"].fillna("") if "緊急連絡人住所アパート等" in excel_df.columns else pd.Series([""] * len(excel_df))
# 両方に値がある場合のみ全角空白を挿入
combined = (addr3.astype(str) + "　" + apt.astype(str))
output_df["緊急連絡人１現住所3"] = combined.str.replace(r'^　+|　+$', '', regex=True)
```

### テストケース
1. 両方に値がある場合 → 全角空白で結合
2. 住所３のみの場合 → そのまま（末尾に空白なし）
3. アパート等のみの場合 → そのまま（先頭に空白なし）
4. 両方空の場合 → 空文字列

---

## 実装タスクリスト

- [ ] 開発ブランチ作成: `fix/nap-emergency-contact-address3`
- [ ] テスト作成（Red）
  - [ ] 緊急連絡人１現住所3が全角空白で結合されることを検証するテスト
  - [ ] 既存テストの期待値修正
- [ ] 実装（Green）
  - [ ] `map_emergency_contact_info`の緊急連絡人１現住所3結合ロジック修正
- [ ] 全テスト実行・確認
- [ ] コミット
- [ ] PR作成

---

## 備考
- ①（契約者現住所3）③（保証人１住所3）と同じパターンの修正
- 入力Excelの列名は「緊急連絡人住所アパート等」（52列目）
