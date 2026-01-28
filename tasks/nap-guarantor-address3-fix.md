# ナップ新規登録 保証人１住所3修正

## 要件定義

### 背景
ナップ新規登録のCSV出力において、BM列（保証人１住所3）に「連保人1住所アパート等」が入っていない。

### 現状
- BM列（保証人１住所3）: `連保人1住所３`のみ（アパート等が取得できていない）
- 原因: 列名が「連保人住所アパート等」（誤）となっており、正しい列名「連保人1住所アパート等」と一致しない

### 要件
- BM列（保証人１住所3）: `連保人1住所３　連保人1住所アパート等`（全角空白で結合）
- 例: `マンションA　201号室`

### 受け入れ条件
- [x] 保証人１住所3に連保人1住所３と連保人1住所アパート等が全角空白で結合されて出力される
- [x] 両方に値がある場合のみ全角空白を挿入
- [x] 片方が空の場合は余計な空白が入らない
- [x] 既存の他の列に影響がない

---

## 設計

### 変更対象ファイル
- `processors/nap_registration.py`
- `tests/test_nap_registration.py`

### 変更箇所
`processors/nap_registration.py` の `map_guarantor_info` メソッド（658-661行目付近）

#### Before
```python
# 保証人住所3 = 連保人1住所３ + 連保人住所アパート等（該当列がある場合）
addr3 = excel_df["連保人1住所３"].fillna("") if "連保人1住所３" in excel_df.columns else ""
apt = excel_df["連保人住所アパート等"].fillna("") if "連保人住所アパート等" in excel_df.columns else ""
output_df["保証人１住所3"] = addr3 + apt
```

#### After
```python
# 保証人住所3 = 連保人1住所３ + 全角空白 + 連保人1住所アパート等
addr3 = excel_df["連保人1住所３"].fillna("") if "連保人1住所３" in excel_df.columns else pd.Series([""] * len(excel_df))
apt = excel_df["連保人1住所アパート等"].fillna("") if "連保人1住所アパート等" in excel_df.columns else pd.Series([""] * len(excel_df))
# 両方に値がある場合のみ全角空白を挿入
combined = (addr3.astype(str) + "　" + apt.astype(str))
output_df["保証人１住所3"] = combined.str.replace(r'^　+|　+$', '', regex=True)
```

### テストケース
1. 両方に値がある場合 → 全角空白で結合
2. 住所３のみの場合 → そのまま（末尾に空白なし）
3. アパート等のみの場合 → そのまま（先頭に空白なし）
4. 両方空の場合 → 空文字列

---

## 実装タスクリスト

- [ ] 開発ブランチ作成: `fix/nap-guarantor-address3`
- [ ] テスト作成（Red）
  - [ ] 保証人１住所3が全角空白で結合されることを検証するテスト
  - [ ] 既存テストの期待値修正
- [ ] 実装（Green）
  - [ ] `map_guarantor_info`の保証人１住所3結合ロジック修正
  - [ ] 列名を「連保人住所アパート等」→「連保人1住所アパート等」に修正
- [ ] 全テスト実行・確認
- [ ] コミット
- [ ] PR作成

---

## 備考
- ①（契約者現住所3）と同じパターンの修正
- 入力Excelの列名は「連保人1住所アパート等」（40列目）
