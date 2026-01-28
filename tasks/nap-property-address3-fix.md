# ナップ新規登録 物件住所3修正

## 要件定義

### 背景
ナップ新規登録のCSV出力において、Q列（物件住所3）に物件名と部屋番号が含まれてしまっている。

### 現状
- Q列（物件住所3）: `番地 + 物件名 + 部屋番号`
- 例: `栄町20-2ﾕﾅｲﾄ横浜ｱﾙﾎﾞﾗﾝ106号室`

### 要件
- Q列（物件住所3）: `番地のみ`
- 例: `栄町20-2`

### 受け入れ条件
- [ ] 物件住所3に番地（物件住所３列）のみが出力される
- [ ] 物件名と部屋番号は含まれない
- [ ] 既存の他の列に影響がない

---

## 設計

### 変更対象ファイル
- `processors/nap_registration.py`
- `tests/test_nap_registration.py`

### 変更箇所
`processors/nap_registration.py` の `map_property_info` メソッド（586-590行目付近）

#### Before
```python
# 物件住所3 = 物件住所３ + 物件名 + 部屋番号
addr3 = excel_df["物件住所３"].fillna("") if "物件住所３" in excel_df.columns else ""
prop_name = excel_df["物件名"].fillna("") if "物件名" in excel_df.columns else ""
room = excel_df["部屋番号"].fillna("") if "部屋番号" in excel_df.columns else ""
output_df["物件住所3"] = addr3 + prop_name + room
```

#### After
```python
# 物件住所3 = 物件住所３のみ（番地まで）
addr3 = excel_df["物件住所３"].fillna("") if "物件住所３" in excel_df.columns else ""
output_df["物件住所3"] = addr3
```

### テストケース
1. 物件住所３に値がある場合 → その値のみ出力
2. 物件住所３が空の場合 → 空文字列
3. 物件住所３列が存在しない場合 → 空文字列

---

## 実装タスクリスト

- [x] 開発ブランチ作成: `fix/nap-property-address3`
- [x] テスト作成（Red）
  - [x] 物件住所3が番地のみになることを検証するテスト
  - [x] 既存テストの期待値修正
- [x] 実装（Green）
  - [x] `map_property_info`の物件住所3結合ロジック修正
- [x] 全テスト実行・確認（51テスト成功）
- [ ] コミット
- [ ] PR作成

---

## 備考
- 物件名と部屋番号は別の列（L列、M列）に既に出力されている
- 今回の修正は物件住所3の冗長なデータを削除するもの
