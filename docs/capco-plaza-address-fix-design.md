# カプコ・プラザ新規登録 住所分割バグ修正 - 設計書

## 修正方針

正規表現ベースの住所分割を、辞書ベースの `AddressSplitter` クラスに置き換える。

### 既存の共通基盤

- **AddressSplitter**: `processors/common/address_splitter.py`
- **市区町村辞書**: `data/municipalities.json`（全国1,741+市区町村を収録）
- **使用実績**: アーク、GB、IOG新規登録で稼働中

## 修正対象ファイル

### 1. `processors/capco_registration.py`

#### インポート追加
```python
from processors.common.address_splitter import AddressSplitter
```

#### DataConverter.__init__ 修正
```python
def __init__(self):
    self.logger = logging.getLogger(__name__)
    self.address_splitter = AddressSplitter()
```

#### split_address メソッド置き換え（372-438行）

**変更前**: 正規表現 + ハードコード特別処理（市川市・市原市・町田市）
**変更後**: AddressSplitter.split_address() に委譲

- シグネチャ維持: `split_address(self, full_address, building_name, room_name)`
- 戻り値維持: `{"prefecture", "city", "address3", "property_address3"}`
- address3 の組み立てロジック（remaining + 建物名 + 部屋名）は維持

### 2. `processors/plaza_registration.py`

#### インポート追加
```python
from processors.common.address_splitter import AddressSplitter
```

#### DataConverter.__init__ 修正
```python
def __init__(self):
    self.logger = logging.getLogger(__name__)
    self.address_splitter = AddressSplitter()
```

#### split_address メソッド置き換え（470-554行）

**変更前**: 正規表現 + ハードコード特別処理 + 東京23区リスト
**変更後**: AddressSplitter.split_address() に委譲

- シグネチャ維持: `split_address(self, address)`
- 戻り値維持: `{"prefecture", "city", "remaining"}`

## 不要コードの整理

- `CapcoConfig.PREFECTURES`: DataConverter内でのみ使用 → 削除候補（他の参照を確認）
- `PlazaConfig.PREFECTURES`: DataConverter内でのみ使用 → 削除候補
- 東京23区ハードコードリスト: AddressSplitterが辞書でカバー → 削除
- 市川市・市原市・町田市のハードコード: AddressSplitterが辞書でカバー → 削除

## テスト計画

### 新規: `tests/test_capco_registration_address.py`

| テストケース | 入力 | 期待: prefecture | 期待: city |
|---|---|---|---|
| 村を含む市（東村山市） | 東京都東村山市恩多町5-25-2 | 東京都 | 東村山市 |
| 村を含む市（武蔵村山市） | 東京都武蔵村山市学園3-38-1 | 東京都 | 武蔵村山市 |
| 村を含む市（羽村市） | 東京都羽村市羽西1-10-10 | 東京都 | 羽村市 |
| 町を含む市（町田市） | 東京都町田市成瀬が丘1-10-24 | 東京都 | 町田市 |
| 市で始まる市（市川市） | 千葉県市川市八幡2-1-1 | 千葉県 | 市川市 |
| 市で始まる市（市原市） | 千葉県市原市五井中央西1-1-25 | 千葉県 | 市原市 |
| 政令指定都市 | 神奈川県横浜市港北区新吉田東6-61-10 | 神奈川県 | 横浜市港北区 |
| 通常の市 | 神奈川県川崎市高津区二子3-33-28 | 神奈川県 | 川崎市高津区 |
| 建物名・部屋名の組み立て | （address3, property_address3の検証） | - | - |
| 空住所 | "" | "" | "" |

### 追加: `tests/test_plaza_registration.py`

| テストケース | 入力 | 期待: city |
|---|---|---|
| 東村山市 | 東京都東村山市恩多町5-25-2 | 東村山市 |
| 武蔵村山市 | 東京都武蔵村山市学園3-38-1 | 武蔵村山市 |
| 羽村市 | 東京都羽村市羽西1-10-10 | 羽村市 |
| 通常の市 | 神奈川県相模原市緑区町屋3-2-2 | 相模原市緑区 |

## 検証方法

```bash
# テスト実行
pytest tests/test_capco_registration_address.py -v
pytest tests/test_plaza_registration.py -v

# コード品質
ruff check .
ruff format .

# 実データ検証（提供されたCSVで4件の村含み住所が正しく処理されることを確認）
```

---
作成日: 2026-02-16
