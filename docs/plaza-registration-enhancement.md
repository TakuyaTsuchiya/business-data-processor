# プラザ新規登録 微修正 - 要件定義書・設計書

## 概要

**対象機能:** プラザ新規登録プロセッサ
**対象ファイル:** `processors/plaza_registration.py`
**作成日:** 2026-01-29
**ブランチ:** `feature/plaza-registration-enhancement`

---

## 要件定義

### 背景
クライアントからプラザ新規登録処理に対して2点の微修正リクエストあり。

### 要件1: 氏名のアルファベット変換

#### 対象フィールド
| 出力列 | 出力列名 | 入力列 | 入力列名 |
|--------|----------|--------|----------|
| B列 | 契約者氏名 | G列 | 氏名（漢字） |
| BF列 | 保証人１氏名 | AG列 | 連帯保証人　名（漢字） |
| BZ列 | 緊急連絡人１氏名 | AK列 | 緊急連絡人　氏名（漢字） |

#### 変換ルール
- **条件:** 氏名全体がアルファベット（全角・半角）のみで構成されている場合のみ変換
- **変換内容:** 全角アルファベット → 半角大文字
- **例:** `ＶＵＨＡＩＮＩＮＨ` → `VUHAININH`
- **備考:** 日本語との混在は通常ない前提

#### テストケース
| 入力 | 期待出力 | 備考 |
|------|----------|------|
| `ＶＵＨＡＩＮＩＮＨ` | `VUHAININH` | 全角アルファベットのみ |
| `NGUYEN` | `NGUYEN` | 既に半角大文字 |
| `nguyen` | `NGUYEN` | 半角小文字 → 大文字化 |
| `Ａｂｃ` | `ABC` | 全角小文字混在 |
| `田中太郎` | `田中太郎` | 日本語（変換しない） |
| `田中TARO` | `田中TARO` | 混在（変換しない） |
| `""` | `""` | 空文字 |

### 要件2: 引継情報にメールアドレス追加

#### 入出力マッピング
| 項目 | 詳細 |
|------|------|
| 入力元 | プラザCSVのN列「メール」（インデックス13） |
| 出力先 | 出力CSVのK列「引継情報」 |

#### フォーマット
```
{処理日}　プラザ一括登録　●メールアドレス：{メール}
```
- 区切り文字: 全角スペース（`\u3000`）
- 処理日フォーマット: `YYYY/M/D`（例: `2026/1/29`）

#### メールアドレスが空の場合
```
{処理日}　プラザ一括登録　●メールアドレス：
```
- 空の場合も「●メールアドレス：」は出力する

#### テストケース
| 処理日 | メール | 期待出力 |
|--------|--------|----------|
| 2026/1/13 | test@example.com | `2026/1/13　プラザ一括登録　●メールアドレス：test@example.com` |
| 2026/1/13 | (空) | `2026/1/13　プラザ一括登録　●メールアドレス：` |

---

## 設計

### 新規メソッド

#### `DataConverter.is_alphabet_only(text: str) -> bool`
文字列がアルファベット（全角・半角）のみで構成されているか判定する。

```python
def is_alphabet_only(self, text: str) -> bool:
    """文字列がアルファベット（全角・半角）のみで構成されているか判定"""
    if not text:
        return False
    # NFKCで正規化後、アルファベットのみかチェック
    normalized = unicodedata.normalize('NFKC', text)
    return normalized.isalpha() and normalized.isascii()
```

#### `DataConverter.convert_fullwidth_alpha_to_halfwidth_upper(text: str) -> str`
全角アルファベットを半角大文字に変換する。

```python
def convert_fullwidth_alpha_to_halfwidth_upper(self, text: str) -> str:
    """全角アルファベットを半角大文字に変換"""
    if not text:
        return ""
    normalized = unicodedata.normalize('NFKC', text)
    return normalized.upper()
```

### 氏名変換ロジック

```python
# 氏名を取得してスペース除去
name = self.converter.remove_all_spaces(
    self.converter.safe_str_convert(row[cols[index]])
)
# アルファベットのみの場合は半角大文字に変換
if self.converter.is_alphabet_only(name):
    name = self.converter.convert_fullwidth_alpha_to_halfwidth_upper(name)
output_row["氏名フィールド"] = name
```

### 引継情報フォーマット

```python
today = datetime.now().strftime("%Y/%-m/%-d")
email = self.converter.safe_str_convert(row[cols[13]])  # N列「メール」
output_row["引継情報"] = f"{today}　プラザ一括登録　●メールアドレス：{email}"
```

---

## 実装タスクリスト

### Phase 1: テスト作成（Red）
- [ ] テストファイル作成: `tests/test_plaza_registration.py`
- [ ] `is_alphabet_only`メソッドのテスト作成
- [ ] `convert_fullwidth_alpha_to_halfwidth_upper`メソッドのテスト作成
- [ ] 氏名変換統合テスト作成（契約者・保証人・緊急連絡人）
- [ ] 引継情報フォーマットテスト作成（メールあり・なし両方）

### Phase 2: 実装（Green）
- [ ] `is_alphabet_only`メソッド追加
- [ ] `convert_fullwidth_alpha_to_halfwidth_upper`メソッド追加
- [ ] 契約者氏名の変換処理追加（行344-346付近）
- [ ] 保証人１氏名の変換処理追加（行507-510付近）
- [ ] 緊急連絡人１氏名の変換処理追加（行536-539付近）
- [ ] 引継情報へのメールアドレス追加（行385-387付近）

### Phase 3: リファクタリング
- [ ] コードの重複排除（氏名変換処理の共通化検討）
- [ ] テストカバレッジ確認
- [ ] 動作確認

---

## 変更箇所まとめ

| ファイル | 行番号（目安） | 変更内容 |
|----------|----------------|----------|
| `plaza_registration.py` | 223-237 | 新メソッド2つ追加 |
| `plaza_registration.py` | 344-346 | 契約者氏名にアルファベット変換追加 |
| `plaza_registration.py` | 385-387 | 引継情報にメールアドレス追加 |
| `plaza_registration.py` | 507-510 | 保証人１氏名にアルファベット変換追加 |
| `plaza_registration.py` | 536-539 | 緊急連絡人１氏名にアルファベット変換追加 |
| `tests/test_plaza_registration.py` | 新規 | テストコード |
