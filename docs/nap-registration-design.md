# ナップ新規登録機能 設計書

## 1. アーキテクチャ

```
┌─────────────────────────────────────────────────────────┐
│ Streamlit UI Layer (screens/registration/nap.py)       │
│ - ファイルアップロード                                    │
│ - 処理実行ボタン                                         │
│ - 結果表示                                               │
└─────────────────┬───────────────────────────────────────┘
                  │
                  ↓
┌─────────────────────────────────────────────────────────┐
│ Service Layer (services/registration.py)               │
│ - process_nap_data()                                    │
└─────────────────┬───────────────────────────────────────┘
                  │
                  ↓
┌─────────────────────────────────────────────────────────┐
│ Processor Layer (processors/nap_registration.py)       │
│ ┌─────────────────────────────────────────────────────┐ │
│ │ NapConfig                                           │ │
│ │ - OUTPUT_COLUMNS (111列定義)                        │ │
│ │ - FIXED_VALUES (固定値)                             │ │
│ │ - INPUT_COLUMNS (Excel列定義)                       │ │
│ └─────────────────────────────────────────────────────┘ │
│ ┌─────────────────────────────────────────────────────┐ │
│ │ FileReader                                          │ │
│ │ - read_excel_file() : Excelファイル読み込み         │ │
│ │ - read_csv_file() : ContractList読み込み            │ │
│ └─────────────────────────────────────────────────────┘ │
│ ┌─────────────────────────────────────────────────────┐ │
│ │ DuplicateChecker                                    │ │
│ │ - filter_contract_list() : 委託先法人ID=5抽出       │ │
│ │ - check_duplicates() : 重複チェック                 │ │
│ └─────────────────────────────────────────────────────┘ │
│ ┌─────────────────────────────────────────────────────┐ │
│ │ DataMapper                                          │ │
│ │ - create_output_dataframe() : 111列DF作成           │ │
│ │ - map_contractor_info() : 契約者情報マッピング       │ │
│ │ - map_property_info() : 物件情報マッピング           │ │
│ │ - map_guarantor_info() : 保証人情報マッピング        │ │
│ │ - map_emergency_contact_info() : 緊急連絡人マッピング│ │
│ │ - apply_fixed_values() : 固定値適用                 │ │
│ └─────────────────────────────────────────────────────┘ │
│ ┌─────────────────────────────────────────────────────┐ │
│ │ process_nap_data() : メイン処理関数                  │ │
│ └─────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────┘
```

## 2. クラス設計

### 2.1 NapConfig

```python
class NapConfig:
    """ナップ新規登録の設定・定数管理"""

    OUTPUT_FILE_PREFIX = "ナップ新規登録"

    # 111列の完全定義
    OUTPUT_COLUMNS = [
        "引継番号", "契約者氏名", "契約者カナ", "契約者生年月日",
        # ... (111列分)
        "登録フラグ"
    ]

    # 固定値
    FIXED_VALUES = {
        "クライアントCD": "9268",
        "委託先法人ID": "5",
        "入居ステータス": "入居中",
        "滞納ステータス": "未精算",
        "受託状況": "契約中",
        "契約種類": "レントワン",
        "回収口座金融機関CD": "1",
        "回収口座金融機関名": "みずほ銀行",
        "回収口座種類": "普通",
        "回収口座番号": "4389306",
        "回収口座名義": "ナップ賃貸保証株式会社",
        # ... その他の固定値
    }

    # Excel入力列マッピング
    COLUMN_MAPPING = {
        "引継番号": "承認番号",
        "契約者氏名": "契約者氏名",
        "契約者カナ": "契約者氏名かな",
        # ... 全マッピング定義
    }

    # 委託先法人IDフィルタ値
    TARGET_CORPORATION_ID = "5"

    # Excelファイルのskiprows設定
    EXCEL_SKIPROWS = 13
```

### 2.2 FileReader

```python
class FileReader:
    """ファイル読み込みクラス"""

    def read_excel_file(
        self,
        file_content: bytes,
        skiprows: int = 13
    ) -> pd.DataFrame:
        """
        Excelファイルを読み込む

        Args:
            file_content: ファイルバイナリ
            skiprows: スキップする行数

        Returns:
            DataFrame

        Raises:
            ValueError: ファイル読み込み失敗
        """
        pass

    def read_csv_file(
        self,
        file_content: bytes
    ) -> pd.DataFrame:
        """
        CSVファイルを読み込む（エンコーディング自動判定）

        Args:
            file_content: ファイルバイナリ

        Returns:
            DataFrame

        Raises:
            ValueError: ファイル読み込み失敗
        """
        pass
```

### 2.3 DuplicateChecker

```python
class DuplicateChecker:
    """重複チェッククラス"""

    def filter_contract_list(
        self,
        contract_df: pd.DataFrame,
        target_id: str = "5"
    ) -> pd.DataFrame:
        """
        ContractListを委託先法人IDでフィルタ

        Args:
            contract_df: ContractList DataFrame
            target_id: 対象の委託先法人ID

        Returns:
            フィルタ済みDataFrame
        """
        pass

    def check_duplicates(
        self,
        excel_df: pd.DataFrame,
        contract_df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.DataFrame, Dict, List[str]]:
        """
        重複チェック実行

        Args:
            excel_df: Excel入力データ
            contract_df: ContractList（フィルタ済み）

        Returns:
            - new_data: 新規データ
            - existing_data: 既存データ
            - stats: 統計情報
            - logs: 処理ログ
        """
        pass
```

### 2.4 DataMapper

```python
class DataMapper:
    """データマッピングクラス"""

    def __init__(self, config: NapConfig):
        self.config = config

    def create_output_dataframe(
        self,
        excel_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        111列の出力DataFrameを作成

        Args:
            excel_df: 入力DataFrame

        Returns:
            111列のDataFrame
        """
        pass

    def map_contractor_info(
        self,
        output_df: pd.DataFrame,
        excel_df: pd.DataFrame
    ) -> None:
        """契約者情報をマッピング（in-place）"""
        pass

    def map_property_info(
        self,
        output_df: pd.DataFrame,
        excel_df: pd.DataFrame
    ) -> None:
        """物件情報をマッピング（in-place）"""
        pass

    def map_guarantor_info(
        self,
        output_df: pd.DataFrame,
        excel_df: pd.DataFrame
    ) -> None:
        """保証人情報をマッピング（in-place）

        注: 入力の「連帯保証人」を出力の「保証人１」にマッピング
        """
        pass

    def map_emergency_contact_info(
        self,
        output_df: pd.DataFrame,
        excel_df: pd.DataFrame
    ) -> None:
        """緊急連絡人情報をマッピング（in-place）"""
        pass

    def apply_fixed_values(
        self,
        output_df: pd.DataFrame
    ) -> None:
        """固定値を適用（in-place）"""
        pass
```

## 3. 処理フロー

```
1. ファイル読み込み
   ├─ read_excel_file(skiprows=13)
   │   └─ 51列のDataFrame取得
   └─ read_csv_file()
       └─ ContractList DataFrame取得

2. ContractListフィルタリング
   └─ filter_contract_list(target_id="5")
       └─ 委託先法人ID=5のデータのみ抽出

3. 重複チェック
   └─ check_duplicates(excel_df, filtered_contract_df)
       ├─ Excel「承認番号」↔ ContractList「引継番号」照合
       ├─ 新規データと既存データに分離
       └─ 統計情報・ログ生成

4. データマッピング（新規データのみ）
   ├─ create_output_dataframe() : 111列空DataFrame作成
   ├─ map_contractor_info() : 契約者情報
   ├─ map_property_info() : 物件情報
   ├─ map_guarantor_info() : 保証人情報（連帯保証人→保証人１）
   ├─ map_emergency_contact_info() : 緊急連絡人情報
   └─ apply_fixed_values() : 固定値適用

5. 出力
   └─ return (output_df, logs, filename)
```

## 4. データ構造

### 4.1 Excel入力データ（51列）

| 列番号 | 列名 | データ型 | 備考 |
|--------|------|----------|------|
| 4 | 契約管理ID | str | 使用しない |
| 5 | 承認番号 | float/int | 引継番号として使用 |
| 6 | 契約者氏名 | str | - |
| 7 | 契約者氏名かな | str | - |
| 10 | 契約者生年月日 | date/str | - |
| 11 | 契約者電話 | str | - |
| 12 | 契約者携帯1 | str | - |
| 13 | 物件名 | str | - |
| 14 | 部屋番号 | str | - |
| 15-17 | 物件住所１〜３ | str | - |
| 18-20 | 契約者１住所１〜３ | str | - |
| 21-26 | 賃料関連 | float/int | - |
| 29-30 | 勤務先情報 | str | - |
| 31-40 | 連帯保証人情報 | str/date | 出力の「保証人１」にマッピング |
| 41-50 | 緊急連絡人情報 | str | - |
| 51 | 加盟店: 加盟店名 | str | 管理会社 |

### 4.2 ContractList（122列）

主要列のみ記載：

| 列名 | データ型 | 備考 |
|------|----------|------|
| 引継番号 | str | 重複チェックキー |
| 委託先法人ID | str | フィルタ条件（"5"のみ対象） |

### 4.3 出力データ（111列）

標準新規登録フォーマット。詳細は要件定義書参照。

## 5. エラーハンドリング

### 5.1 ファイル読み込みエラー
- Excelファイルが不正 → ValueError
- CSVエンコーディングNG → 複数エンコーディング試行後エラー

### 5.2 データ検証エラー
- 必須列欠如 → ValueError（具体的な列名を含む）
- 承認番号が空 → 警告ログ、該当行をスキップ

### 5.3 重複チェックエラー
- ContractListが空 → 全データを新規として処理
- 委託先法人ID=5が0件 → 警告ログ

## 6. テスト戦略

### 6.1 ユニットテスト

#### 6.1.1 FileReaderのテスト
- `test_read_excel_file_success`: 正常読み込み
- `test_read_excel_file_invalid`: 不正ファイル
- `test_read_csv_file_cp932`: CP932エンコーディング
- `test_read_csv_file_utf8`: UTF-8エンコーディング
- `test_read_csv_file_shift_jis`: Shift_JISエンコーディング

#### 6.1.2 DuplicateCheckerのテスト
- `test_filter_contract_list_with_target_id`: ID=5のフィルタ
- `test_filter_contract_list_empty`: 該当データなし
- `test_check_duplicates_no_duplicates`: 重複なし
- `test_check_duplicates_all_duplicates`: 全件重複
- `test_check_duplicates_partial`: 一部重複

#### 6.1.3 DataMapperのテスト
- `test_create_output_dataframe`: 111列作成
- `test_map_contractor_info`: 契約者情報マッピング
- `test_map_property_info`: 物件情報マッピング
- `test_map_guarantor_info`: 保証人情報マッピング（連帯保証人→保証人１）
- `test_map_emergency_contact_info`: 緊急連絡人情報マッピング
- `test_apply_fixed_values`: 固定値適用

### 6.2 統合テスト
- `test_process_nap_data_end_to_end`: E2Eテスト（実データ使用）

## 7. パフォーマンス考慮

- pandas vectorized操作を使用（ループ最小化）
- 大量データ対応（1000件程度を想定）
- メモリ効率的なマージ処理

## 8. 保守性

- 設定値はNapConfigに集中管理
- 既存コード（プラザ新規登録）のパターンを踏襲
- 明確なクラス分離（単一責任原則）
- 型ヒントによる可読性向上
