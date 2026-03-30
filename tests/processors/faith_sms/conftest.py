"""
フェイスSMS処理テスト用の共通フィクスチャ

フェイスSMSは119列以上のCSVを処理し、以下の2つの方法でデータにアクセスする:
1. 列名（df['委託先法人ID']）→ フィルターおよびマッピングで使用
2. 列番号（df.iloc[:, 71]）→ 滞納残債フィルター、保証人TEL(46)、緊急連絡人TEL(56)
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta, date


# フェイスSMSで列番号（iloc）でアクセスされる列のインデックス
FAITH_COLUMN_INDICES = {
    "滞納残債": 71,  # BT列 - Filter 5で使用
    "保証人TEL": 46,  # AU列 - guarantor.py Filter 6で使用
    "緊急連絡人TEL": 56,  # BE列 - emergency_contact.py Filter 6で使用
}

# 列名でアクセスされる列（フィルターおよびマッピングで使用）
FAITH_NAMED_COLUMNS = [
    "委託先法人ID",
    "入金予定日",
    "入金予定金額",
    "回収ランク",
    "TEL携帯",
    "契約者氏名",
    "物件名",
    "物件番号",
    "管理番号",
    "回収口座銀行名",
    "回収口座支店名",
    "回収口座種類",
    "回収口座番号",
    "回収口座名義人",
    "滞納残債",
    "保証人１氏名",
    "緊急連絡人１氏名",
]


def create_faith_sms_dataframe(rows_data: list) -> pd.DataFrame:
    """
    フェイスSMS用のテストDataFrameを作成するヘルパー関数

    119列のダミーDataFrameを作成し、必要な列だけ実データを設定する。
    フェイスSMSは列名アクセス（df['委託先法人ID']等）と
    列番号アクセス（df.iloc[:, 71]等）の両方を使うため、両方を正しく設定する。

    Args:
        rows_data: 各行のデータを含む辞書のリスト
    Returns:
        119列+名前付き列のDataFrame
    """
    num_rows = len(rows_data)
    num_cols = 119

    # 全ての列を空文字で初期化
    df = pd.DataFrame(
        [["" for _ in range(num_cols)] for _ in range(num_rows)],
        columns=[f"col_{i}" for i in range(num_cols)],
    )

    # 列番号でアクセスする列にデータを設定（iloc用）
    for col_name, col_index in FAITH_COLUMN_INDICES.items():
        if col_index < num_cols:
            for row_idx, row_data in enumerate(rows_data):
                if col_name in row_data:
                    df.iloc[row_idx, col_index] = row_data[col_name]

    # 列名でアクセスする列を追加
    for col_name in FAITH_NAMED_COLUMNS:
        df[col_name] = ""
        for row_idx, row_data in enumerate(rows_data):
            if col_name in row_data:
                df.loc[row_idx, col_name] = row_data[col_name]

    # 滞納残債は列番号71（iloc）と列名の両方でアクセスされるため、
    # 列名側のデータも同期する（create後に上書き）
    for row_idx, row_data in enumerate(rows_data):
        if "滞納残債" in row_data:
            df.loc[row_idx, "滞納残債"] = row_data["滞納残債"]

    return df


def dataframe_to_csv_bytes(df: pd.DataFrame, encoding: str = "utf-8") -> bytes:
    """
    DataFrameをCSVのバイト列に変換する。
    SMS処理関数はファイルのバイト列を受け取るため、変換が必要。
    """
    return df.to_csv(index=False, encoding=encoding).encode(encoding)


def create_valid_faith_row(**overrides):
    """
    全フィルターを通過する有効なデータ行を生成するヘルパー。
    overridesで特定のフィールドだけ上書きできる。
    """
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y/%m/%d")
    row = {
        "TEL携帯": "090-1234-5678",
        "滞納残債": "10,000",
        "入金予定日": yesterday,
        "入金予定金額": "1",
        "回収ランク": "通常",
        "委託先法人ID": "1",
        "契約者氏名": "テスト太郎",
        "物件名": "テストマンション",
        "物件番号": "101",
        "管理番号": "M001",
        "回収口座銀行名": "テスト銀行",
        "回収口座支店名": "テスト支店",
        "回収口座種類": "普通",
        "回収口座番号": "1234567",
        "回収口座名義人": "テスト名義",
        "保証人１氏名": "保証太郎",
        "緊急連絡人１氏名": "連絡太郎",
        "保証人TEL": "080-1111-2222",
        "緊急連絡人TEL": "070-3333-4444",
    }
    row.update(overrides)
    return row


@pytest.fixture
def payment_deadline_date():
    """支払期限日のフィクスチャ"""
    return date(2025, 12, 31)
