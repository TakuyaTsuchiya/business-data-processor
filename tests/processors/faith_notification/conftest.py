"""
フェイス差込み用リスト（faith_notification）テスト用フィクスチャ
"""

import pandas as pd
import pytest
from datetime import timedelta

# カラムインデックス定数
COL = {
    "MANAGEMENT_NO": 0,
    "RESIDENCE_STATUS": 14,
    "CONTRACT_NAME": 20,
    "POSTAL_CODE": 22,
    "ADDRESS1": 23,
    "ADDRESS2": 24,
    "ADDRESS3": 25,
    "BANK_CD": 34,
    "BANK_NAME": 35,
    "BRANCH_CD": 36,
    "BRANCH_NAME": 37,
    "ACCOUNT_TYPE": 38,
    "ACCOUNT_NO": 39,
    "ACCOUNT_HOLDER": 40,
    "GUARANTOR1_NAME": 41,
    "GUARANTOR1_POSTAL": 42,
    "GUARANTOR1_ADDR1": 43,
    "GUARANTOR1_ADDR2": 44,
    "GUARANTOR1_ADDR3": 45,
    "GUARANTOR2_NAME": 48,
    "GUARANTOR2_POSTAL": 49,
    "GUARANTOR2_ADDR1": 50,
    "GUARANTOR2_ADDR2": 51,
    "GUARANTOR2_ADDR3": 52,
    "CONTACT1_NAME": 55,
    "CONTACT1_POSTAL": 57,
    "CONTACT1_ADDR1": 58,
    "CONTACT1_ADDR2": 59,
    "CONTACT1_ADDR3": 60,
    "CONTACT2_NAME": 62,
    "CONTACT2_POSTAL": 63,
    "CONTACT2_ADDR1": 64,
    "CONTACT2_ADDR2": 65,
    "CONTACT2_ADDR3": 66,
    "DEBT_AMOUNT": 71,
    "PAYMENT_DATE": 72,
    "PAYMENT_AMOUNT": 73,
    "COLLECTION_RANK": 86,
    "PROPERTY_ADDR1": 92,
    "PROPERTY_ADDR2": 93,
    "PROPERTY_ADDR3": 94,
    "PROPERTY_NAME": 95,
    "PROPERTY_NO": 96,
    "TRUSTEE_ID": 118,
}


def create_notification_dataframe(rows_data):
    """テスト用DataFrameを生成する

    Args:
        rows_data: 各行のデータ。カラムインデックス→値のdictのリスト。

    Returns:
        119列のDataFrame。未指定セルは空文字列。
    """
    num_cols = 119
    num_rows = len(rows_data)
    df = pd.DataFrame(
        [["" for _ in range(num_cols)] for _ in range(num_rows)],
        columns=[f"col_{i}" for i in range(num_cols)],
    )
    for row_idx, row_data in enumerate(rows_data):
        for col_idx, value in row_data.items():
            if isinstance(col_idx, int) and col_idx < num_cols:
                df.iloc[row_idx, col_idx] = value
    return df


def _yesterday_str():
    return (pd.Timestamp.now().normalize() - timedelta(days=1)).strftime("%Y/%m/%d")


def _base_valid_row_data():
    """全共通フィルタを通過し、契約者・保証人・連絡人の住所が揃った行データ"""
    yesterday = _yesterday_str()
    return {
        COL["MANAGEMENT_NO"]: "MGT001",
        COL["RESIDENCE_STATUS"]: "入居中",
        COL["CONTRACT_NAME"]: "テスト太郎",
        COL["POSTAL_CODE"]: "100-0001",
        COL["ADDRESS1"]: "東京都",
        COL["ADDRESS2"]: "千代田区",
        COL["ADDRESS3"]: "丸の内1-1-1",
        COL["BANK_CD"]: "0001",
        COL["BANK_NAME"]: "みずほ銀行",
        COL["BRANCH_CD"]: "001",
        COL["BRANCH_NAME"]: "東京支店",
        COL["ACCOUNT_TYPE"]: "普通",
        COL["ACCOUNT_NO"]: "1234567",
        COL["ACCOUNT_HOLDER"]: "テスト太郎",
        COL["GUARANTOR1_NAME"]: "保証一郎",
        COL["GUARANTOR1_POSTAL"]: "200-0001",
        COL["GUARANTOR1_ADDR1"]: "神奈川県",
        COL["GUARANTOR1_ADDR2"]: "横浜市",
        COL["GUARANTOR1_ADDR3"]: "中区1-1",
        COL["GUARANTOR2_NAME"]: "保証二郎",
        COL["GUARANTOR2_POSTAL"]: "300-0001",
        COL["GUARANTOR2_ADDR1"]: "埼玉県",
        COL["GUARANTOR2_ADDR2"]: "さいたま市",
        COL["GUARANTOR2_ADDR3"]: "大宮区1-1",
        COL["CONTACT1_NAME"]: "連絡一郎",
        COL["CONTACT1_POSTAL"]: "400-0001",
        COL["CONTACT1_ADDR1"]: "千葉県",
        COL["CONTACT1_ADDR2"]: "千葉市",
        COL["CONTACT1_ADDR3"]: "中央区1-1",
        COL["CONTACT2_NAME"]: "連絡二郎",
        COL["CONTACT2_POSTAL"]: "500-0001",
        COL["CONTACT2_ADDR1"]: "群馬県",
        COL["CONTACT2_ADDR2"]: "前橋市",
        COL["CONTACT2_ADDR3"]: "大手町1-1",
        COL["DEBT_AMOUNT"]: "10,000",
        COL["PAYMENT_DATE"]: yesterday,
        COL["PAYMENT_AMOUNT"]: 100,
        COL["COLLECTION_RANK"]: "通常",
        COL["PROPERTY_ADDR1"]: "東京都",
        COL["PROPERTY_ADDR2"]: "港区",
        COL["PROPERTY_ADDR3"]: "六本木1-1",
        COL["PROPERTY_NAME"]: "テストマンション",
        COL["PROPERTY_NO"]: "101",
        COL["TRUSTEE_ID"]: 1,
    }


@pytest.fixture
def base_valid_row():
    """全共通フィルタを通過する基本行データのdict"""
    return _base_valid_row_data()


@pytest.fixture
def valid_df():
    """共通フィルタを通過する1行のDataFrame"""
    return create_notification_dataframe([_base_valid_row_data()])
