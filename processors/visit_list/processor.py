"""
訪問リスト作成プロセッサー
Business Data Processor

ContractList.csvから訪問スタッフ用の訪問リストExcelを生成
"""

import pandas as pd
from datetime import datetime
from typing import Tuple, List, Dict


# 22列の出力構造定義
OUTPUT_COLUMNS = [
    "管理番号",           # 1
    "最新契約種類",        # 2
    "受託状況",           # 3
    "入居ステータス",       # 4
    "滞納ステータス",       # 5
    "退去手続き（実費）",    # 6
    "営業担当者",          # 7
    "[人物]氏名",          # 8 - 各シートで変わる
    "",                   # 9 - 結合住所（空白ヘッダー）
    "現住所1",            # 10 - 各シートで変わる
    "現住所2",            # 11
    "現住所3",            # 12
    "滞納残債",            # 13
    "入金予定日",          # 14
    "入金予定金額",         # 15
    "月額賃料合計",         # 16
    "回収ランク",          # 17
    "クライアントCD",       # 18
    "クライアント名",       # 19
    "委託先法人ID",        # 20
    "委託先法人名",         # 21
    "解約日",             # 22
]


class VisitListConfig:
    """訪問リスト作成の設定・定数"""

    # 5種類の人物別の列定義（ContractListの列番号）
    PERSON_TYPES = {
        "contractor": {
            "name": "契約者",
            "sheet_name": "契約者",
            "name_col": 20,      # 契約者氏名
            "address1_col": 23,  # 現住所1
            "address2_col": 24,  # 現住所2
            "address3_col": 25,  # 現住所3
        },
        "guarantor1": {
            "name": "保証人1",
            "sheet_name": "保証人1",
            "name_col": 41,      # 保証人１氏名
            "address1_col": 43,  # 現住所1.1
            "address2_col": 44,  # 現住所2.1
            "address3_col": 45,  # 現住所3.1
        },
        "guarantor2": {
            "name": "保証人2",
            "sheet_name": "保証人2",
            "name_col": 48,      # 保証人２氏名
            "address1_col": 50,  # 現住所1.2
            "address2_col": 51,  # 現住所2.2
            "address3_col": 52,  # 現住所3.2
        },
        "contact1": {
            "name": "連絡人1",
            "sheet_name": "連絡人1",
            "name_col": 55,      # 緊急連絡人１氏名
            "address1_col": 58,  # 現住所1.3
            "address2_col": 59,  # 現住所2.3
            "address3_col": 60,  # 現住所3.3
        },
        "contact2": {
            "name": "連絡人2",
            "sheet_name": "連絡人2",
            "name_col": 62,      # 緊急連絡人２氏名
            "address1_col": 64,  # 現住所1.4
            "address2_col": 65,  # 現住所2.4
            "address3_col": 66,  # 現住所3.4
        },
    }

    OUTPUT_FILE_PREFIX = "訪問リスト"


def combine_address(address1, address2, address3) -> str:
    """
    住所を結合

    Args:
        address1: 現住所1
        address2: 現住所2
        address3: 現住所3

    Returns:
        str: 結合された住所
    """
    parts = []
    for addr in [address1, address2, address3]:
        if pd.notna(addr) and str(addr).strip() != '':
            parts.append(str(addr).strip())
    return ''.join(parts)


def filter_records(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """
    フィルタリング処理

    5つのフィルタ条件:
    1. 回収ランク: 「交渉困難」「死亡決定」「弁護士介入」
    2. 入金予定日: 当日以前
    3. 入金予定金額: 2, 3, 5を除外
    4. 委託先法人ID: 5と空白のみ
    5. 現住所1: 空白を除外

    Args:
        df: ContractList DataFrame

    Returns:
        Tuple[pd.DataFrame, List[str]]: フィルタ後のDataFrameとログ
    """
    logs = []
    initial_count = len(df)
    logs.append(f"入力レコード数: {initial_count}件")

    # 1. 回収ランクフィルタ
    mask_rank = df.iloc[:, 86].isin(["交渉困難", "死亡決定", "弁護士介入"])
    df_filtered = df[mask_rank].copy()
    logs.append(f"回収ランクフィルタ後: {len(df_filtered)}件")

    # 2. 入金予定日フィルタ（当日以前）
    today = datetime.now().date()
    df_filtered['payment_date_parsed'] = pd.to_datetime(
        df_filtered.iloc[:, 72], errors='coerce'
    )
    mask_date = df_filtered['payment_date_parsed'].notna() & (
        df_filtered['payment_date_parsed'].dt.date <= today
    )
    df_filtered = df_filtered[mask_date].copy()
    df_filtered = df_filtered.drop(columns=['payment_date_parsed'])
    logs.append(f"入金予定日フィルタ後: {len(df_filtered)}件")

    # 3. 入金予定金額フィルタ（2, 3, 5を除外）
    payment_amount_numeric = pd.to_numeric(df_filtered.iloc[:, 73], errors='coerce')
    mask_amount = ~payment_amount_numeric.isin([2, 3, 5])
    df_filtered = df_filtered[mask_amount].copy()
    logs.append(f"入金予定金額フィルタ後: {len(df_filtered)}件")

    # 4. 委託先法人IDフィルタ（5と空白のみ）
    delegated_corp_id = df_filtered.iloc[:, 118]
    mask_corp_id = (delegated_corp_id == '5') | (delegated_corp_id == 5) | delegated_corp_id.isna() | (delegated_corp_id == '')
    df_filtered = df_filtered[mask_corp_id].copy()
    logs.append(f"委託先法人IDフィルタ後: {len(df_filtered)}件")

    # 5. 現住所1フィルタ（契約者の住所があるもののみ）
    mask_address = df_filtered.iloc[:, 23].notna() & (df_filtered.iloc[:, 23] != '')
    df_filtered = df_filtered[mask_address].copy()
    logs.append(f"現住所1フィルタ後（最終）: {len(df_filtered)}件")

    return df_filtered, logs


def create_output_row(row: pd.Series, person_type: str, config: Dict) -> Dict:
    """
    ContractListの1行から22列の出力データを作成

    Args:
        row: ContractListの1行
        person_type: 人物種類（"contractor", "guarantor1" など）
        config: 人物別の列定義

    Returns:
        Dict: 22列のデータ辞書
    """
    # 該当人物の情報
    person_name = row.iloc[config["name_col"]]
    person_addr1 = row.iloc[config["address1_col"]]
    person_addr2 = row.iloc[config["address2_col"]]
    person_addr3 = row.iloc[config["address3_col"]]
    person_combined = combine_address(person_addr1, person_addr2, person_addr3)

    # 基本情報
    output = {
        "管理番号": row.iloc[0],
        "最新契約種類": row.iloc[2],
        "受託状況": row.iloc[3] if len(row) > 3 else None,  # 仮
        "入居ステータス": row.iloc[14],
        "滞納ステータス": row.iloc[15],
        "退去手続き（実費）": row.iloc[17],
        "営業担当者": row.iloc[19],
        "[人物]氏名": person_name,
        "": person_combined,  # 結合住所（空白ヘッダー）
        "現住所1": person_addr1,
        "現住所2": person_addr2,
        "現住所3": person_addr3,
        "滞納残債": row.iloc[71],
        "入金予定日": row.iloc[72],
        "入金予定金額": row.iloc[73],
        "月額賃料合計": row.iloc[84],
        "回収ランク": row.iloc[86],
        "クライアントCD": row.iloc[97],
        "クライアント名": row.iloc[98],
        "委託先法人ID": row.iloc[118],
        "委託先法人名": row.iloc[119],
        "解約日": row.iloc[120],
    }

    return output
