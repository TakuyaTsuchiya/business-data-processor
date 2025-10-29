"""
訪問リスト作成プロセッサー
Business Data Processor

ContractList.csvから訪問スタッフ用の訪問リストExcelを生成
"""

import pandas as pd
import io
from datetime import datetime
from typing import Tuple, List, Dict
import sys
import os

# 共通定義のインポート
processors_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if processors_dir not in sys.path:
    sys.path.append(processors_dir)

from common.contract_list_columns import ContractListColumns as COL
from common.prefecture_order import get_prefecture_order, extract_prefecture_from_address, PREFECTURE_ORDER


class VisitListConfig:
    """訪問リスト作成の設定・定数"""

    # フィルタ条件（固定値）
    FILTER_COLLECTION_RANKS = ["交渉困難", "死亡決定", "弁護士介入"]
    FILTER_PAYMENT_AMOUNTS_EXCLUDE = [2, 3, 5]

    # 5種類の人物別の列定義
    PERSON_TYPES = {
        "contractor": {
            "name": "契約者",
            "sheet_name": "契約者",
            "name_col": COL.CONTRACT_NAME,
            "postal_col": 22,  # 郵便番号
            "address1_col": 23,  # 現住所1
            "address2_col": 24,  # 現住所2
            "address3_col": 25,  # 現住所3
            "tel_col": COL.TEL_MOBILE,
        },
        "guarantor1": {
            "name": "保証人1",
            "sheet_name": "保証人1",
            "name_col": 42,
            "postal_col": 43,
            "address1_col": 44,
            "address2_col": 45,
            "address3_col": 46,
            "tel_col": 46,  # TEL携帯.1
        },
        "guarantor2": {
            "name": "保証人2",
            "sheet_name": "保証人2",
            "name_col": 49,
            "postal_col": 50,
            "address1_col": 51,
            "address2_col": 52,
            "address3_col": 53,
            "tel_col": 53,  # TEL携帯.2
        },
        "contact1": {
            "name": "連絡人1",
            "sheet_name": "連絡人1",
            "name_col": COL.EMERGENCY_CONTACT_NAME,  # 53
            "postal_col": 58,
            "address1_col": 59,
            "address2_col": 60,
            "address3_col": 61,
            "tel_col": COL.TEL_MOBILE_2,  # 56
        },
        "contact2": {
            "name": "連絡人2",
            "sheet_name": "連絡人2",
            "name_col": 60,
            "postal_col": 64,
            "address1_col": 65,
            "address2_col": 66,
            "address3_col": 67,
            "tel_col": 63,
        },
    }

    OUTPUT_FILE_PREFIX = "訪問リスト"


def read_csv_auto_encoding(file_content: bytes) -> pd.DataFrame:
    """
    CSVファイルを自動エンコーディング判定で読み込み

    Args:
        file_content: CSVファイルの内容（bytes）

    Returns:
        pd.DataFrame: 読み込んだDataFrame

    Raises:
        ValueError: すべてのエンコーディングで読み込みに失敗した場合
    """
    encodings = ['cp932', 'shift_jis', 'utf-8-sig', 'utf-8']

    for enc in encodings:
        try:
            return pd.read_csv(io.BytesIO(file_content), encoding=enc)
        except Exception:
            continue

    raise ValueError("CSVファイルの読み込みに失敗しました。エンコーディングを確認してください。")


def filter_records(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """
    フィルタリング処理

    フィルタ条件:
    1. 回収ランク: 「交渉困難」「死亡決定」「弁護士介入」のみ
    2. 入金予定日: 当日以前（当日含む）
    3. 入金予定金額: 2, 3, 5を除外
    4. 現住所1: 空白を除外

    Args:
        df: 入力DataFrame

    Returns:
        Tuple[pd.DataFrame, List[str]]: (フィルタ後のDataFrame, 処理ログ)
    """
    logs = []
    initial_count = len(df)
    logs.append(f"入力レコード数: {initial_count}件")

    # 1. 回収ランクフィルタ
    mask_rank = df.iloc[:, COL.COLLECTION_RANK].isin(VisitListConfig.FILTER_COLLECTION_RANKS)
    df_filtered = df[mask_rank].copy()
    logs.append(f"回収ランクフィルタ後: {len(df_filtered)}件")

    # 2. 入金予定日フィルタ（当日以前）
    today = datetime.now().date()
    # 日付列を datetime 型に変換
    df_filtered['payment_date_parsed'] = pd.to_datetime(
        df_filtered.iloc[:, COL.PAYMENT_DATE],
        errors='coerce'
    )
    mask_date = df_filtered['payment_date_parsed'].notna() & (
        df_filtered['payment_date_parsed'].dt.date <= today
    )
    df_filtered = df_filtered[mask_date].copy()
    df_filtered = df_filtered.drop(columns=['payment_date_parsed'])
    logs.append(f"入金予定日フィルタ後: {len(df_filtered)}件")

    # 3. 入金予定金額フィルタ（2, 3, 5を除外）
    payment_amount_col = df_filtered.iloc[:, COL.PAYMENT_AMOUNT]
    # 数値に変換（エラーは NaN）
    payment_amount_numeric = pd.to_numeric(payment_amount_col, errors='coerce')
    mask_amount = ~payment_amount_numeric.isin(VisitListConfig.FILTER_PAYMENT_AMOUNTS_EXCLUDE)
    df_filtered = df_filtered[mask_amount].copy()
    logs.append(f"入金予定金額フィルタ後: {len(df_filtered)}件")

    # 4. 現住所1フィルタ（空白を除外）
    # 契約者の現住所1をチェック（列23）
    address1_col = df_filtered.iloc[:, 23]
    mask_address = address1_col.notna() & (address1_col != '') & (address1_col != ' ')
    df_filtered = df_filtered[mask_address].copy()
    logs.append(f"現住所1フィルタ後（最終）: {len(df_filtered)}件")

    return df_filtered, logs


def combine_address(address1: str, address2: str, address3: str) -> str:
    """
    住所を結合

    Args:
        address1: 現住所1（都道府県）
        address2: 現住所2（市区町村）
        address3: 現住所3（町名以降）

    Returns:
        str: 結合された住所
    """
    parts = []
    for addr in [address1, address2, address3]:
        if pd.notna(addr) and str(addr).strip() != '':
            parts.append(str(addr).strip())

    return ''.join(parts)


def extract_person_data(
    df: pd.DataFrame,
    person_type: str,
    config: Dict
) -> pd.DataFrame:
    """
    特定人物のデータを抽出

    Args:
        df: 入力DataFrame
        person_type: 人物種類（"contractor", "guarantor1" など）
        config: 列定義

    Returns:
        pd.DataFrame: 抽出されたDataFrame
    """
    # 該当人物の住所1が空白のレコードを除外
    address1_col = df.iloc[:, config["address1_col"]]
    mask_address = address1_col.notna() & (address1_col != '') & (address1_col != ' ')
    df_person = df[mask_address].copy()

    # 結合住所列を作成
    df_person['結合住所'] = df_person.apply(
        lambda row: combine_address(
            row.iloc[config["address1_col"]],
            row.iloc[config["address2_col"]],
            row.iloc[config["address3_col"]]
        ),
        axis=1
    )

    return df_person


def sort_by_prefecture(df: pd.DataFrame, address1_col_index: int) -> pd.DataFrame:
    """
    都道府県順（北→南）でソート

    Args:
        df: 入力DataFrame
        address1_col_index: 現住所1の列インデックス

    Returns:
        pd.DataFrame: ソート済みDataFrame
    """
    # 現住所1から都道府県を抽出し、順序番号を取得
    df['prefecture_order'] = df.iloc[:, address1_col_index].apply(
        lambda addr: get_prefecture_order(extract_prefecture_from_address(str(addr)))
    )

    # 都道府県順でソート
    df_sorted = df.sort_values('prefecture_order').drop(columns=['prefecture_order'])

    return df_sorted


def generate_excel(
    df_dict: Dict[str, pd.DataFrame],
    output_filename: str
) -> Tuple[bytes, List[str]]:
    """
    5シートのExcelファイルを生成

    Args:
        df_dict: 人物種類別のDataFrameの辞書
        output_filename: 出力ファイル名

    Returns:
        Tuple[bytes, List[str]]: (Excelファイルの内容、処理ログ)
    """
    logs = []

    # Excelファイルを作成
    excel_buffer = io.BytesIO()
    with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
        for person_type, config in VisitListConfig.PERSON_TYPES.items():
            if person_type in df_dict:
                df = df_dict[person_type]
                sheet_name = config["sheet_name"]

                # 結合住所列を現住所1の左に移動
                # 現在の列順を取得
                cols = df.columns.tolist()
                if '結合住所' in cols:
                    cols.remove('結合住所')
                    # 現住所1の位置を探す（列インデックス）
                    address1_col_index = config["address1_col"]
                    # 現住所1の列名を取得
                    address1_col_name = df.columns[address1_col_index]
                    # address1_col_nameの位置を探す
                    insert_index = cols.index(address1_col_name)
                    # 結合住所を挿入
                    cols.insert(insert_index, '結合住所')
                    df = df[cols]

                # Excelシートに書き込み
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                logs.append(f"{sheet_name}シート: {len(df)}件")

    excel_buffer.seek(0)
    return excel_buffer.getvalue(), logs


def process_visit_list(file_content: bytes) -> Tuple[bytes, List[str], str]:
    """
    訪問リスト作成のメイン処理

    Args:
        file_content: ContractList.csvの内容（bytes）

    Returns:
        Tuple[bytes, List[str], str]: (Excelファイル内容、処理ログ、出力ファイル名)
    """
    logs = []

    try:
        # 1. CSVファイル読み込み
        logs.append("=== CSVファイル読み込み ===")
        df_input = read_csv_auto_encoding(file_content)
        logs.append(f"読み込み成功: {len(df_input)}行")

        # 2. フィルタリング
        logs.append("\n=== フィルタリング処理 ===")
        df_filtered, filter_logs = filter_records(df_input)
        logs.extend(filter_logs)

        if len(df_filtered) == 0:
            raise ValueError("フィルタ条件に一致するレコードがありません")

        # 3. 5種類の人物別にデータを抽出
        logs.append("\n=== 人物別データ抽出 ===")
        df_dict = {}
        for person_type, config in VisitListConfig.PERSON_TYPES.items():
            df_person = extract_person_data(df_filtered, person_type, config)
            if len(df_person) > 0:
                # 都道府県順でソート
                df_person_sorted = sort_by_prefecture(df_person, config["address1_col"])
                df_dict[person_type] = df_person_sorted
                logs.append(f"{config['name']}: {len(df_person_sorted)}件")
            else:
                logs.append(f"{config['name']}: 0件（スキップ）")

        # 4. Excelファイル生成
        logs.append("\n=== Excelファイル生成 ===")
        today_str = datetime.now().strftime("%Y%m%d")
        output_filename = f"{VisitListConfig.OUTPUT_FILE_PREFIX}_{today_str}.xlsx"

        excel_content, excel_logs = generate_excel(df_dict, output_filename)
        logs.extend(excel_logs)

        logs.append(f"\n処理完了: {output_filename}")

        return excel_content, logs, output_filename

    except Exception as e:
        raise Exception(f"訪問リスト作成処理エラー: {str(e)}")
