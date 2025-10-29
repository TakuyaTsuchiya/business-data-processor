"""
訪問リスト作成プロセッサー
Business Data Processor

ContractList.csvから訪問スタッフ用の訪問リストExcelを生成
"""

import pandas as pd
import io
from datetime import datetime
from typing import Tuple, List, Dict
from openpyxl import Workbook
from openpyxl.styles import Font, Border
from processors.common.prefecture_order import get_prefecture_order, extract_prefecture_from_address


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
            "output_name_col": "契約者氏名",
            "name_col": 20,      # 契約者氏名
            "address1_col": 23,  # 現住所1
            "address2_col": 24,  # 現住所2
            "address3_col": 25,  # 現住所3
        },
        "guarantor1": {
            "name": "保証人1",
            "sheet_name": "保証人1",
            "output_name_col": "保証人１氏名",
            "name_col": 41,      # 保証人１氏名
            "address1_col": 43,  # 現住所1.1
            "address2_col": 44,  # 現住所2.1
            "address3_col": 45,  # 現住所3.1
        },
        "guarantor2": {
            "name": "保証人2",
            "sheet_name": "保証人2",
            "output_name_col": "保証人２氏名",
            "name_col": 48,      # 保証人２氏名
            "address1_col": 50,  # 現住所1.2
            "address2_col": 51,  # 現住所2.2
            "address3_col": 52,  # 現住所3.2
        },
        "contact1": {
            "name": "連絡人1",
            "sheet_name": "連絡人1",
            "output_name_col": "緊急連絡人１氏名",
            "name_col": 55,      # 緊急連絡人１氏名
            "address1_col": 58,  # 現住所1.3
            "address2_col": 59,  # 現住所2.3
            "address3_col": 60,  # 現住所3.3
        },
        "contact2": {
            "name": "連絡人2",
            "sheet_name": "連絡人2",
            "output_name_col": "緊急連絡人２氏名",
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

    # 1. 回収ランクフィルタ（「交渉困難」「死亡決定」「弁護士介入」を除外）
    mask_rank = ~df.iloc[:, 86].isin(["交渉困難", "死亡決定", "弁護士介入"])
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
        "受託状況": row.iloc[3],
        "入居ステータス": row.iloc[14],
        "滞納ステータス": row.iloc[15],
        "退去手続き（実費）": pd.to_numeric(row.iloc[17], errors='coerce'),
        "営業担当者": row.iloc[19],
        config["output_name_col"]: person_name,
        "": person_combined,  # 結合住所（空白ヘッダー）
        "現住所1": person_addr1,
        "現住所2": person_addr2,
        "現住所3": person_addr3,
        "滞納残債": pd.to_numeric(row.iloc[71], errors='coerce'),
        "入金予定日": row.iloc[72],
        "入金予定金額": pd.to_numeric(row.iloc[73], errors='coerce'),
        "月額賃料合計": pd.to_numeric(row.iloc[84], errors='coerce'),
        "回収ランク": row.iloc[86],
        "クライアントCD": row.iloc[97],
        "クライアント名": row.iloc[98],
        "委託先法人ID": row.iloc[118],
        "委託先法人名": row.iloc[119],
        "解約日": row.iloc[120],
    }

    return output


def create_output_row_bulk(df_person: pd.DataFrame, person_type: str, config: Dict) -> pd.DataFrame:
    """
    DataFrameの全行を一括処理（ベクトル化版）

    Args:
        df_person: ContractListのDataFrame
        person_type: 人物種類（"contractor", "guarantor1" など）
        config: 人物別の列定義

    Returns:
        pd.DataFrame: 22列の出力DataFrame
    """
    # 人物情報の列番号
    name_col = config["name_col"]
    addr1_col = config["address1_col"]
    addr2_col = config["address2_col"]
    addr3_col = config["address3_col"]

    # 結合住所を一括生成
    combined_addresses = (
        df_person.iloc[:, addr1_col].fillna('').astype(str) +
        df_person.iloc[:, addr2_col].fillna('').astype(str) +
        df_person.iloc[:, addr3_col].fillna('').astype(str)
    )

    # 全列を一括取得（ベクトル化）
    df_output = pd.DataFrame({
        "管理番号": df_person.iloc[:, 0].values,
        "最新契約種類": df_person.iloc[:, 2].values,
        "受託状況": df_person.iloc[:, 3].values,
        "入居ステータス": df_person.iloc[:, 14].values,
        "滞納ステータス": df_person.iloc[:, 15].values,
        "退去手続き（実費）": pd.to_numeric(
            df_person.iloc[:, 17].astype(str).str.replace(',', ''), errors='coerce'
        ),
        "営業担当者": df_person.iloc[:, 19].values,
        config["output_name_col"]: df_person.iloc[:, name_col].values,
        "": combined_addresses.values,
        "現住所1": df_person.iloc[:, addr1_col].values,
        "現住所2": df_person.iloc[:, addr2_col].values,
        "現住所3": df_person.iloc[:, addr3_col].values,
        "滞納残債": pd.to_numeric(
            df_person.iloc[:, 71].astype(str).str.replace(',', ''), errors='coerce'
        ),
        "入金予定日": df_person.iloc[:, 72].values,
        "入金予定金額": pd.to_numeric(
            df_person.iloc[:, 73].astype(str).str.replace(',', ''), errors='coerce'
        ),
        "月額賃料合計": pd.to_numeric(
            df_person.iloc[:, 84].astype(str).str.replace(',', ''), errors='coerce'
        ),
        "回収ランク": df_person.iloc[:, 86].values,
        "クライアントCD": df_person.iloc[:, 97].values,
        "クライアント名": df_person.iloc[:, 98].values,
        "委託先法人ID": df_person.iloc[:, 118].values,
        "委託先法人名": df_person.iloc[:, 119].values,
        "解約日": df_person.iloc[:, 120].values,
    })

    return df_output


def sort_by_prefecture(df: pd.DataFrame) -> pd.DataFrame:
    """
    都道府県順（北→南）でソート

    Args:
        df: 22列形式のDataFrame

    Returns:
        pd.DataFrame: ソート後のDataFrame
    """
    # 現住所1から都道府県を抽出し、順序番号を取得
    df['prefecture_order'] = df['現住所1'].apply(
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
    5シートのExcelファイルを生成（フォント設定付き）

    Args:
        df_dict: 人物タイプ別のDataFrame辞書
        output_filename: 出力ファイル名

    Returns:
        Tuple[bytes, List[str]]: Excelバイト列とログ
    """
    logs = []

    # Excelファイルを作成
    excel_buffer = io.BytesIO()

    with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
        for person_type, config in VisitListConfig.PERSON_TYPES.items():
            if person_type in df_dict and len(df_dict[person_type]) > 0:
                df = df_dict[person_type]
                sheet_name = config["sheet_name"]

                # Excelシートに書き込み
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                logs.append(f"{sheet_name}シート: {len(df)}件")

        # フォント設定と罫線削除を適用
        workbook = writer.book
        font = Font(name='游ゴシック Regular', size=11)

        for sheet_name in workbook.sheetnames:
            ws = workbook[sheet_name]

            # フォントと罫線の設定
            for row in ws.iter_rows():
                for cell in row:
                    cell.font = font
                    cell.border = Border()  # 罫線削除

            # 数値列にカンマ区切り書式を適用
            headers = [cell.value for cell in ws[1]]
            numeric_cols = []
            for idx, header in enumerate(headers, start=1):
                if header in ["退去手続き（実費）", "滞納残債", "入金予定金額", "月額賃料合計"]:
                    numeric_cols.append(idx)

            # 2行目以降の数値列にカンマ書式
            for row in ws.iter_rows(min_row=2):
                for col_idx in numeric_cols:
                    cell = row[col_idx - 1]
                    if cell.value is not None and isinstance(cell.value, (int, float)):
                        cell.number_format = '#,##0'

    excel_buffer.seek(0)
    return excel_buffer.getvalue(), logs


def process_visit_list(df_input: pd.DataFrame) -> Tuple[bytes, str, str, List[str]]:
    """
    訪問リスト作成のメイン処理

    Args:
        df_input: ContractList DataFrame

    Returns:
        Tuple[bytes, str, str, List[str]]: Excelバイト列、ファイル名、メッセージ、ログ
    """
    logs = []

    # 1. フィルタリング
    df_filtered, filter_logs = filter_records(df_input)
    logs.extend(filter_logs)

    if len(df_filtered) == 0:
        return None, "", "フィルタ条件に一致するレコードがありませんでした", logs

    # 2. 5つの人物タイプ別にデータ作成
    df_dict = {}
    for person_type, config in VisitListConfig.PERSON_TYPES.items():
        # 該当人物の現住所1が存在するレコードのみ
        address1_col = config["address1_col"]
        mask = df_filtered.iloc[:, address1_col].notna() & (df_filtered.iloc[:, address1_col] != '')
        df_person = df_filtered[mask].copy()

        if len(df_person) > 0:
            # 一括処理（ベクトル化）
            df_output = create_output_row_bulk(df_person, person_type, config)

            # 都道府県順でソート
            df_output = sort_by_prefecture(df_output)

            df_dict[person_type] = df_output

    # 3. Excelファイル生成
    today = datetime.now()
    filename = f"{today.strftime('%m%d')}訪問リスト.xlsx"
    excel_bytes, excel_logs = generate_excel(df_dict, filename)
    logs.extend(excel_logs)

    # 4. サマリーメッセージ
    total_records = sum(len(df) for df in df_dict.values())
    message = f"訪問リスト作成完了 - 合計 {total_records}件"

    return excel_bytes, filename, message, logs
