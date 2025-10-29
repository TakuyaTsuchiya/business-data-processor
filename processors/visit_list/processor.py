"""
訪問リスト作成プロセッサー
Business Data Processor

ContractList.csvから訪問スタッフ用の訪問リストExcelを生成
お手本の32列構造に準拠
"""

import pandas as pd
import io
from datetime import datetime
from typing import Tuple, List, Dict
import sys
import os
from openpyxl import Workbook
from openpyxl.styles import Font

# 共通定義のインポート
processors_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if processors_dir not in sys.path:
    sys.path.append(processors_dir)

from common.contract_list_columns import ContractListColumns as COL
from common.prefecture_order import get_prefecture_order, extract_prefecture_from_address


# お手本の32列構造定義
OUTPUT_COLUMNS = [
    "管理番号",           # 0
    "最新契約種類",        # 1
    "入居ステータス",       # 2
    "滞納ステータス",       # 3
    "退去手続き（実費）",    # 4
    "更新契約手数料",       # 5
    "営業担当者",          # 6
    "[人物]氏名",          # 7 ← 各シートで変わる
    "",                   # 8 結合住所（空白ヘッダー）
    "現住所1",            # 9 ← 各シートで変わる
    "現住所2",            # 10
    "現住所3",            # 11
    "保証人１氏名",        # 12
    "",                   # 13 結合住所（空白ヘッダー）
    "現住所1.1",          # 14
    "現住所2.1",          # 15
    "現住所3.1",          # 16
    "緊急連絡人１氏名",     # 17
    "",                   # 18 結合住所（空白ヘッダー）
    "現住所1.2",          # 19
    "現住所2.2",          # 20
    "現住所3.2",          # 21
    "滞納残債",            # 22
    "入金予定日",          # 23
    "入金予定金額",         # 24
    "月額賃料合計",         # 25
    "回収ランク",          # 26
    "クライアントCD",       # 27
    "クライアント名",       # 28
    "委託先法人ID",        # 29
    "委託先法人名",         # 30
    "解約日",             # 31
]


class VisitListConfig:
    """訪問リスト作成の設定・定数"""

    # フィルタ条件（固定値）
    FILTER_COLLECTION_RANKS = ["交渉困難", "死亡決定", "弁護士介入"]
    FILTER_PAYMENT_AMOUNTS_EXCLUDE = [2, 3, 5]

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


def read_csv_auto_encoding(file_content: bytes) -> pd.DataFrame:
    """CSVファイルを自動エンコーディング判定で読み込み"""
    encodings = ['cp932', 'shift_jis', 'utf-8-sig', 'utf-8']

    for enc in encodings:
        try:
            return pd.read_csv(io.BytesIO(file_content), encoding=enc)
        except Exception:
            continue

    raise ValueError("CSVファイルの読み込みに失敗しました。")


def filter_records(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """フィルタリング処理"""
    logs = []
    initial_count = len(df)
    logs.append(f"入力レコード数: {initial_count}件")

    # 1. 回収ランクフィルタ
    mask_rank = df.iloc[:, 86].isin(VisitListConfig.FILTER_COLLECTION_RANKS)
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
    mask_amount = ~payment_amount_numeric.isin(VisitListConfig.FILTER_PAYMENT_AMOUNTS_EXCLUDE)
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


def combine_address(address1, address2, address3) -> str:
    """住所を結合"""
    parts = []
    for addr in [address1, address2, address3]:
        if pd.notna(addr) and str(addr).strip() != '':
            parts.append(str(addr).strip())
    return ''.join(parts)


def create_output_row(row: pd.Series, person_type: str, config: Dict) -> Dict:
    """
    ContractListの1行から出力用の32列データを作成

    Args:
        row: ContractListの1行
        person_type: 人物種類（"contractor", "guarantor1" など）
        config: 人物別の列定義

    Returns:
        Dict: 32列のデータ辞書
    """
    # 基本情報（全シート共通）
    output = {
        "管理番号": row.iloc[0],
        "最新契約種類": row.iloc[2],
        "入居ステータス": row.iloc[14],
        "滞納ステータス": row.iloc[15],
        "退去手続き（実費）": row.iloc[17],
        "更新契約手数料": row.iloc[18],
        "営業担当者": row.iloc[19],
    }

    # 該当人物の情報（列7-11）
    person_name = row.iloc[config["name_col"]]
    person_addr1 = row.iloc[config["address1_col"]]
    person_addr2 = row.iloc[config["address2_col"]]
    person_addr3 = row.iloc[config["address3_col"]]
    person_combined = combine_address(person_addr1, person_addr2, person_addr3)

    output["[人物]氏名"] = person_name
    output[""] = person_combined  # 結合住所（空白ヘッダー）
    output["現住所1"] = person_addr1
    output["現住所2"] = person_addr2
    output["現住所3"] = person_addr3

    # 保証人1の情報（列12-16）
    g1_name = row.iloc[41]
    g1_addr1 = row.iloc[43]
    g1_addr2 = row.iloc[44]
    g1_addr3 = row.iloc[45]
    g1_combined = combine_address(g1_addr1, g1_addr2, g1_addr3)

    output["保証人１氏名"] = g1_name
    # 2つ目の空白列
    output_list = list(output.items())
    output_list.append(("", g1_combined))  # 結合住所
    output = dict(output_list)

    output["現住所1.1"] = g1_addr1
    output["現住所2.1"] = g1_addr2
    output["現住所3.1"] = g1_addr3

    # 連絡人1の情報（列17-21）
    c1_name = row.iloc[55]
    c1_addr1 = row.iloc[58]
    c1_addr2 = row.iloc[59]
    c1_addr3 = row.iloc[60]
    c1_combined = combine_address(c1_addr1, c1_addr2, c1_addr3)

    output["緊急連絡人１氏名"] = c1_name
    # 3つ目の空白列
    output_list = list(output.items())
    output_list.append(("", c1_combined))  # 結合住所
    output = dict(output_list)

    output["現住所1.2"] = c1_addr1
    output["現住所2.2"] = c1_addr2
    output["現住所3.2"] = c1_addr3

    # 滞納・金額情報（列22-26）
    output["滞納残債"] = row.iloc[71]
    output["入金予定日"] = row.iloc[72]
    output["入金予定金額"] = row.iloc[73]
    output["月額賃料合計"] = row.iloc[84]
    output["回収ランク"] = row.iloc[86]

    # クライアント情報（列27-31）
    output["クライアントCD"] = row.iloc[97]
    output["クライアント名"] = row.iloc[98]
    output["委託先法人ID"] = row.iloc[118]
    output["委託先法人名"] = row.iloc[119]
    output["解約日"] = row.iloc[120]

    return output


def extract_person_data(
    df: pd.DataFrame,
    person_type: str,
    config: Dict
) -> pd.DataFrame:
    """特定人物のデータを抽出し、32列形式に変換"""
    # 該当人物の住所1が空白のレコードを除外
    address1_col = df.iloc[:, config["address1_col"]]
    mask_address = address1_col.notna() & (address1_col != '') & (address1_col != ' ')
    df_person = df[mask_address].copy()

    if len(df_person) == 0:
        return pd.DataFrame()

    # 各行を32列形式に変換
    output_rows = []
    for idx, row in df_person.iterrows():
        output_row = create_output_row(row, person_type, config)
        output_rows.append(output_row)

    df_output = pd.DataFrame(output_rows, columns=OUTPUT_COLUMNS)

    return df_output


def sort_by_prefecture(df: pd.DataFrame) -> pd.DataFrame:
    """都道府県順（北→南）でソート"""
    # 現住所1（列9）から都道府県を抽出し、順序番号を取得
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

        # フォント設定を適用
        workbook = writer.book
        font = Font(name='游ゴシック', size=11)

        for sheet_name in workbook.sheetnames:
            ws = workbook[sheet_name]
            for row in ws.iter_rows():
                for cell in row:
                    cell.font = font

    excel_buffer.seek(0)
    return excel_buffer.getvalue(), logs


def process_visit_list(file_content: bytes) -> Tuple[bytes, List[str], str]:
    """訪問リスト作成のメイン処理"""
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
                df_person_sorted = sort_by_prefecture(df_person)
                df_dict[person_type] = df_person_sorted
                logs.append(f"{config['name']}: {len(df_person_sorted)}件")
            else:
                logs.append(f"{config['name']}: 0件（スキップ）")

        # 4. Excelファイル生成
        logs.append("\n=== Excelファイル生成 ===")
        today_str = datetime.now().strftime("%m%d")
        output_filename = f"{today_str}{VisitListConfig.OUTPUT_FILE_PREFIX}.xlsx"

        excel_content, excel_logs = generate_excel(df_dict, output_filename)
        logs.extend(excel_logs)

        logs.append(f"\n処理完了: {output_filename}")

        return excel_content, logs, output_filename

    except Exception as e:
        raise Exception(f"訪問リスト作成処理エラー: {str(e)}")
