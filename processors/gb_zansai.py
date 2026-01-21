#!/usr/bin/env python3
"""
ガレージバンク残債取り込みプロセッサ

ガレージバンクの請求データExcelとContractListを突合し、
管理前滞納額一括CSVを出力する。

入力:
    - 情報連携シート_弁護士法人フェイス法律事務所.xlsx: 01_請求データシート
    - ContractList.csv: 自社システムの既存データ（委託先法人ID=7で絞ったもの）

出力:
    - ガレージバンク管理前取込_YYYYMMDD.csv: 管理番号と管理前滞納額の2列
"""

import pandas as pd
import io
from datetime import datetime
from typing import Tuple, List


class GBZansaiConfig:
    """ガレージバンク残債取り込みの設定"""

    # Excel読み込み設定
    EXCEL_SHEET_NAME = "01_請求データ"

    # 出力列
    OUTPUT_COLUMNS = ["管理番号", "管理前滞納額"]

    # 出力ファイル名フォーマット
    OUTPUT_FILENAME_FORMAT = "ガレージバンク管理前取込_{date}.csv"


def read_contract_list(file) -> pd.DataFrame:
    """
    ContractListを読み込む（cp932エンコーディング）

    Args:
        file: アップロードされたCSVファイル

    Returns:
        pd.DataFrame: ContractListのDataFrame
    """
    content = file.read()

    # cp932でデコード
    try:
        csv_str = content.decode('cp932')
    except UnicodeDecodeError:
        # フォールバック: shift_jis, utf-8-sig
        try:
            csv_str = content.decode('shift_jis')
        except UnicodeDecodeError:
            csv_str = content.decode('utf-8-sig')

    df = pd.read_csv(io.StringIO(csv_str), dtype=str)

    return df


def read_seikyu_data(file) -> pd.DataFrame:
    """
    請求データExcelを読み込む（01_請求データシート）

    Args:
        file: アップロードされたExcelファイル

    Returns:
        pd.DataFrame: 請求データのDataFrame
    """
    content = file.read()
    excel_buffer = io.BytesIO(content)

    df = pd.read_excel(
        excel_buffer,
        sheet_name=GBZansaiConfig.EXCEL_SHEET_NAME
    )

    return df


def match_data(seikyu_df: pd.DataFrame, contract_df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """
    請求データとContractListをマッチングする

    Args:
        seikyu_df: 請求データのDataFrame
        contract_df: ContractListのDataFrame

    Returns:
        Tuple[pd.DataFrame, List[str]]: マッチ結果と処理ログ
    """
    logs = []

    # 型を統一して比較（文字列に変換）
    seikyu_df = seikyu_df.copy()
    contract_df = contract_df.copy()

    seikyu_df["ユーザーID_str"] = seikyu_df["ユーザーID"].astype(str)
    contract_df["引継番号_str"] = contract_df["引継番号"].astype(str)

    # マッチング用の辞書を作成（引継番号 → 管理番号）
    hikitsugi_to_kanri = dict(zip(
        contract_df["引継番号_str"],
        contract_df["管理番号"]
    ))

    # 結果を格納するリスト
    results = []
    unmatched = []

    for _, row in seikyu_df.iterrows():
        user_id_str = str(row["ユーザーID"])
        seikyu_amount = row["請求総額"]

        if user_id_str in hikitsugi_to_kanri:
            kanri_no = hikitsugi_to_kanri[user_id_str]
            results.append({
                "管理番号": kanri_no,
                "管理前滞納額": seikyu_amount
            })
        else:
            unmatched.append(user_id_str)

    # マッチしなかったレコードのログ
    for user_id in unmatched:
        logs.append(f"⚠️ マッチしませんでした: ユーザーID {user_id}")

    # 結果のDataFrame
    if results:
        result_df = pd.DataFrame(results)
    else:
        result_df = pd.DataFrame(columns=GBZansaiConfig.OUTPUT_COLUMNS)

    return result_df, logs


def generate_output(df: pd.DataFrame) -> pd.DataFrame:
    """
    出力用のDataFrameを生成する

    Args:
        df: マッチ結果のDataFrame

    Returns:
        pd.DataFrame: 出力用DataFrame
    """
    # 列の順序を保証
    output_df = df[GBZansaiConfig.OUTPUT_COLUMNS].copy()
    return output_df


def process_gb_zansai(seikyu_file, contract_file) -> Tuple[pd.DataFrame, List[str], str]:
    """
    ガレージバンク残債取り込みのメイン処理関数

    Args:
        seikyu_file: 請求データExcelファイル
        contract_file: ContractList CSVファイル

    Returns:
        Tuple[pd.DataFrame, List[str], str]: 出力DataFrame, 処理ログ, ファイル名
    """
    logs = []

    # ファイル読み込み
    contract_df = read_contract_list(contract_file)
    seikyu_df = read_seikyu_data(seikyu_file)

    # 入力件数ログ
    logs.append(f"📂 請求データ: {len(seikyu_df)}件")
    logs.append(f"📂 ContractList: {len(contract_df)}件")

    # マッチング処理
    result_df, match_logs = match_data(seikyu_df, contract_df)
    logs.extend(match_logs)

    # マッチ件数ログ
    matched_count = len(result_df)
    unmatched_count = len(seikyu_df) - matched_count
    logs.append(f"✅ マッチ: {matched_count}件")
    if unmatched_count > 0:
        logs.append(f"⚠️ マッチなし: {unmatched_count}件")

    # 出力生成
    output_df = generate_output(result_df)

    # ファイル名生成
    today = datetime.now().strftime("%Y%m%d")
    filename = GBZansaiConfig.OUTPUT_FILENAME_FORMAT.format(date=today)

    return output_df, logs, filename
