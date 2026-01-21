"""
ガレージバンク催告書 契約者 プロセッサ

入力:
- ContractList_*.csv
- 情報連携シート_*.xlsx (01_請求データ)

処理:
1. 引継番号とユーザーIDでマッチング
2. フィルタ条件適用
3. 22列の出力データ生成
"""
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Tuple
from processors.common.detailed_logger import DetailedLogger


def match_billing_data(contract_df: pd.DataFrame, billing_df: pd.DataFrame) -> pd.DataFrame:
    """
    ContractListと請求データをマッチングする

    Args:
        contract_df: ContractListのDataFrame
        billing_df: 01_請求データのDataFrame

    Returns:
        マッチング結果のDataFrame（マッチしないレコードは除外）
    """
    if contract_df.empty or billing_df.empty:
        # 空のDataFrameを返す（マッチするものがないので全て除外）
        empty_df = pd.DataFrame()
        empty_df['請求金額'] = pd.Series(dtype='float64')
        empty_df['遅延損害金額'] = pd.Series(dtype='float64')
        empty_df['その他費用'] = pd.Series(dtype='float64')
        return empty_df

    # 引継番号（B列=1列目）を文字列として取得
    contract_df = contract_df.copy()
    contract_df['_引継番号_str'] = contract_df.iloc[:, 1].astype(str).str.strip()

    # 請求データのユーザーIDを文字列として取得
    billing_df = billing_df.copy()
    billing_df['_ユーザーID_str'] = billing_df['ユーザーID'].astype(str).str.strip()

    # マッチング用のカラムを準備
    billing_subset = billing_df[['_ユーザーID_str', '請求金額', '遅延損害金額', 'その他費用']].copy()

    # マージ（inner joinでマッチしないものは除外）
    result = contract_df.merge(
        billing_subset,
        left_on='_引継番号_str',
        right_on='_ユーザーID_str',
        how='inner'
    )

    # 一時カラムを削除
    result = result.drop(columns=['_引継番号_str', '_ユーザーID_str'])

    return result


def apply_gb_filters(df: pd.DataFrame) -> Tuple[pd.DataFrame, list]:
    """
    ガレージバンク用フィルタを適用する

    フィルタ条件:
    - 委託先法人ID = 7
    - 入金予定日 < 本日（空白含む）
    - 滞納残債 >= 1円
    - 入金予定金額 != 2, 3, 5
    - 回収ランク != 死亡決定, 破産決定, 弁護士介入

    Args:
        df: マッチング済みのDataFrame

    Returns:
        (フィルタ済みDataFrame, ログリスト)
    """
    logs = []
    initial_count = len(df)

    if df.empty:
        logs.append("入力データが空です")
        return df, logs

    # 委託先法人ID（118列目）= 7 でフィルタ
    before_count = len(df)
    df = df[df.iloc[:, 118] == 7]
    log = DetailedLogger.log_filter_result(before_count, len(df), "委託先法人ID（=7）")
    logs.append(log)

    if df.empty:
        return df, logs

    # 入金予定日（72列目）< 本日（空白含む）
    today = pd.Timestamp.now().normalize()
    before_count = len(df)
    df = df.copy()
    df['_入金予定日_converted'] = pd.to_datetime(df.iloc[:, 72], errors='coerce')
    # 過去の日付または空欄（NaT）のみを残す
    df = df[(df['_入金予定日_converted'] < today) | (df['_入金予定日_converted'].isna())]
    df = df.drop('_入金予定日_converted', axis=1)
    log = DetailedLogger.log_filter_result(before_count, len(df), "入金予定日（<本日、空白含む）")
    logs.append(log)

    if df.empty:
        return df, logs

    # 滞納残債（71列目）>= 1円
    before_count = len(df)
    # カンマを削除して数値に変換
    arrears_numeric = pd.to_numeric(
        df.iloc[:, 71].astype(str).str.replace(',', ''),
        errors='coerce'
    )
    df = df[arrears_numeric >= 1]
    log = DetailedLogger.log_filter_result(before_count, len(df), "滞納残債（>=1円）")
    logs.append(log)

    if df.empty:
        return df, logs

    # 入金予定金額（73列目）!= 2, 3, 5
    before_count = len(df)
    payment_amounts = pd.to_numeric(df.iloc[:, 73], errors='coerce')
    df = df[~payment_amounts.isin([2, 3, 5])]
    log = DetailedLogger.log_filter_result(before_count, len(df), "入金予定金額（2,3,5除外）")
    logs.append(log)

    if df.empty:
        return df, logs

    # 回収ランク（86列目）!= 死亡決定, 破産決定, 弁護士介入
    before_count = len(df)
    df = df[~df.iloc[:, 86].isin(['死亡決定', '破産決定', '弁護士介入'])]
    log = DetailedLogger.log_filter_result(before_count, len(df), "回収ランク（死亡決定・破産決定・弁護士介入除外）")
    logs.append(log)

    logs.append(f"共通フィルタリング完了: {initial_count}件 -> {len(df)}件")

    return df, logs


def filter_complete_address(df: pd.DataFrame) -> Tuple[pd.DataFrame, list]:
    """
    住所が完全なレコードのみ抽出する

    条件: 郵便番号・現住所1-3（22,23,24,25列目）が全て入力済み

    Args:
        df: フィルタ済みDataFrame

    Returns:
        (住所フィルタ済みDataFrame, ログリスト)
    """
    logs = []
    before_count = len(df)

    if df.empty:
        logs.append("入力データが空です")
        return df, logs

    # 住所フィルタ（22,23,24,25列目がすべて入力されている）
    df = df[
        df.iloc[:, 22].notna() & (df.iloc[:, 22] != '') &
        df.iloc[:, 23].notna() & (df.iloc[:, 23] != '') &
        df.iloc[:, 24].notna() & (df.iloc[:, 24] != '') &
        df.iloc[:, 25].notna() & (df.iloc[:, 25] != '')
    ]

    log = DetailedLogger.log_filter_result(before_count, len(df), "住所（郵便番号・現住所1-3が完全）")
    logs.append(log)

    return df, logs


def map_output_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    出力カラムをマッピングする（22列）

    Args:
        df: フィルタ済みDataFrame

    Returns:
        出力用DataFrame（22列）
    """
    if df.empty:
        return pd.DataFrame(columns=[
            '管理番号', '契約者氏名', '郵便番号', '現住所1', '現住所2', '現住所3',
            '滞納残債', '請求金額', '遅延損害金額', 'その他費用',
            '物件住所1', '物件住所2', '物件住所3', '物件名', '物件番号',
            '回収口座銀行CD', '回収口座銀行名', '回収口座支店CD', '回収口座支店名',
            '回収口座種類', '回収口座番号', '回収口座名義人'
        ])

    result_df = pd.DataFrame({
        '管理番号': df.iloc[:, 0],
        '契約者氏名': df.iloc[:, 20],
        '郵便番号': df.iloc[:, 22],
        '現住所1': df.iloc[:, 23],
        '現住所2': df.iloc[:, 24],
        '現住所3': df.iloc[:, 25],
        '滞納残債': df.iloc[:, 71],
        '請求金額': df['請求金額'],
        '遅延損害金額': df['遅延損害金額'],
        'その他費用': df['その他費用'],
        '物件住所1': df.iloc[:, 92],
        '物件住所2': df.iloc[:, 93],
        '物件住所3': df.iloc[:, 94],
        '物件名': df.iloc[:, 95],
        '物件番号': df.iloc[:, 96],
        '回収口座銀行CD': df.iloc[:, 34],
        '回収口座銀行名': df.iloc[:, 35],
        '回収口座支店CD': df.iloc[:, 36],
        '回収口座支店名': df.iloc[:, 37],
        '回収口座種類': df.iloc[:, 38],
        '回収口座番号': df.iloc[:, 39],
        '回収口座名義人': df.iloc[:, 40],
    })

    return result_df


def process_gb_notification(
    contract_df: pd.DataFrame,
    billing_df: pd.DataFrame
) -> Tuple[pd.DataFrame, str, str, list]:
    """
    ガレージバンク催告書を生成する

    Args:
        contract_df: ContractListのDataFrame
        billing_df: 01_請求データのDataFrame

    Returns:
        (result_df, filename, message, logs)
    """
    logs = []

    try:
        logs.append("処理開始: ガレージバンク催告書 契約者")
        logs.append(f"ContractList入力データ: {len(contract_df)}件")
        logs.append(f"請求データ入力データ: {len(billing_df)}件")

        # 1. マッチング処理
        matched_df = match_billing_data(contract_df, billing_df)
        logs.append(f"マッチング後: {len(matched_df)}件")

        if matched_df.empty:
            filename = f"ガレージバンク催告書{datetime.now().strftime('%y%m%d')}.xlsx"
            message = "マッチするデータがありませんでした"
            logs.append(message)
            return pd.DataFrame(columns=[
                '管理番号', '契約者氏名', '郵便番号', '現住所1', '現住所2', '現住所3',
                '滞納残債', '請求金額', '遅延損害金額', 'その他費用',
                '物件住所1', '物件住所2', '物件住所3', '物件名', '物件番号',
                '回収口座銀行CD', '回収口座銀行名', '回収口座支店CD', '回収口座支店名',
                '回収口座種類', '回収口座番号', '回収口座名義人'
            ]), filename, message, logs

        # 2. 共通フィルタ適用
        filtered_df, filter_logs = apply_gb_filters(matched_df)
        logs.extend(filter_logs)

        if filtered_df.empty:
            filename = f"ガレージバンク催告書{datetime.now().strftime('%y%m%d')}.xlsx"
            message = "フィルタ条件を満たすデータがありませんでした"
            logs.append(message)
            return map_output_columns(filtered_df), filename, message, logs

        # 3. 住所フィルタ適用
        address_filtered_df, address_logs = filter_complete_address(filtered_df)
        logs.extend(address_logs)

        # 4. 出力カラムマッピング
        result_df = map_output_columns(address_filtered_df)
        logs.append(f"出力カラムマッピング完了: {len(result_df)}件, {len(result_df.columns)}列")

        # 5. ファイル名生成
        filename = f"ガレージバンク催告書{datetime.now().strftime('%y%m%d')}.xlsx"

        message = f"ガレージバンク催告書を作成しました。出力件数: {len(result_df)}件"
        logs.append(f"処理完了: 出力件数 {len(result_df)}件")

        return result_df, filename, message, logs

    except Exception as e:
        logs.append(f"エラー発生: {str(e)}")
        raise Exception(f"処理中にエラーが発生しました: {str(e)}")
