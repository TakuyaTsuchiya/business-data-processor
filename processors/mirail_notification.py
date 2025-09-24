"""
ミライル（フェイス封筒）用リスト作成プロセッサー
契約者、保証人、連絡人の郵送用リストを生成する
"""
import pandas as pd
import numpy as np
import io
from datetime import datetime
from typing import Tuple, Optional
from processors.common.detailed_logger import DetailedLogger


def read_csv_auto_encoding(file_content: bytes) -> pd.DataFrame:
    """アップロードされたCSVファイルを自動エンコーディング判定で読み込み"""
    encodings = ['utf-8', 'utf-8-sig', 'shift_jis', 'cp932']

    for enc in encodings:
        try:
            return pd.read_csv(io.BytesIO(file_content), encoding=enc, dtype=str)
        except Exception:
            continue

    raise ValueError("CSVファイルの読み込みに失敗しました。エンコーディングを確認してください。")


def process_mirail_notification(
    file_content: bytes,
    target_type: str,  # 'contractor', 'guarantor', 'contact'
    client_pattern: str  # 'included' or 'excluded'
) -> Tuple[Optional[pd.DataFrame], Optional[str], str, list]:
    """
    ミライル（フェイス封筒）用リストを生成する

    Args:
        file_content: ContractList*.csvのバイトデータ
        target_type: 対象者タイプ（contractor/guarantor/contact）
        client_pattern: クライアントCDパターン（included=1,4,5 / excluded=1,4,5以外）

    Returns:
        (result_df, filename, message, logs)
        0件の場合: (None, None, エラーメッセージ, logs)
    """
    logger = DetailedLogger()

    try:
        # 処理開始
        target_name_map = {
            'contractor': '契約者',
            'guarantor': '保証人',
            'contact': '連絡人'
        }
        target_name = target_name_map.get(target_type, target_type)
        pattern_text = "（1,4,5）" if client_pattern == 'included' else "（1,4,5以外）"

        logger.log(f"処理開始: {target_name}{pattern_text}")

        # CSV読み込み
        logger.log("CSVファイル読み込み中...")
        df = read_csv_auto_encoding(file_content)
        logger.log(f"入力データ: {len(df)}件, {len(df.columns)}列")

        # データ型変換
        logger.log("データ型変換中...")
        df['委託先法人ID'] = pd.to_numeric(df['委託先法人ID'], errors='coerce')
        df['クライアントCD'] = pd.to_numeric(df['クライアントCD'], errors='coerce')
        df['入金予定金額'] = pd.to_numeric(df['入金予定金額'], errors='coerce')
        df['入金予定日'] = pd.to_datetime(df['入金予定日'], errors='coerce', format='%Y/%m/%d')

        # 1. 委託先法人IDフィルタ（5と空白のみ）
        before_count = len(df)
        df_filtered = df[(df['委託先法人ID'] == 5) | df['委託先法人ID'].isna()]
        after_count = len(df_filtered)
        logger.log(f"委託先法人IDフィルタ（5と空白のみ）: {before_count}件 → {after_count}件（除外: {before_count - after_count}件）")

        # 2. 入金予定日フィルタ（本日より前、本日除く）
        before_count = len(df_filtered)
        today = pd.Timestamp(datetime.now().date())
        # 入金予定日がNaTでない かつ 本日より前
        df_filtered = df_filtered[
            df_filtered['入金予定日'].notna() &
            (df_filtered['入金予定日'] < today)
        ]
        after_count = len(df_filtered)
        logger.log(f"入金予定日フィルタ（本日より前）: {before_count}件 → {after_count}件（除外: {before_count - after_count}件）")

        # 3. 入金予定金額フィルタ（2,3,5,12を除外）
        before_count = len(df_filtered)
        df_filtered = df_filtered[~df_filtered['入金予定金額'].isin([2, 3, 5, 12])]
        after_count = len(df_filtered)
        logger.log(f"入金予定金額フィルタ（2,3,5,12除外）: {before_count}件 → {after_count}件（除外: {before_count - after_count}件）")

        # 4. 回収ランクフィルタ（弁護士介入のみ）
        before_count = len(df_filtered)
        df_filtered = df_filtered[df_filtered['回収ランク'] == '弁護士介入']
        after_count = len(df_filtered)
        logger.log(f"回収ランクフィルタ（弁護士介入のみ）: {before_count}件 → {after_count}件（除外: {before_count - after_count}件）")

        # 5. クライアントCDフィルタ
        before_count = len(df_filtered)
        if client_pattern == 'included':
            # 1,4,5を選択
            df_filtered = df_filtered[df_filtered['クライアントCD'].isin([1, 4, 5])]
            logger.log(f"クライアントCDフィルタ（1,4,5を選択）: {before_count}件 → {len(df_filtered)}件（除外: {before_count - len(df_filtered)}件）")
        else:
            # 1,4,5,10,40を除外
            df_filtered = df_filtered[~df_filtered['クライアントCD'].isin([1, 4, 5, 10, 40])]
            logger.log(f"クライアントCDフィルタ（1,4,5,10,40を除外）: {before_count}件 → {len(df_filtered)}件（除外: {before_count - len(df_filtered)}件）")

        # 6. 対象別住所チェック
        before_count = len(df_filtered)
        if target_type == 'contractor':
            df_filtered = check_contractor_address(df_filtered, logger)
        elif target_type == 'guarantor':
            df_filtered = check_guarantor_address(df_filtered, logger)
        elif target_type == 'contact':
            df_filtered = check_contact_address(df_filtered, logger)

        after_count = len(df_filtered)
        logger.log(f"住所完全性チェック後: {before_count}件 → {after_count}件（除外: {before_count - after_count}件）")

        # 結果チェック
        if len(df_filtered) == 0:
            message = f"フィルタ条件に該当するデータが見つかりませんでした。"
            logger.log(message)
            return None, None, message, logger.get_logs()

        # ファイル名生成
        now = datetime.now()
        pattern_suffix = "" if client_pattern == 'included' else "以外"
        filename = f"{now.strftime('%m%d')}{target_name}（1,4,5{pattern_suffix}）.xlsx"

        # 成功メッセージ
        message = f"{target_name}{pattern_text}のリストを作成しました。出力件数: {len(df_filtered)}件"
        logger.log(f"処理完了: 出力件数 {len(df_filtered)}件")
        logger.log(f"出力ファイル名: {filename}")

        return df_filtered, filename, message, logger.get_logs()

    except Exception as e:
        error_message = f"処理中にエラーが発生しました: {str(e)}"
        logger.log(error_message)
        return None, None, error_message, logger.get_logs()


def check_contractor_address(df: pd.DataFrame, logger: DetailedLogger) -> pd.DataFrame:
    """契約者の住所完全性チェック"""
    logger.log("契約者住所完全性チェック:")

    # 列インデックス22-25（W-Z列）: 郵便番号、現住所1、現住所2、現住所3
    address_cols = df.columns[22:26].tolist()  # 0-indexedなので22から25まで

    # 各列の空欄をチェック
    for i, col in enumerate(address_cols):
        null_count = df[col].isna().sum() + (df[col] == '').sum()
        if null_count > 0:
            col_name = ['郵便番号', '現住所1', '現住所2', '現住所3'][i]
            logger.log(f"  - {col_name}空欄: {null_count}件")

    # いずれかの列が空欄（NaNまたは空文字）の行を除外
    valid_mask = True
    for col in address_cols:
        valid_mask = valid_mask & df[col].notna() & (df[col] != '')

    return df[valid_mask]


def check_guarantor_address(df: pd.DataFrame, logger: DetailedLogger) -> pd.DataFrame:
    """保証人の住所完全性チェック"""
    logger.log("保証人住所完全性チェック:")

    # 保証人1（列インデックス41-45）: AP-AT列
    g1_name_col = df.columns[41]  # AP列: 保証人１氏名
    g1_postal = df.columns[42]    # AQ列: 郵便番号
    g1_addr1 = df.columns[43]     # AR列: 現住所1
    g1_addr2 = df.columns[44]     # AS列: 現住所2
    g1_addr3 = df.columns[45]     # AT列: 現住所3

    # 保証人2（列インデックス48-52）: AW-BA列
    g2_name_col = df.columns[48]  # AW列: 保証人２氏名
    g2_postal = df.columns[49]    # AX列: 郵便番号
    g2_addr1 = df.columns[50]     # AY列: 現住所1
    g2_addr2 = df.columns[51]     # AZ列: 現住所2
    g2_addr3 = df.columns[52]     # BA列: 現住所3

    # 保証人1の住所完全性チェック
    g1_address_cols = [g1_postal, g1_addr1, g1_addr2, g1_addr3]
    g1_complete = True
    for col in g1_address_cols:
        g1_complete = g1_complete & df[col].notna() & (df[col] != '')

    # 保証人1の空欄数をログ
    for i, col in enumerate(g1_address_cols):
        null_count = df[col].isna().sum() + (df[col] == '').sum()
        if null_count > 0:
            col_name = ['郵便番号', '現住所1', '現住所2', '現住所3'][i]
            logger.log(f"  - 保証人1{col_name}空欄: {null_count}件")

    # 保証人2が存在する行をチェック
    g2_exists = df[g2_name_col].notna() & (df[g2_name_col] != '')
    g2_count = g2_exists.sum()

    if g2_count > 0:
        logger.log(f"  保証人2が存在: {g2_count}件")

        # 保証人2の住所完全性チェック
        g2_address_cols = [g2_postal, g2_addr1, g2_addr2, g2_addr3]
        g2_complete = True
        for col in g2_address_cols:
            g2_complete = g2_complete & df[col].notna() & (df[col] != '')

        # 保証人2の空欄数をログ（保証人2が存在する行のみ）
        for i, col in enumerate(g2_address_cols):
            null_count = (df[g2_exists][col].isna().sum() +
                         (df[g2_exists][col] == '').sum())
            if null_count > 0:
                col_name = ['郵便番号', '現住所1', '現住所2', '現住所3'][i]
                logger.log(f"  - 保証人2{col_name}空欄: {null_count}件")

        # 保証人2が存在しない場合は保証人1の住所が完全であればOK
        # 保証人2が存在する場合は少なくとも一方の住所が完全であればOK
        valid_mask = g1_complete | (g2_exists & g2_complete) | (~g2_exists & g1_complete)
    else:
        # 保証人2が存在しない場合は保証人1の住所のみチェック
        valid_mask = g1_complete

    excluded_count = (~valid_mask).sum()
    logger.log(f"  両保証人とも住所不完全で除外: {excluded_count}件")

    return df[valid_mask]


def check_contact_address(df: pd.DataFrame, logger: DetailedLogger) -> pd.DataFrame:
    """緊急連絡人の住所完全性チェック"""
    logger.log("緊急連絡人住所完全性チェック:")

    # 緊急連絡人1（列インデックス）: BD, BF-BI列
    e1_name_col = df.columns[55]  # BD列: 緊急連絡人１氏名
    e1_postal = df.columns[57]    # BF列: 郵便番号
    e1_addr1 = df.columns[58]     # BG列: 現住所1
    e1_addr2 = df.columns[59]     # BH列: 現住所2
    e1_addr3 = df.columns[60]     # BI列: 現住所3

    # 緊急連絡人2（列インデックス）: BK-BO列
    e2_name_col = df.columns[62]  # BK列: 緊急連絡人２氏名
    e2_postal = df.columns[63]    # BL列: 郵便番号
    e2_addr1 = df.columns[64]     # BM列: 現住所1
    e2_addr2 = df.columns[65]     # BN列: 現住所2
    e2_addr3 = df.columns[66]     # BO列: 現住所3

    # 緊急連絡人1の住所完全性チェック
    e1_address_cols = [e1_postal, e1_addr1, e1_addr2, e1_addr3]
    e1_complete = True
    for col in e1_address_cols:
        e1_complete = e1_complete & df[col].notna() & (df[col] != '')

    # 緊急連絡人1の空欄数をログ
    for i, col in enumerate(e1_address_cols):
        null_count = df[col].isna().sum() + (df[col] == '').sum()
        if null_count > 0:
            col_name = ['郵便番号', '現住所1', '現住所2', '現住所3'][i]
            logger.log(f"  - 緊急連絡人1{col_name}空欄: {null_count}件")

    # 緊急連絡人2が存在する行をチェック
    e2_exists = df[e2_name_col].notna() & (df[e2_name_col] != '')
    e2_count = e2_exists.sum()

    if e2_count > 0:
        logger.log(f"  緊急連絡人2が存在: {e2_count}件")

        # 緊急連絡人2の住所完全性チェック
        e2_address_cols = [e2_postal, e2_addr1, e2_addr2, e2_addr3]
        e2_complete = True
        for col in e2_address_cols:
            e2_complete = e2_complete & df[col].notna() & (df[col] != '')

        # 緊急連絡人2の空欄数をログ（緊急連絡人2が存在する行のみ）
        for i, col in enumerate(e2_address_cols):
            null_count = (df[e2_exists][col].isna().sum() +
                         (df[e2_exists][col] == '').sum())
            if null_count > 0:
                col_name = ['郵便番号', '現住所1', '現住所2', '現住所3'][i]
                logger.log(f"  - 緊急連絡人2{col_name}空欄: {null_count}件")

        # 少なくとも一方の住所が完全であればOK
        valid_mask = e1_complete | (e2_exists & e2_complete) | (~e2_exists & e1_complete)
    else:
        # 緊急連絡人2が存在しない場合は緊急連絡人1の住所のみチェック
        valid_mask = e1_complete

    excluded_count = (~valid_mask).sum()
    logger.log(f"  両緊急連絡人とも住所不完全で除外: {excluded_count}件")

    return df[valid_mask]