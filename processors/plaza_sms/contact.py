import pandas as pd
import re
from datetime import datetime, date
from typing import Tuple, List

# SMS共通モジュールから関数とヘッダーをインポート
from processors.sms_common import (
    SMS_TEMPLATE_HEADERS,
    format_payment_deadline,
    read_csv_auto_encoding
)
from processors.common.detailed_logger import DetailedLogger


def process_plaza_sms_contact_data(file_content: bytes, payment_deadline_date: date) -> Tuple[pd.DataFrame, List[str], str, dict]:
    """
    プラザSMS緊急連絡人データ処理（Streamlit対応版）
    
    フィルタ条件:
    1. 委託先法人ID: 6のみ選択
    2. 回収ランク: 「弁護士介入」「死亡決定」「破産決定」除外
    3. 入金予定日: 前日以前が対象（当日は除外）
    4. 入金予定金額: 2,3,5,12を除外
    5. BE列　緊急連絡人１のTEL（携帯）: 090/080/070形式の携帯電話番号のみ
    
    データマッピング:
    - 電話番号: BE列「緊急連絡人１のTEL（携帯）」（列番号56）
    - (info1)契約者名: 緊急連絡人１氏名
    - (info2)物件名: 物件名 + 物件番号（全角スペース結合）
    - (info3)金額: 滞納残債（カンマ区切り表示）
    - (info4)銀行口座: 回収口座5項目（全角スペース結合）
    - (info5)メモ: 管理番号
    - 連絡人: 緊急連絡人１氏名
    - 支払期限: ユーザー指定日付（YYYY年MM月DD日形式）
    
    Args:
        file_content: アップロードされたCSVファイルの内容（bytes）
        payment_deadline_date: 支払期限日付（dateオブジェクト）
        
    Returns:
        tuple: (変換済みDF, ログリスト, 出力ファイル名, 統計情報)
    """
    try:
        # ログリスト初期化
        logs = []
        
        # CSVファイル読み込み（自動エンコーディング判定）
        df = read_csv_auto_encoding(file_content)

        initial_rows = len(df)
        logs.append(DetailedLogger.log_initial_load(initial_rows))
        
        # Filter 1: 委託先法人ID (Keep only 6)
        df['委託先法人ID'] = pd.to_numeric(df['委託先法人ID'], errors='coerce').fillna(-1).astype(int)
        
        # フィルター適用
        before_count = len(df)
        excluded_trustee = df[df['委託先法人ID'] != 6]
        df = df[df['委託先法人ID'] == 6]
        logs.append(DetailedLogger.log_filter_result(before_count, len(df), '委託先法人ID'))
        
        # 除外データの詳細を記録
        if len(excluded_trustee) > 0:
            detail = DetailedLogger.log_exclusion_details(
                excluded_trustee, 118, '委託先法人ID', 'id', top_n=3
            )
            if detail:
                logs.append(detail)
        
        # Filter 2: 入金予定日 (Keep dates before today and NaN)
        df['入金予定日'] = pd.to_datetime(df['入金予定日'], format='%Y/%m/%d', errors='coerce')
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        
        # フィルター適用
        before_count = len(df)
        excluded_dates = df[~(df['入金予定日'].isna() | (df['入金予定日'] < today))]
        df = df[df['入金予定日'].isna() | (df['入金予定日'] < today)]
        logs.append(DetailedLogger.log_filter_result(before_count, len(df), '入金予定日'))
        
        # 除外データの詳細を記録
        if len(excluded_dates) > 0:
            detail = DetailedLogger.log_exclusion_details(
                excluded_dates, 72, '入金予定日', 'date', top_n=3
            )
            if detail:
                logs.append(detail)
        
        # Filter 3: 入金予定金額 (Exclude specific amounts: 2, 3, 5, 12)
        payment_amount_exclude_numeric = [2, 3, 5, 12]
        payment_amount_exclude_string = ["2", "3", "5", "12"]
        df['入金予定金額_numeric'] = pd.to_numeric(df['入金予定金額'], errors='coerce')
        df['入金予定金額_string'] = df['入金予定金額'].astype(str)
        
        # フィルター適用
        before_count = len(df)
        excluded_amounts = df[(df['入金予定金額_numeric'].isin(payment_amount_exclude_numeric) | 
                              df['入金予定金額_string'].isin(payment_amount_exclude_string))]
        df = df[~(df['入金予定金額_numeric'].isin(payment_amount_exclude_numeric) | 
                  df['入金予定金額_string'].isin(payment_amount_exclude_string))]
        df = df.drop(['入金予定金額_numeric', '入金予定金額_string'], axis=1)
        logs.append(DetailedLogger.log_filter_result(before_count, len(df), '入金予定金額'))
        
        # 除外データの詳細を記録
        if len(excluded_amounts) > 0:
            detail = DetailedLogger.log_exclusion_details(
                excluded_amounts, 73, '入金予定金額', 'amount', top_n=3
            )
            if detail:
                logs.append(detail)
        
        # Filter 4: 回収ランク (Exclude specific ranks)
        collection_rank_exclude = ["弁護士介入", "死亡決定", "破産決定"]
        
        # フィルター適用
        before_count = len(df)
        excluded_ranks = df[df['回収ランク'].isin(collection_rank_exclude)]
        df = df[~df['回収ランク'].isin(collection_rank_exclude)]
        logs.append(DetailedLogger.log_filter_result(before_count, len(df), '回収ランク'))
        
        # 除外データの詳細を記録
        if len(excluded_ranks) > 0:
            detail = DetailedLogger.log_exclusion_details(
                excluded_ranks, 86, '回収ランク', 'category', top_n=3
            )
            if detail:
                logs.append(detail)
        
        # Filter 5: BE列　緊急連絡人１のTEL（携帯） (Keep only valid mobile phone numbers) - 列番号56を使用
        mobile_phone_regex = r'^(090|080|070)-\d{4}-\d{4}$'
        
        # BE列（列番号56）の電話番号を取得
        contact_phone_series = df.iloc[:, 56].astype(str).str.strip().replace('nan', '')
        
        def is_mobile_phone(phone_number):
            if phone_number == '':
                return False
            return bool(re.match(mobile_phone_regex, phone_number))
        
        # フィルター適用
        before_count = len(df)
        excluded_phones_mask = ~contact_phone_series.apply(is_mobile_phone)
        excluded_phones_df = df[excluded_phones_mask]
        df = df[contact_phone_series.apply(is_mobile_phone)]
        logs.append(DetailedLogger.log_filter_result(before_count, len(df), 'BE列緊急連絡人１TEL'))
        
        # 除外データの詳細を記録
        if len(excluded_phones_df) > 0:
            detail = DetailedLogger.log_exclusion_details(
                excluded_phones_df, 56, 'BE列緊急連絡人１TEL', 'phone', top_n=3
            )
            if detail:
                logs.append(detail)
        
        # Data mapping to output format - load from external template
        output_column_order = SMS_TEMPLATE_HEADERS
        
        # Create temporary column names for DataFrame construction (to handle empty column names)
        temp_column_order = []
        empty_col_counter = 1
        for col in output_column_order:
            if col == "":
                temp_column_order.append(f"_empty_{empty_col_counter}")
                empty_col_counter += 1
            else:
                temp_column_order.append(col)
        
        # Create output DataFrame with temporary column names
        output_df = pd.DataFrame(columns=temp_column_order)
        
        # Map data - BE列（列番号56）の電話番号を使用
        output_df['電話番号'] = df.iloc[:, 56].astype(str)
        output_df['(info1)契約者名'] = df['契約者氏名']
        
        # Combine property name and number
        output_df['(info2)物件名'] = df['物件名'].astype(str) + df['物件番号'].fillna('').astype(str).apply(
            lambda x: f'　{x}' if x and x != 'nan' else ''
        )
        
        # Format amount with comma separation
        df['滞納残債'] = df['滞納残債'].astype(str).str.replace(r'[^0-9,]', '', regex=True)
        output_df['(info3)金額'] = pd.to_numeric(
            df['滞納残債'].str.replace(',', ''), errors='coerce'
        ).fillna(0).astype(int).apply(lambda x: f'{x:,}')
        
        # Combine bank account information with full-width spaces
        output_df['(info4)銀行口座'] = (
            df['回収口座銀行名'].astype(str) + '　' +
            df['回収口座支店名'].astype(str) + '　' +
            df['回収口座種類'].astype(str) + '　' +
            df['回収口座番号'].astype(str).str.replace('="', '').str.replace('"', '') + '　' +
            df['回収口座名義人'].astype(str)
        )
        
        output_df['(info5)メモ'] = df['管理番号'].astype(str)
        
        # 連絡人列に緊急連絡人１氏名をマッピング
        output_df['連絡人'] = df['緊急連絡人１氏名'].astype(str)
        
        # Set payment deadline (BG column - column 59)
        payment_deadline_formatted = format_payment_deadline(payment_deadline_date)
        output_df['支払期限'] = payment_deadline_formatted
        
        # Fill empty columns and other remaining columns
        for col in temp_column_order:
            if col.startswith('_empty_') and col not in output_df.columns:
                output_df[col] = ''
            elif col in ['保証人'] and col not in output_df.columns:
                output_df[col] = ''
        
        # Ensure correct column order
        output_df = output_df[temp_column_order]
        
        # Create output filename
        date_str = datetime.now().strftime("%m%d")
        output_filename = f"{date_str}プラザSMS連絡人.csv"
        
        # Restore original column names from template (convert _empty_X back to blank)
        df_copy = output_df.copy()
        df_copy.columns = output_column_order
        
        # 最終結果のログ
        logs.append(DetailedLogger.log_final_result(len(output_df)))
        
        # 統計情報を辞書形式で作成
        stats = {
            'initial_rows': initial_rows,
            'processed_rows': len(output_df)
        }
        
        return df_copy, logs, output_filename, stats
        
    except Exception as e:
        raise Exception(f"プラザSMS連絡人処理エラー: {str(e)}")
