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
from domain.rules.business_rules import CLIENT_IDS, EXCLUDE_AMOUNTS
from processors.common.detailed_logger import DetailedLogger


def process_faith_sms_contract_data(file_content: bytes, payment_deadline_date: date) -> Tuple[pd.DataFrame, List[str], str, dict]:
    """
    フェイスSMS退去済み契約者データ処理（Streamlit対応版）
    
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
        
        # Filter 1: 委託先法人ID (Keep only 1, 2, 3, 4)
        trustee_ids_to_keep = CLIENT_IDS['faith']
        df['委託先法人ID'] = pd.to_numeric(df['委託先法人ID'], errors='coerce').fillna(-1).astype(int)
        
        # フィルター適用
        before_count = len(df)
        excluded_trustee = df[~df['委託先法人ID'].isin(trustee_ids_to_keep)]
        df = df[df['委託先法人ID'].isin(trustee_ids_to_keep)]
        logs.append(DetailedLogger.log_filter_result(before_count, len(df), '委託先法人ID'))
        
        # 除外データの詳細を記録
        if len(excluded_trustee) > 0:
            detail = DetailedLogger.log_exclusion_details(
                excluded_trustee, 118, '委託先法人ID', 'id', top_n=3
            )
            if detail:
                logs.append(detail)
        
        # Filter 2: 入金予定日 (Keep empty or dates before today)
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
        
        # Filter 3: 入金予定金額 (Exclude specific amounts: 2, 3, 5 as numeric or string values)
        payment_amount_exclude_numeric = EXCLUDE_AMOUNTS['faith']
        payment_amount_exclude_string = [str(x) for x in EXCLUDE_AMOUNTS['faith']]
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
        collection_rank_exclude = ["弁護士介入", "破産決定", "死亡決定"]
        
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

        # Filter 5: BT列　滞納残債 (Keep only >= 1)
        # BT列は列番号71（0ベース）
        arrears_column = df.iloc[:, 71]
        arrears_numeric = pd.to_numeric(
            arrears_column.astype(str).str.replace(',', ''),
            errors='coerce'
        )

        # フィルター適用
        before_count = len(df)
        valid_arrears_mask = arrears_numeric >= 1
        excluded_arrears = df[~valid_arrears_mask]
        df = df[valid_arrears_mask]
        logs.append(DetailedLogger.log_filter_result(before_count, len(df), '滞納残債（1円以上）'))

        # 除外データの詳細を記録
        if len(excluded_arrears) > 0:
            detail = DetailedLogger.log_exclusion_details(
                excluded_arrears, 71, '滞納残債', 'amount', top_n=3
            )
            if detail:
                logs.append(detail)

        # Filter 6: TEL携帯 (Keep only valid mobile phone numbers)
        mobile_phone_regex = r'^(090|080|070)-\d{4}-\d{4}$'
        df['TEL携帯'] = df['TEL携帯'].astype(str).str.strip().replace('nan', '')
        
        def is_mobile_phone(phone_number):
            if phone_number == '':
                return False
            return bool(re.match(mobile_phone_regex, phone_number))
        
        # フィルター適用
        before_count = len(df)
        excluded_phones = df[~df['TEL携帯'].apply(is_mobile_phone)]
        df = df[df['TEL携帯'].apply(is_mobile_phone)]
        logs.append(DetailedLogger.log_filter_result(before_count, len(df), 'TEL携帯'))
        
        # 除外データの詳細を記録
        if len(excluded_phones) > 0:
            detail = DetailedLogger.log_exclusion_details(
                excluded_phones, 27, 'TEL携帯', 'phone', top_n=3
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
        
        # Map data
        output_df['電話番号'] = df['TEL携帯'].astype(str)
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
        
        # Set payment deadline (BG column - column 59)
        payment_deadline_formatted = format_payment_deadline(payment_deadline_date)
        output_df['支払期限'] = payment_deadline_formatted
        
        # Fill empty columns and other remaining columns
        for col in temp_column_order:
            if col.startswith('_empty_') and col not in output_df.columns:
                output_df[col] = ''
            elif col in ['保証人', '連絡人'] and col not in output_df.columns:
                output_df[col] = ''
        
        # Ensure correct column order
        output_df = output_df[temp_column_order]
        
        # Create output filename
        date_str = datetime.now().strftime("%m%d")
        output_filename = f"{date_str}フェイスSMS契約者.csv"
        
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
        raise Exception(f"FAITH SMS退去済み契約者処理エラー: {str(e)}")
