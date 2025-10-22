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

def process_mirail_sms_contract_data(file_content: bytes, payment_deadline_date: date) -> Tuple[pd.DataFrame, List[str], str, dict]:
    """
    ミライルSMS契約者データ処理（Streamlit対応版）
    
    フィルタ条件:
    1. DO列　委託先法人ID: 5と空白セルのみ選択
    2. CI列　回収ランク: 「弁護士介入」「訴訟中」のみ除外
    3. BU列　入金予定日: 前日以前が対象（当日は除外）
    4. BV列　入金予定金額: 2,3,5,12を除外
    5. AB列　TEL携帯: 090/080/070形式の携帯電話番号のみ
    6. 残債フィルタ: なし（全残債対象）
    
    データマッピング:
    - 電話番号: AB列「TEL携帯」
    - (info1)契約者名: 契約者氏名
    - (info2)物件名: 物件名 + 物件番号（全角スペース結合）
    - (info3)金額: 滞納残債（カンマ区切り表示）
    - (info4)銀行口座: 回収口座5項目（全角スペース結合）
    - (info5)メモ: 管理番号
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
        
        # Filter 1: DO列　委託先法人ID (Keep only 5 and blank)
        # DO列は列番号118（0ベース）
        trustee_id_column = df.iloc[:, 118].astype(str).str.strip()
        
        # 5または空白（NaN含む）のみ保持
        valid_trustee_mask = (trustee_id_column == '5') | (trustee_id_column == '') | (trustee_id_column == 'nan')
        
        # フィルター適用
        before_count = len(df)
        excluded_trustee = df[~valid_trustee_mask]
        df = df[valid_trustee_mask]
        logs.append(DetailedLogger.log_filter_result(before_count, len(df), '委託先法人ID'))
        
        # 除外データの詳細を記録
        if len(excluded_trustee) > 0:
            detail = DetailedLogger.log_exclusion_details(
                excluded_trustee, 118, '委託先法人ID', 'id', top_n=3
            )
            if detail:
                logs.append(detail)
        
        # Filter 2: BU列　入金予定日 (Keep dates before today, exclude today and future)
        # BU列は列番号72（0ベース）
        payment_date_column = df.iloc[:, 72]
        df['入金予定日_parsed'] = pd.to_datetime(payment_date_column, format='%Y/%m/%d', errors='coerce')
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        
        # 前日以前のみ保持（当日は除外）
        valid_date_mask = df['入金予定日_parsed'].isna() | (df['入金予定日_parsed'] < today)
        
        # フィルター適用
        before_count = len(df)
        excluded_dates = df[~valid_date_mask]
        df = df[valid_date_mask]
        logs.append(DetailedLogger.log_filter_result(before_count, len(df), '入金予定日'))
        
        # 除外データの詳細を記録
        if len(excluded_dates) > 0:
            detail = DetailedLogger.log_exclusion_details(
                excluded_dates, 72, '入金予定日', 'date', top_n=3
            )
            if detail:
                logs.append(detail)
        
        # Filter 3: BV列　入金予定金額 (Exclude specific amounts: 2, 3, 5, 12)
        # BV列は列番号73（0ベース）
        payment_amount_exclude_numeric = [2, 3, 5, 12]
        payment_amount_exclude_string = ["2", "3", "5", "12"]
        
        payment_amount_column = df.iloc[:, 73]
        df['入金予定金額_numeric'] = pd.to_numeric(payment_amount_column, errors='coerce')
        df['入金予定金額_string'] = payment_amount_column.astype(str)
        
        # フィルター適用
        before_count = len(df)
        excluded_amounts = df[(df['入金予定金額_numeric'].isin(payment_amount_exclude_numeric) | 
                              df['入金予定金額_string'].isin(payment_amount_exclude_string))]
        valid_amount_mask = ~(df['入金予定金額_numeric'].isin(payment_amount_exclude_numeric) | 
                             df['入金予定金額_string'].isin(payment_amount_exclude_string))
        df = df[valid_amount_mask]
        df = df.drop(['入金予定金額_numeric', '入金予定金額_string'], axis=1)
        logs.append(DetailedLogger.log_filter_result(before_count, len(df), '入金予定金額'))
        
        # 除外データの詳細を記録
        if len(excluded_amounts) > 0:
            detail = DetailedLogger.log_exclusion_details(
                excluded_amounts, 73, '入金予定金額', 'amount', top_n=3
            )
            if detail:
                logs.append(detail)
        
        # Filter 4: CI列　回収ランク (Exclude specific ranks: 弁護士介入, 訴訟中)
        # CI列は列番号86（0ベース）
        collection_rank_exclude = ["弁護士介入", "訴訟中"]
        collection_rank_column = df.iloc[:, 86]
        
        # フィルター適用
        before_count = len(df)
        excluded_ranks = df[collection_rank_column.isin(collection_rank_exclude)]
        df = df[~collection_rank_column.isin(collection_rank_exclude)]
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

        # Filter 6: AB列　TEL携帯 (Keep only valid mobile phone numbers)
        # AB列は列番号27（0ベース）
        mobile_phone_regex = r'^(090|080|070)-\d{4}-\d{4}$'
        mobile_phone_column = df.iloc[:, 27].astype(str).str.strip().replace('nan', '')
        
        def is_mobile_phone(phone_number):
            if phone_number == '':
                return False
            return bool(re.match(mobile_phone_regex, phone_number))
        
        # フィルター適用
        before_count = len(df)
        excluded_phones_mask = ~mobile_phone_column.apply(is_mobile_phone)
        excluded_phones_df = df[excluded_phones_mask]
        df = df[mobile_phone_column.apply(is_mobile_phone)]
        logs.append(DetailedLogger.log_filter_result(before_count, len(df), 'AB列TEL携帯'))
        
        # 除外データの詳細を記録
        if len(excluded_phones_df) > 0:
            detail = DetailedLogger.log_exclusion_details(
                excluded_phones_df, 27, 'AB列TEL携帯', 'phone', top_n=3
            )
            if detail:
                logs.append(detail)
        
        # Data mapping to output format - use predefined headers
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
        
        # Map data - AB列（列番号27）の電話番号を使用
        output_df['電話番号'] = df.iloc[:, 27].astype(str)
        output_df['(info1)契約者名'] = df['契約者氏名']
        
        # Combine property name and number with full-width space
        output_df['(info2)物件名'] = df['物件名'].astype(str) + df['物件番号'].fillna('').astype(str).apply(
            lambda x: f'　{x}' if x and x != 'nan' else ''
        )
        
        # Format amount with comma separation
        df['滞納残債'] = df['滞納残債'].astype(str).str.replace(r'[^0-9,]', '', regex=True)
        output_df['(info3)金額'] = pd.to_numeric(
            df['滞納残債'].str.replace(',', ''), errors='coerce'
        ).fillna(0).astype(int).apply(lambda x: f'{x:,}')
        
        # Combine bank account information with full-width spaces
        # 回収口座銀行名　回収口座支店名　回収口座種類　回収口座番号　回収口座名義人
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
        output_filename = f"{date_str}ミライルSMS契約者.csv"
        
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
        raise Exception(f"ミライル SMS契約者処理エラー: {str(e)}")