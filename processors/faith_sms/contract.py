import pandas as pd
import io
import re
import os
from datetime import datetime, date
from typing import Tuple, List

def format_payment_deadline(date_input: date) -> str:
    """
    日付オブジェクトを日本語形式に変換
    
    Args:
        date_input: datetimeのdateオブジェクト
        
    Returns:
        str: 'YYYY年MM月DD日' 形式の文字列
        
    Examples:
        format_payment_deadline(date(2025, 6, 30)) -> '2025年6月30日'
        format_payment_deadline(date(2025, 12, 1)) -> '2025年12月1日'
    """
    return date_input.strftime("%Y年%m月%d日")

def load_faith_sms_template_headers() -> List[str]:
    """SMSテンプレートヘッダーを読み込み（新共通ファイル優先、旧ファイルへフォールバック）"""
    current_dir = os.path.dirname(__file__)

    candidate_filenames = [
        'sms_template_headers.txt',
        'faith_sms_template_headers.txt',
    ]

    template_paths: List[str] = []
    for filename in candidate_filenames:
        template_paths.extend([
            os.path.join(current_dir, '..', '..', 'templates', filename),
            os.path.abspath(os.path.join(current_dir, '..', '..', 'templates', filename)),
            f'templates/{filename}',
            f'./templates/{filename}',
        ])

    for template_path in template_paths:
        if os.path.exists(template_path):
            try:
                encodings = ['utf-8', 'utf-8-sig', 'cp932', 'shift_jis']
                header_line = None
                for encoding in encodings:
                    try:
                        with open(template_path, 'r', encoding=encoding) as f:
                            header_line = f.read().strip()
                        break
                    except UnicodeDecodeError:
                        continue
                if header_line is None:
                    raise Exception('ヘッダーファイルの読み込みに失敗しました（エンコーディングエラー）')
                return header_line.split(',')
            except Exception:
                continue

    raise FileNotFoundError(
        f"SMSテンプレートヘッダーファイルが見つかりません。試行パス: {template_paths}"
    )

def read_csv_auto_encoding(file_content: bytes) -> pd.DataFrame:
    """アップロードされたCSVファイルを自動エンコーディング判定で読み込み"""
    encodings = ['utf-8', 'utf-8-sig', 'shift_jis', 'cp932', 'euc_jp']
    
    for enc in encodings:
        try:
            return pd.read_csv(io.BytesIO(file_content), encoding=enc, dtype=str)
        except Exception:
            continue
    
    raise ValueError("CSVファイルの読み込みに失敗しました。エンコーディングを確認してください。")

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
        logs.append(f"元データ読み込み: {initial_rows}件")
        
        # Filter 1: 委託先法人ID (Keep only 1, 2, 3, 4)
        trustee_ids_to_keep = [1, 2, 3, 4]
        df['委託先法人ID'] = pd.to_numeric(df['委託先法人ID'], errors='coerce').fillna(-1).astype(int)
        
        # 除外データの詳細を記録
        excluded_trustee = df[~df['委託先法人ID'].isin(trustee_ids_to_keep)]
        if len(excluded_trustee) > 0:
            excluded_ids = excluded_trustee['委託先法人ID'].value_counts().head(5).to_dict()
            logs.append(f"フィルター1除外 - 委託先法人ID詳細: {excluded_ids}")
        
        df = df[df['委託先法人ID'].isin(trustee_ids_to_keep)]
        logs.append(f"フィルター1 - 委託先法人ID(1-4): {len(df)}件")
        
        # Filter 2: 入金予定日 (Keep empty or dates before today)
        df['入金予定日'] = pd.to_datetime(df['入金予定日'], format='%Y/%m/%d', errors='coerce')
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        
        # 除外データの詳細を記録
        excluded_dates = df[~(df['入金予定日'].isna() | (df['入金予定日'] < today))]
        if len(excluded_dates) > 0:
            future_dates = excluded_dates['入金予定日'].dropna().dt.strftime('%Y/%m/%d').value_counts().head(3).to_dict()
            logs.append(f"フィルター2除外 - 未来日付例: {future_dates}")
        
        df = df[df['入金予定日'].isna() | (df['入金予定日'] < today)]
        logs.append(f"フィルター2 - 入金予定日(前日以前+空白): {len(df)}件")
        
        # Filter 3: 入金予定金額 (Exclude specific amounts: 2, 3, 5 as numeric or string values)
        payment_amount_exclude_numeric = [2, 3, 5]
        payment_amount_exclude_string = ["2", "3", "5"]
        df['入金予定金額_numeric'] = pd.to_numeric(df['入金予定金額'], errors='coerce')
        df['入金予定金額_string'] = df['入金予定金額'].astype(str)
        
        # 除外データの詳細を記録
        excluded_amounts = df[(df['入金予定金額_numeric'].isin(payment_amount_exclude_numeric) | 
                              df['入金予定金額_string'].isin(payment_amount_exclude_string))]
        if len(excluded_amounts) > 0:
            excluded_values = excluded_amounts['入金予定金額'].value_counts().head(3).to_dict()
            logs.append(f"フィルター3除外 - 金額詳細: {excluded_values}")
        
        df = df[~(df['入金予定金額_numeric'].isin(payment_amount_exclude_numeric) | 
                  df['入金予定金額_string'].isin(payment_amount_exclude_string))]
        df = df.drop(['入金予定金額_numeric', '入金予定金額_string'], axis=1)
        logs.append(f"フィルター3 - 入金予定金額(2,3,5円除外): {len(df)}件")
        
        # Filter 4: 回収ランク (Exclude specific ranks)
        collection_rank_exclude = ["弁護士介入", "破産決定", "死亡決定"]
        
        # 除外データの詳細を記録
        excluded_ranks = df[df['回収ランク'].isin(collection_rank_exclude)]
        if len(excluded_ranks) > 0:
            excluded_rank_counts = excluded_ranks['回収ランク'].value_counts().to_dict()
            logs.append(f"フィルター4除外 - 回収ランク詳細: {excluded_rank_counts}")
        
        df = df[~df['回収ランク'].isin(collection_rank_exclude)]
        logs.append(f"フィルター4 - 回収ランク(弁護士介入等除外): {len(df)}件")
        
        # Filter 5: TEL携帯 (Keep only valid mobile phone numbers)
        mobile_phone_regex = r'^(090|080|070)-\d{4}-\d{4}$'
        df['TEL携帯'] = df['TEL携帯'].astype(str).str.strip().replace('nan', '')
        
        def is_mobile_phone(phone_number):
            if phone_number == '':
                return False
            return bool(re.match(mobile_phone_regex, phone_number))
        
        # 除外データの詳細を記録
        excluded_phones = df[~df['TEL携帯'].apply(is_mobile_phone)]
        if len(excluded_phones) > 0:
            invalid_phone_samples = excluded_phones['TEL携帯'].value_counts().head(5).to_dict()
            logs.append(f"フィルター5除外 - TEL携帯形式例: {invalid_phone_samples}")
        
        df = df[df['TEL携帯'].apply(is_mobile_phone)]
        logs.append(f"フィルター5 - TEL携帯(090/080/070のみ): {len(df)}件")
        
        # Data mapping to output format - load from external template
        output_column_order = load_faith_sms_template_headers()
        
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
        
        # 統計情報を辞書形式で作成
        stats = {
            'initial_rows': initial_rows,
            'processed_rows': len(output_df)
        }
        
        return df_copy, logs, output_filename, stats
        
    except Exception as e:
        raise Exception(f"FAITH SMS退去済み契約者処理エラー: {str(e)}")
