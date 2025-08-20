import pandas as pd
import io
import re
import os
from datetime import datetime
from typing import Tuple, List

def load_faith_sms_template_headers() -> List[str]:
    """外部ファイルからフェイスSMSテンプレートヘッダーを読み込み"""
    # 現在のスクリプトのディレクトリから相対パスでテンプレートファイルを探す
    current_dir = os.path.dirname(__file__)
    project_root = os.path.join(current_dir, '..', '..')
    template_path = os.path.join(project_root, 'templates', 'faith_sms_template_headers.txt')
    
    try:
        # 複数のエンコーディングで試行
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
            raise Exception("ヘッダーファイルの読み込みに失敗しました（エンコーディングエラー）")
            
        return header_line.split(',')
    except FileNotFoundError:
        raise FileNotFoundError(f"SMSテンプレートヘッダーファイルが見つかりません: {template_path}")
    except Exception as e:
        raise Exception(f"SMSテンプレートヘッダー読み込みエラー: {str(e)}")

def read_csv_auto_encoding(file_content: bytes) -> pd.DataFrame:
    """アップロードされたCSVファイルを自動エンコーディング判定で読み込み"""
    encodings = ['utf-8', 'utf-8-sig', 'shift_jis', 'cp932', 'euc_jp']
    
    for enc in encodings:
        try:
            return pd.read_csv(io.BytesIO(file_content), encoding=enc, dtype=str)
        except Exception:
            continue
    
    raise ValueError("CSVファイルの読み込みに失敗しました。エンコーディングを確認してください。")

def process_faith_sms_vacated_contract_data(file_content: bytes) -> Tuple[pd.DataFrame, str, int, int]:
    """
    フェイスSMS退去済み契約者データ処理（Streamlit対応版）
    
    Args:
        file_content: アップロードされたCSVファイルの内容（bytes）
        
    Returns:
        tuple: (変換済みDF, 出力ファイル名, 元データ件数, 処理後件数)
    """
    try:
        # CSVファイル読み込み（自動エンコーディング判定）
        df = read_csv_auto_encoding(file_content)

        initial_rows = len(df)
        
        # Filter 1: 委託先法人ID (Keep only 1, 2, 3, 4)
        trustee_ids_to_keep = [1, 2, 3, 4]
        df['委託先法人ID'] = pd.to_numeric(df['委託先法人ID'], errors='coerce').fillna(-1).astype(int)
        df = df[df['委託先法人ID'].isin(trustee_ids_to_keep)]
        
        # Filter 2: 入金予定日 (Keep empty or dates before today)
        df['入金予定日'] = pd.to_datetime(df['入金予定日'], format='%Y/%m/%d', errors='coerce')
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        df = df[df['入金予定日'].isna() | (df['入金予定日'] < today)]
        
        # Filter 3: 入金予定金額 (Exclude specific amounts: 2, 3, 5 as numeric or string values)
        payment_amount_exclude_numeric = [2, 3, 5]
        payment_amount_exclude_string = ["2", "3", "5"]
        df['入金予定金額_numeric'] = pd.to_numeric(df['入金予定金額'], errors='coerce')
        df['入金予定金額_string'] = df['入金予定金額'].astype(str)
        df = df[~(df['入金予定金額_numeric'].isin(payment_amount_exclude_numeric) | 
                  df['入金予定金額_string'].isin(payment_amount_exclude_string))]
        df = df.drop(['入金予定金額_numeric', '入金予定金額_string'], axis=1)
        
        # Filter 4: 回収ランク (Exclude specific ranks)
        collection_rank_exclude = ["弁護士介入", "破産決定", "死亡決定"]
        df = df[~df['回収ランク'].isin(collection_rank_exclude)]
        
        # Filter 5: TEL携帯 (Keep only valid mobile phone numbers)
        mobile_phone_regex = r'^(090|080|070)\d{8}$'
        df['TEL携帯'] = df['TEL携帯'].astype(str).str.strip().replace('nan', '')
        
        def is_mobile_phone(phone_number):
            if phone_number == '':
                return False
            return bool(re.match(mobile_phone_regex, phone_number))
        
        df = df[df['TEL携帯'].apply(is_mobile_phone)]
        
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
        
        # Fill empty columns and other remaining columns
        for col in temp_column_order:
            if col.startswith('_empty_') and col not in output_df.columns:
                output_df[col] = ''
            elif col in ['保証人', '連絡人', '支払期限'] and col not in output_df.columns:
                output_df[col] = ''
        
        # Ensure correct column order
        output_df = output_df[temp_column_order]
        
        # Create output filename
        date_str = datetime.now().strftime("%m%d")
        output_filename = f"{date_str}FAITH_SMS_Vacated_Contract.csv"
        
        # Restore original column names from template (convert _empty_X back to blank)
        df_copy = output_df.copy()
        df_copy.columns = output_column_order
        
        return df_copy, output_filename, initial_rows, len(output_df)
        
    except Exception as e:
        raise Exception(f"FAITH SMS退去済み契約者処理エラー: {str(e)}")