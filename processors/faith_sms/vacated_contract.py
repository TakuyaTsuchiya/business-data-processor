import os
import pandas as pd
import chardet
import re
from datetime import datetime

def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        raw_data = f.read(10000)
    result = chardet.detect(raw_data)
    return result['encoding']

def process_faith_sms_vacated_contract_data(file_path):
    try:
        # Encoding detection and CSV loading
        encodings_to_try = []
        detected_encoding = detect_encoding(file_path)
        if detected_encoding:
            encodings_to_try.append(detected_encoding)
        
        common_encodings = ['shift_jis', 'cp932', 'euc_jp', 'utf-8', 'utf-8-sig']
        for enc in common_encodings:
            if enc not in encodings_to_try:
                encodings_to_try.append(enc)

        df = None
        for enc in encodings_to_try:
            try:
                df = pd.read_csv(file_path, encoding=enc)
                break
            except Exception:
                continue

        if df is None:
            raise IOError(f"Could not read CSV file {file_path} with any encoding")

        initial_rows = len(df)
        
        # Filter 1: 委託先法人ID (Keep only 1, 2, 3, 4)
        trustee_ids_to_keep = [1, 2, 3, 4]
        df['委託先法人ID'] = pd.to_numeric(df['委託先法人ID'], errors='coerce').fillna(-1).astype(int)
        df = df[df['委託先法人ID'].isin(trustee_ids_to_keep)]
        
        # Filter 2: 入金予定日 (Keep empty or dates before today)
        df['入金予定日'] = pd.to_datetime(df['入金予定日'], format='%Y/%m/%d', errors='coerce')
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        df = df[df['入金予定日'].isna() | (df['入金予定日'] < today)]
        
        # Filter 3: 入金予定金額 (Exclude specific amounts)
        payment_amount_exclude = ["1円", "2円", "3円", "5円"]
        df['入金予定金額'] = df['入金予定金額'].astype(str)
        df = df[~df['入金予定金額'].isin(payment_amount_exclude)]
        
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
        
        # Data mapping to output format (59 columns)
        # Create column names for 59-column structure
        main_columns = [
            '電話番号',
            '(info1)契約者名', 
            '(info2)物件名',
            '(info3)金額',
            '(info4)銀行口座',
            '(info5)メモ'
        ]
        
        empty_columns = [f'_empty_{i}' for i in range(50)]
        
        final_columns = [
            '保証人',
            '連絡人', 
            '支払期限'
        ]
        
        output_column_order = main_columns + empty_columns + final_columns
        
        # Create output DataFrame
        output_df = pd.DataFrame(columns=output_column_order)
        
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
        
        # Fill empty columns
        for i in range(50):
            empty_col_name = f'_empty_{i}'
            output_df[empty_col_name] = ''
        
        # Fill final columns
        output_df['保証人'] = ''
        output_df['連絡人'] = ''
        output_df['支払期限'] = ''
        
        # Ensure correct column order
        output_df = output_df[output_column_order]
        
        # Create output filename
        date_str = datetime.now().strftime("%m%d")
        output_filename = f"{date_str}FAITH_SMS_Vacated_Contract.csv"
        
        # Clean column names for output (convert _empty_X to blank)
        df_copy = output_df.copy()
        new_columns = []
        for col in df_copy.columns:
            if col.startswith('_empty_'):
                new_columns.append('')
            else:
                new_columns.append(col)
        df_copy.columns = new_columns
        
        return df_copy, output_filename, initial_rows, len(output_df)
        
    except Exception as e:
        raise Exception(f"Error processing FAITH SMS vacated contract data: {str(e)}")