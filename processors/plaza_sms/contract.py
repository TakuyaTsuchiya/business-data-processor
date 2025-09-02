import pandas as pd
import re
from datetime import datetime, date
from typing import Tuple, List, Dict

# SMS共通モジュールから関数とヘッダーをインポート
from processors.sms_common import (
    SMS_TEMPLATE_HEADERS,
    format_payment_deadline,
    read_csv_auto_encoding
)


def process_plaza_sms_contract_data(
    contract_file_content: bytes, 
    callcenter_file_content: bytes, 
    payment_deadline_date: date
) -> Tuple[pd.DataFrame, pd.DataFrame, List[str], str, str, dict]:
    """
    プラザSMS契約者データ処理（Streamlit対応版）
    
    Args:
        contract_file_content: ContractList.csvの内容（bytes）
        callcenter_file_content: コールセンター回収委託CSVの内容（bytes）
        payment_deadline_date: 支払期限日付（dateオブジェクト）
        
    Returns:
        tuple: (日本人向けDF, 外国人向けDF, ログリスト, 日本人ファイル名, 外国人ファイル名, 統計情報)
    """
    try:
        # ログリスト初期化
        logs = []
        
        # CSVファイル読み込み（自動エンコーディング判定）
        logs.append("ContractList.csvを読み込み中...")
        contract_df = read_csv_auto_encoding(contract_file_content)
        initial_rows = len(contract_df)
        logs.append(f"ContractList読み込み完了: {initial_rows}件")
        
        logs.append("コールセンター回収委託CSVを読み込み中...")
        callcenter_df = read_csv_auto_encoding(callcenter_file_content)
        logs.append(f"コールセンター回収委託CSV読み込み完了: {len(callcenter_df)}件")
        
        # コールセンターCSVの列名を確認し、国籍列を特定
        # D列（会員番号）とP列（国籍）を使用
        if len(callcenter_df.columns) >= 16:  # P列まで存在することを確認
            member_no_col = callcenter_df.columns[3]  # D列（0-indexed で 3）
            nationality_col = callcenter_df.columns[15]  # P列（0-indexed で 15）
            logs.append(f"VLOOKUP用列: 会員番号列='{member_no_col}', 国籍列='{nationality_col}'")
        else:
            raise ValueError("コールセンター回収委託CSVの列数が不足しています")
        
        # Filter 1: 委託先法人ID (Keep only 6)
        contract_df['委託先法人ID'] = pd.to_numeric(contract_df['委託先法人ID'], errors='coerce').fillna(-1).astype(int)
        
        # 除外データの詳細を記録
        excluded_trustee = contract_df[contract_df['委託先法人ID'] != 6]
        if len(excluded_trustee) > 0:
            excluded_ids = excluded_trustee['委託先法人ID'].value_counts().head(5).to_dict()
            logs.append(f"フィルター1除外 - 委託先法人ID詳細: {excluded_ids}")
        
        contract_df = contract_df[contract_df['委託先法人ID'] == 6]
        logs.append(f"フィルター1 - 委託先法人ID(6のみ): {len(contract_df)}件")
        
        # Filter 2: 入金予定日 (Keep empty or dates before today)
        contract_df['入金予定日'] = pd.to_datetime(contract_df['入金予定日'], format='%Y/%m/%d', errors='coerce')
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        
        # 除外データの詳細を記録
        excluded_dates = contract_df[~(contract_df['入金予定日'].isna() | (contract_df['入金予定日'] < today))]
        if len(excluded_dates) > 0:
            future_dates = excluded_dates['入金予定日'].dropna().dt.strftime('%Y/%m/%d').value_counts().head(3).to_dict()
            logs.append(f"フィルター2除外 - 未来日付例: {future_dates}")
        
        contract_df = contract_df[contract_df['入金予定日'].isna() | (contract_df['入金予定日'] < today)]
        logs.append(f"フィルター2 - 入金予定日(前日以前+空白): {len(contract_df)}件")
        
        # Filter 3: 入金予定金額 (Exclude specific amounts: 2, 3, 5, 12 as numeric or string values)
        payment_amount_exclude_numeric = [2, 3, 5, 12]
        payment_amount_exclude_string = ["2", "3", "5", "12"]
        contract_df['入金予定金額_numeric'] = pd.to_numeric(contract_df['入金予定金額'], errors='coerce')
        contract_df['入金予定金額_string'] = contract_df['入金予定金額'].astype(str)
        
        # 除外データの詳細を記録
        excluded_amounts = contract_df[(contract_df['入金予定金額_numeric'].isin(payment_amount_exclude_numeric) | 
                                       contract_df['入金予定金額_string'].isin(payment_amount_exclude_string))]
        if len(excluded_amounts) > 0:
            excluded_values = excluded_amounts['入金予定金額'].value_counts().head(5).to_dict()
            logs.append(f"フィルター3除外 - 金額詳細: {excluded_values}")
        
        contract_df = contract_df[~(contract_df['入金予定金額_numeric'].isin(payment_amount_exclude_numeric) | 
                                   contract_df['入金予定金額_string'].isin(payment_amount_exclude_string))]
        contract_df = contract_df.drop(['入金予定金額_numeric', '入金予定金額_string'], axis=1)
        logs.append(f"フィルター3 - 入金予定金額(2,3,5,12円除外): {len(contract_df)}件")
        
        # Filter 4: 回収ランク (Exclude specific ranks)
        collection_rank_exclude = ["弁護士介入", "死亡決定", "破産決定"]
        
        # 除外データの詳細を記録
        excluded_ranks = contract_df[contract_df['回収ランク'].isin(collection_rank_exclude)]
        if len(excluded_ranks) > 0:
            excluded_rank_counts = excluded_ranks['回収ランク'].value_counts().to_dict()
            logs.append(f"フィルター4除外 - 回収ランク詳細: {excluded_rank_counts}")
        
        contract_df = contract_df[~contract_df['回収ランク'].isin(collection_rank_exclude)]
        logs.append(f"フィルター4 - 回収ランク(弁護士介入等除外): {len(contract_df)}件")
        
        # Filter 5: 電話番号 (Keep only valid mobile phone numbers)
        mobile_phone_regex = r'^(090|080|070)-\d{4}-\d{4}$'
        
        # AB列（TEL携帯）を取得
        phone_series = contract_df['TEL携帯'].astype(str).str.strip().replace('nan', '')
        
        def is_mobile_phone(phone_number):
            if phone_number == '':
                return False
            return bool(re.match(mobile_phone_regex, phone_number))
        
        # 除外データの詳細を記録
        excluded_phones_mask = ~phone_series.apply(is_mobile_phone)
        if excluded_phones_mask.sum() > 0:
            invalid_phone_samples = phone_series[excluded_phones_mask].value_counts().head(5).to_dict()
            logs.append(f"フィルター5除外 - 携帯電話形式外例: {invalid_phone_samples}")
        
        contract_df = contract_df[phone_series.apply(is_mobile_phone)]
        logs.append(f"フィルター5 - 契約者携帯電話(090/080/070のみ): {len(contract_df)}件")
        
        # VLOOKUP処理：国籍情報の結合
        logs.append("国籍情報のVLOOKUP処理を開始...")
        
        # 引継番号を文字列として処理
        contract_df['引継番号'] = contract_df['引継番号'].astype(str).str.strip()
        callcenter_df[member_no_col] = callcenter_df[member_no_col].astype(str).str.strip()
        
        # マージ（VLOOKUP相当）
        merged_df = contract_df.merge(
            callcenter_df[[member_no_col, nationality_col]], 
            left_on='引継番号', 
            right_on=member_no_col,
            how='left'
        )
        
        # 国籍情報が見つからない場合の処理
        no_nationality = merged_df[nationality_col].isna()
        if no_nationality.sum() > 0:
            logs.append(f"警告: {no_nationality.sum()}件の国籍情報が見つかりませんでした（日本人として処理）")
            merged_df.loc[no_nationality, nationality_col] = '日本'
        
        # 国籍による分離
        japanese_mask = merged_df[nationality_col].astype(str).str.strip() == '日本'
        japanese_df = merged_df[japanese_mask].copy()
        foreign_df = merged_df[~japanese_mask].copy()
        
        logs.append(f"国籍による分離完了 - 日本人: {len(japanese_df)}件, 外国人: {len(foreign_df)}件")
        
        # SMS出力フォーマット用のヘッダーを読み込み
        output_column_order = SMS_TEMPLATE_HEADERS
        
        # 日本人向けと外国人向けのDataFrameを作成
        output_dfs = []
        for df_name, df in [('日本', japanese_df), ('外国', foreign_df)]:
            if len(df) == 0:
                logs.append(f"{df_name}人向けデータが0件のため、空のDataFrameを作成")
                # 空のDataFrameを作成
                empty_df = pd.DataFrame(columns=output_column_order)
                output_dfs.append(empty_df)
                continue
            
            # Create temporary column names for DataFrame construction
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
            
            # 保証人・連絡人列は空のまま
            output_df['保証人'] = ''
            output_df['連絡人'] = ''
            
            # Set payment deadline (BG column - column 59)
            payment_deadline_formatted = format_payment_deadline(payment_deadline_date)
            output_df['支払期限'] = payment_deadline_formatted
            
            # Fill empty columns
            for col in temp_column_order:
                if col.startswith('_empty_') and col not in output_df.columns:
                    output_df[col] = ''
            
            # Ensure correct column order
            output_df = output_df[temp_column_order]
            
            # Restore original column names
            df_copy = output_df.copy()
            df_copy.columns = output_column_order
            
            output_dfs.append(df_copy)
            logs.append(f"{df_name}人向けデータ作成完了: {len(df_copy)}件")
        
        # Create output filenames
        date_str = datetime.now().strftime("%m%d")
        japanese_filename = f"{date_str}プラザ契約者SMS日本.csv"
        foreign_filename = f"{date_str}プラザ契約者SMS外国.csv"
        
        # 統計情報を辞書形式で作成
        stats = {
            'initial_rows': initial_rows,
            'processed_rows': len(merged_df),
            'japanese_rows': len(japanese_df),
            'foreign_rows': len(foreign_df)
        }
        
        return output_dfs[0], output_dfs[1], logs, japanese_filename, foreign_filename, stats
        
    except Exception as e:
        raise Exception(f"プラザSMS契約者処理エラー: {str(e)}")
