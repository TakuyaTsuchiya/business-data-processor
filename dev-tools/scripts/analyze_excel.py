#!/usr/bin/env python3
"""
正解.xlsxファイル分析スクリプト
システム出力との差分を特定するための分析
"""

import pandas as pd
import re

def analyze_excel_file():
    """正解Excelファイルを分析"""
    try:
        # 正解ファイル読み込み
        print("正解ファイルを読み込み中...")
        correct_df = pd.read_excel("C:/Users/user04/Downloads/正解.xlsx")
        print(f"正解データ件数: {len(correct_df)}件")
        
        # 列名確認
        print("\n列名一覧:")
        for i, col in enumerate(correct_df.columns):
            print(f"  {i+1}: {col}")
        
        # 電話番号列を特定（複数の可能性を考慮）
        phone_col = None
        possible_phone_cols = ['電話番号', 'TEL携帯', 'TEL', '携帯電話', '電話']
        
        for col in possible_phone_cols:
            if col in correct_df.columns:
                phone_col = col
                break
        
        if phone_col:
            print(f"\n電話番号列: '{phone_col}'")
            
            # TEL携帯形式の分析
            tel_data = correct_df[phone_col].astype(str).str.strip()
            
            # 090/080/070パターン（ハイフンあり）
            mobile_pattern = r'^(090|080|070)-\d{4}-\d{4}$'
            mobile_count = tel_data.str.match(mobile_pattern).sum()
            print(f"携帯電話(090/080/070-xxxx-xxxx): {mobile_count}件")
            
            # 固定電話パターン
            fixed_patterns = [
                r'^0[1-9]-\d{4}-\d{4}$',  # 0x-xxxx-xxxx
                r'^03-\d{4}-\d{4}$',      # 03-xxxx-xxxx
                r'^06-\d{4}-\d{4}$',      # 06-xxxx-xxxx
            ]
            
            fixed_count = 0
            for pattern in fixed_patterns:
                fixed_count += tel_data.str.match(pattern).sum()
            
            print(f"固定電話: {fixed_count}件")
            
            # 空白・無効データ
            empty_count = (tel_data == '') | (tel_data == 'nan') | tel_data.isna()
            print(f"空白・無効: {empty_count.sum()}件")
            
            # パターン別サンプル表示
            print(f"\n電話番号形式サンプル（上位10件）:")
            value_counts = tel_data.value_counts().head(10)
            for phone, count in value_counts.items():
                print(f"  {phone}: {count}件")
            
            # 携帯電話のみの件数（システム出力と比較）
            system_output = 6490
            print(f"\n比較分析:")
            print(f"  正解ファイル総件数: {len(correct_df)}件")
            print(f"  携帯電話のみ: {mobile_count}件")
            print(f"  システム出力: {system_output}件")
            print(f"  差分: {mobile_count - system_output}件")
            
        else:
            print("電話番号列が見つかりませんでした")
            
    except Exception as e:
        print(f"エラー: {str(e)}")

if __name__ == "__main__":
    analyze_excel_file()