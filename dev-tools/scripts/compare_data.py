#!/usr/bin/env python3
"""
正解ファイルとシステム出力の差分を詳細比較
どの22件が不一致なのかを特定
"""

import pandas as pd

def compare_data():
    """正解とシステム出力を詳細比較"""
    try:
        # 正解ファイル読み込み
        print("正解ファイルを読み込み中...")
        correct_df = pd.read_excel("C:/Users/user04/Downloads/正解.xlsx")
        print(f"正解データ件数: {len(correct_df)}件")
        
        # システム出力読み込み（エンコーディング自動判定）
        print("システム出力を読み込み中...")
        encodings = ['cp932', 'shift_jis', 'utf-8', 'utf-8-sig']
        system_df = None
        
        for encoding in encodings:
            try:
                system_df = pd.read_csv("C:/Users/user04/Downloads/0820FAITH_SMS_Vacated_Contract.csv", encoding=encoding)
                print(f"システム出力件数: {len(system_df)}件 (エンコーディング: {encoding})")
                break
            except UnicodeDecodeError:
                continue
        
        if system_df is None:
            raise Exception("システム出力ファイルの読み込みに失敗しました")
        
        # 電話番号でキーとして使用
        correct_phones = set(correct_df['電話番号'].astype(str).str.strip())
        system_phones = set(system_df['電話番号'].astype(str).str.strip())
        
        print(f"\n正解ファイルの電話番号: {len(correct_phones)}件")
        print(f"システム出力の電話番号: {len(system_phones)}件")
        
        # 正解にあってシステムにない電話番号
        missing_in_system = correct_phones - system_phones
        print(f"\n正解にあってシステムにない電話番号: {len(missing_in_system)}件")
        
        if missing_in_system:
            print("不足データ一覧:")
            for i, phone in enumerate(sorted(missing_in_system), 1):
                # 正解ファイルから該当データを取得
                matching_row = correct_df[correct_df['電話番号'].astype(str).str.strip() == phone]
                if not matching_row.empty:
                    name = matching_row.iloc[0]['(info1)契約者名']
                    amount = matching_row.iloc[0]['(info3)金額']
                    memo = matching_row.iloc[0]['(info5)メモ']
                    print(f"  {i:2d}. {phone} | {name} | {amount} | {memo}")
        
        # システムにあって正解にない電話番号
        extra_in_system = system_phones - correct_phones
        print(f"\nシステムにあって正解にない電話番号: {len(extra_in_system)}件")
        
        if extra_in_system:
            print("余分データ一覧:")
            for i, phone in enumerate(sorted(extra_in_system), 1):
                # システム出力から該当データを取得
                matching_row = system_df[system_df['電話番号'].astype(str).str.strip() == phone]
                if not matching_row.empty:
                    name = matching_row.iloc[0]['(info1)契約者名']
                    amount = matching_row.iloc[0]['(info3)金額']
                    memo = matching_row.iloc[0]['(info5)メモ']
                    print(f"  {i:2d}. {phone} | {name} | {amount} | {memo}")
        
        # 一致している電話番号
        matching_phones = correct_phones & system_phones
        print(f"\n一致している電話番号: {len(matching_phones)}件")
        
        # 差分サマリー
        print(f"\n=== 差分サマリー ===")
        print(f"正解のみ: {len(missing_in_system)}件")
        print(f"システムのみ: {len(extra_in_system)}件")
        print(f"一致: {len(matching_phones)}件")
        print(f"総差分: {len(missing_in_system) + len(extra_in_system)}件")
        
    except Exception as e:
        print(f"エラー: {str(e)}")

if __name__ == "__main__":
    compare_data()