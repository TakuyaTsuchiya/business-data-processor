#!/usr/bin/env python3
"""
提出日列追加のテスト
"""
import pandas as pd
import sys
from pathlib import Path

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from processors.residence_survey.billing_processor import process_residence_survey_billing

# サンプルデータのパス
CSV_PATH = "/Users/tchytky/Downloads/居住訪問調査報告書_20251008T152416+0900.csv"

def main():
    print("=" * 80)
    print("提出日列追加テスト")
    print("=" * 80)
    print()

    # CSVファイル読み込み
    try:
        df = pd.read_csv(CSV_PATH, encoding='cp932')
    except:
        try:
            df = pd.read_csv(CSV_PATH, encoding='shift_jis')
        except:
            df = pd.read_csv(CSV_PATH, encoding='utf-8-sig')

    print(f"✅ データ読み込み完了: {len(df)}件")
    print()

    # 処理実行（2025年10月を選択）
    selected_month = "202510"
    print(f"選択月: {selected_month[:4]}年{selected_month[4:]}月")
    print()

    excel_buffer, filename, message, logs = process_residence_survey_billing(
        df,
        selected_month=selected_month
    )

    print(message)
    print()
    print("処理ログ:")
    for log in logs:
        print(f"  {log}")
    print()

    # Excelファイルを保存して確認
    output_path = f"/Users/tchytky/Desktop/{filename}"
    with open(output_path, 'wb') as f:
        f.write(excel_buffer.read())

    print(f"✅ 出力ファイル: {output_path}")
    print()

    # Excelファイルを読み込んで列を確認
    df_output = pd.read_excel(output_path, sheet_name=0, header=1)
    print("出力列:")
    for i, col in enumerate(df_output.columns, 1):
        print(f"  {i}. {col}")
    print()

    # 提出日列の内容を確認（最初の5行）
    if '提出日' in df_output.columns:
        print("提出日列のサンプル（最初の5行）:")
        for idx, val in df_output['提出日'].head(5).items():
            print(f"  {idx}: {val}")
        print()
        print("✅ 提出日列が正しく追加されました")
    else:
        print("❌ 提出日列が見つかりません")

    print()
    print("=" * 80)
    print("テスト完了")
    print("=" * 80)


if __name__ == "__main__":
    main()
