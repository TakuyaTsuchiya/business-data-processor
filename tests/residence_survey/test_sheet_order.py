#!/usr/bin/env python3
"""
シート作成順序の固定化テスト
"""
import pandas as pd
import sys
from pathlib import Path

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from processors.residence_survey.billing_processor import process_residence_survey_billing, LAW_FIRM_PRIORITY

# サンプルデータのパス
CSV_PATH = "/Users/tchytky/Downloads/居住訪問調査報告書_20251008T152416+0900.csv"

def main():
    print("=" * 80)
    print("シート作成順序の固定化テスト")
    print("=" * 80)
    print()

    # 期待されるシート順序
    print("【期待されるシート順序】")
    for i, firm in enumerate(LAW_FIRM_PRIORITY, 1):
        print(f"  {i}. {firm}")
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

    # Excelファイルを保存
    output_path = f"/Users/tchytky/Desktop/{filename}"
    with open(output_path, 'wb') as f:
        f.write(excel_buffer.read())

    print(f"✅ 出力ファイル: {output_path}")
    print()

    # シート順序を確認
    excel_file = pd.ExcelFile(output_path)
    sheet_names = excel_file.sheet_names

    print("【実際のシート順序】")
    for i, sheet in enumerate(sheet_names, 1):
        # 各シートのデータ行数を確認（ヘッダー2行を除く）
        df_sheet = pd.read_excel(output_path, sheet_name=sheet, header=1)
        data_rows = len(df_sheet)
        print(f"  {i}. {sheet} ({data_rows}行)")
    print()

    # 順序チェック
    if sheet_names[:len(LAW_FIRM_PRIORITY)] == LAW_FIRM_PRIORITY:
        print("✅ シート順序が正しく固定化されました")
    else:
        print("❌ シート順序が期待と異なります")
        print()
        print("【差分】")
        for i, (expected, actual) in enumerate(zip(LAW_FIRM_PRIORITY, sheet_names), 1):
            if expected != actual:
                print(f"  位置{i}: 期待={expected}, 実際={actual}")

    print()
    print("=" * 80)
    print("テスト完了")
    print("=" * 80)


if __name__ == "__main__":
    main()
