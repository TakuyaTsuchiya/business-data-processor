#!/usr/bin/env python3
"""
居住訪問調査報告書の詳細テスト
実際のdetermine_billing_rows関数を使用して動作を確認
"""
import pandas as pd
import sys
from pathlib import Path

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from processors.residence_survey.billing_processor import (
    determine_billing_rows,
    get_survey_month,
    TAKAHASHI_LAW_FIRM
)

# サンプルデータのパス
CSV_PATH = "/Users/tchytky/Downloads/居住訪問調査報告書_20251008T152416+0900.csv"


def test_with_actual_function(df: pd.DataFrame, selected_month: str):
    """実際の関数を使ってテスト"""
    print(f"\n{'=' * 80}")
    print(f"選択月: {selected_month[:4]}年{selected_month[4:]}月での集計テスト")
    print(f"{'=' * 80}\n")

    total_records = 0
    total_billing_rows = 0
    skipped_records = 0
    details = []

    for idx, row in df.iterrows():
        law_firm = row['依頼元'] if pd.notna(row['依頼元']) else '依頼元不明'
        is_takahashi = law_firm == TAKAHASHI_LAW_FIRM

        # 実際の関数を呼び出し
        billing_times = determine_billing_rows(row, is_takahashi, selected_month)

        total_records += 1

        if billing_times:
            total_billing_rows += len(billing_times)
            details.append({
                'レコード番号': row['レコード番号'],
                '居住者名': row['居住者名'],
                '依頼元': law_firm,
                '請求回数': billing_times,
                '調査日時1': row.get('調査日時【１回目】'),
                '調査日時2': row.get('調査日時【２回目】'),
                '調査日時3': row.get('調査日時【３回目】'),
                '提出日1': row.get('1回目提出日'),
                '提出日2': row.get('2回目提出日'),
                '提出日3': row.get('3回目提出日'),
            })
        else:
            skipped_records += 1

    # サマリ表示
    print(f"総レコード数: {total_records}件")
    print(f"請求対象レコード: {len(details)}件")
    print(f"請求対象外レコード: {skipped_records}件")
    print(f"請求行数: {total_billing_rows}行")
    print()

    # 請求対象の詳細表示
    if details:
        print(f"【請求対象の詳細】")
        print("-" * 80)

        for item in details[:20]:  # 最初の20件を表示
            print(f"\nレコード#{item['レコード番号']}: {item['居住者名']}")
            print(f"  依頼元: {item['依頼元']}")
            print(f"  請求回数: {item['請求回数']}")

            # 各回の情報を表示
            for times in [1, 2, 3]:
                survey_date = item[f'調査日時{times}']
                submit_date = item[f'提出日{times}']

                if pd.notna(survey_date) or pd.notna(submit_date):
                    survey_month = get_survey_month(survey_date) if pd.notna(survey_date) else "未入力"
                    submit_month = get_survey_month(submit_date) if pd.notna(submit_date) else "未入力"

                    print(f"  {times}回目:")
                    print(f"    調査日時: {survey_date} ({survey_month})")
                    print(f"    提出日: {submit_date} ({submit_month})")

                    # 請求対象かどうか
                    if times in item['請求回数']:
                        match_reason = ""
                        if submit_month == selected_month:
                            match_reason = "✅ 提出日が選択月と一致"
                        elif survey_month == selected_month:
                            match_reason = "⚠️ 調査日時が選択月と一致（修正前の挙動）"
                        else:
                            match_reason = "❓ 不明な理由"
                        print(f"    → {match_reason}")

        if len(details) > 20:
            print(f"\n... 他{len(details) - 20}件")

    return details, skipped_records


def analyze_submission_dates(df: pd.DataFrame):
    """提出日の分布を分析"""
    print(f"\n{'=' * 80}")
    print("提出日の月別分布")
    print(f"{'=' * 80}\n")

    month_counts = {}

    for idx, row in df.iterrows():
        for times in [1, 2, 3]:
            submit_date = row.get(f'{times}回目提出日')
            if pd.notna(submit_date):
                month = get_survey_month(submit_date)
                if month:
                    if month not in month_counts:
                        month_counts[month] = 0
                    month_counts[month] += 1

    for month in sorted(month_counts.keys()):
        year = month[:4]
        mon = month[4:]
        print(f"  {year}年{mon}月: {month_counts[month]}件")

    print(f"\n  合計: {sum(month_counts.values())}件")


def main():
    print("=" * 80)
    print("居住訪問調査報告書 - 詳細テスト（実際の関数使用）")
    print("=" * 80)

    # CSVファイル読み込み
    try:
        df = pd.read_csv(CSV_PATH, encoding='cp932')
    except:
        try:
            df = pd.read_csv(CSV_PATH, encoding='shift_jis')
        except:
            df = pd.read_csv(CSV_PATH, encoding='utf-8-sig')

    print(f"\n✅ データ読み込み完了: {len(df)}件\n")

    # 提出日の分布を確認
    analyze_submission_dates(df)

    # 各月でテスト
    test_months = ['202509', '202510']

    for month in test_months:
        test_with_actual_function(df, month)

    print("\n" + "=" * 80)
    print("テスト完了")
    print("=" * 80)


if __name__ == "__main__":
    main()
