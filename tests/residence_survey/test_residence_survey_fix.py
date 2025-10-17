#!/usr/bin/env python3
"""
居住訪問調査報告書の修正前後の動作比較テスト
"""
import pandas as pd
import io
from datetime import datetime
from collections import defaultdict

# サンプルデータのパス
CSV_PATH = "/Users/tchytky/Downloads/居住訪問調査報告書_20251008T152416+0900.csv"


def get_survey_month(survey_date) -> str:
    """日付から月を判定 (YYYYMM形式)"""
    if pd.isna(survey_date):
        return None
    try:
        date = pd.to_datetime(survey_date)
        return date.strftime('%Y%m')
    except:
        return None


def count_by_survey_date(df: pd.DataFrame) -> dict:
    """修正前: 調査日時ベースでカウント"""
    monthly_counts = defaultdict(int)
    details = []

    for idx, row in df.iterrows():
        # 各回の調査月を取得
        survey_months = {
            1: get_survey_month(row.get('調査日時【１回目】')),
            2: get_survey_month(row.get('調査日時【２回目】')),
            3: get_survey_month(row.get('調査日時【３回目】'))
        }

        for times, month in survey_months.items():
            if month:
                monthly_counts[month] += 1
                details.append({
                    'レコード番号': row['レコード番号'],
                    '居住者名': row['居住者名'],
                    '回数': f'{times}回目',
                    '調査日時': row.get(f'調査日時【{times}回目】'),
                    '提出日': row.get(f'{times}回目提出日'),
                    '集計月': month
                })

    return monthly_counts, details


def count_by_submission_date(df: pd.DataFrame) -> dict:
    """修正後: 提出日ベースでカウント"""
    monthly_counts = defaultdict(int)
    details = []

    for idx, row in df.iterrows():
        # 各回の提出月を取得
        submission_months = {
            1: get_survey_month(row.get('1回目提出日')),
            2: get_survey_month(row.get('2回目提出日')),
            3: get_survey_month(row.get('3回目提出日'))
        }

        for times, month in submission_months.items():
            if month:
                monthly_counts[month] += 1
                details.append({
                    'レコード番号': row['レコード番号'],
                    '居住者名': row['居住者名'],
                    '回数': f'{times}回目',
                    '調査日時': row.get(f'調査日時【{times}回目】'),
                    '提出日': row.get(f'{times}回目提出日'),
                    '集計月': month
                })

    return monthly_counts, details


def main():
    print("=" * 80)
    print("居住訪問調査報告書 - 修正前後の動作比較テスト")
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

    # 修正前: 調査日時ベース
    print("【修正前】調査日時ベースでの集計")
    print("-" * 80)
    survey_counts, survey_details = count_by_survey_date(df)

    for month in sorted(survey_counts.keys()):
        year = month[:4]
        mon = month[4:]
        print(f"  {year}年{mon}月: {survey_counts[month]}件")

    print(f"  合計: {sum(survey_counts.values())}件")
    print()

    # 修正後: 提出日ベース
    print("【修正後】提出日ベースでの集計")
    print("-" * 80)
    submission_counts, submission_details = count_by_submission_date(df)

    for month in sorted(submission_counts.keys()):
        year = month[:4]
        mon = month[4:]
        print(f"  {year}年{mon}月: {submission_counts[month]}件")

    print(f"  合計: {sum(submission_counts.values())}件")
    print()

    # 差分分析
    print("【差分分析】調査日時 vs 提出日")
    print("-" * 80)

    all_months = sorted(set(list(survey_counts.keys()) + list(submission_counts.keys())))

    has_difference = False
    for month in all_months:
        survey_count = survey_counts.get(month, 0)
        submission_count = submission_counts.get(month, 0)
        diff = submission_count - survey_count

        if diff != 0:
            has_difference = True
            year = month[:4]
            mon = month[4:]
            diff_str = f"+{diff}" if diff > 0 else str(diff)
            print(f"  {year}年{mon}月: {survey_count}件 → {submission_count}件 ({diff_str})")

    if not has_difference:
        print("  差分なし（調査日時と提出日が同じ月）")

    print()

    # 詳細データの比較（差分がある場合のみ）
    if has_difference:
        print("【差分の詳細】調査日時と提出日が異なる月のレコード")
        print("-" * 80)

        # 詳細データをDataFrameに変換
        df_survey = pd.DataFrame(survey_details)
        df_submission = pd.DataFrame(submission_details)

        # レコード番号+回数をキーにして結合
        df_survey['key'] = df_survey['レコード番号'].astype(str) + '_' + df_survey['回数']
        df_submission['key'] = df_submission['レコード番号'].astype(str) + '_' + df_submission['回数']

        # 集計月が異なるレコードを抽出
        merged = pd.merge(
            df_survey[['key', 'レコード番号', '居住者名', '回数', '調査日時', '提出日', '集計月']],
            df_submission[['key', '集計月']],
            on='key',
            suffixes=('_調査', '_提出')
        )

        diff_records = merged[merged['集計月_調査'] != merged['集計月_提出']]

        if len(diff_records) > 0:
            print(f"  該当レコード: {len(diff_records)}件")
            print()
            for idx, row in diff_records.iterrows():
                print(f"  レコード#{row['レコード番号']} ({row['回数']})")
                print(f"    居住者名: {row['居住者名']}")
                print(f"    調査日時: {row['調査日時']}")
                print(f"    提出日: {row['提出日']}")
                print(f"    修正前: {row['集計月_調査'][:4]}年{row['集計月_調査'][4:]}月")
                print(f"    修正後: {row['集計月_提出'][:4]}年{row['集計月_提出'][4:]}月")
                print()
        else:
            print("  該当レコードなし")

    print("=" * 80)
    print("テスト完了")
    print("=" * 80)


if __name__ == "__main__":
    main()
