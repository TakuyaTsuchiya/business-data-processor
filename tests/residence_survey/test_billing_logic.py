#!/usr/bin/env python3
"""
居住訪問調査報告書の請求ロジックテスト
「2回目があれば1回目+2回目を請求」仕様の確認
"""
import pandas as pd
from datetime import datetime

def get_survey_month(survey_date):
    """日付から月を判定"""
    if pd.isna(survey_date):
        return None
    try:
        date = pd.to_datetime(survey_date)
        return date.strftime('%Y%m')
    except:
        return None


def determine_billing_rows_current(row, selected_month):
    """現在のロジック（問題あり）"""
    submission_months = {
        1: get_survey_month(row.get('1回目提出日')),
        2: get_survey_month(row.get('2回目提出日')),
        3: get_survey_month(row.get('3回目提出日'))
    }

    matching_times = [times for times, month in submission_months.items() if month == selected_month]

    if not matching_times:
        return []

    # 通常パターン
    if 3 in matching_times:
        return [3]
    elif 2 in matching_times:
        return [t for t in [1, 2] if t in matching_times]  # ← 問題！
    elif 1 in matching_times:
        return [1]

    return matching_times


def determine_billing_rows_fixed(row, selected_month):
    """修正後のロジック"""
    submission_months = {
        1: get_survey_month(row.get('1回目提出日')),
        2: get_survey_month(row.get('2回目提出日')),
        3: get_survey_month(row.get('3回目提出日'))
    }

    matching_times = [times for times, month in submission_months.items() if month == selected_month]

    if not matching_times:
        return []

    # 通常パターン
    if 3 in matching_times:
        return [3]
    elif 2 in matching_times:
        return [1, 2]  # ← 修正：2回目があれば必ず1回目+2回目
    elif 1 in matching_times:
        return [1]

    return matching_times


# テストケース
test_cases = [
    {
        "name": "ケース1: 1回目=9月、2回目=10月、選択月=10月",
        "data": {
            '1回目提出日': '2025/09/25',
            '2回目提出日': '2025/10/03',
            '3回目提出日': None
        },
        "selected_month": "202510",
        "expected": [1, 2],
        "description": "2回目が選択月 → 1回目+2回目を請求すべき"
    },
    {
        "name": "ケース2: 1回目=9月、2回目=9月、選択月=9月",
        "data": {
            '1回目提出日': '2025/09/15',
            '2回目提出日': '2025/09/25',
            '3回目提出日': None
        },
        "selected_month": "202509",
        "expected": [1, 2],
        "description": "両方が選択月 → 1回目+2回目を請求"
    },
    {
        "name": "ケース3: 1回目=9月のみ、選択月=9月",
        "data": {
            '1回目提出日': '2025/09/25',
            '2回目提出日': None,
            '3回目提出日': None
        },
        "selected_month": "202509",
        "expected": [1],
        "description": "1回目のみ → 1回目を請求"
    },
    {
        "name": "ケース4: 1回目=9月、2回目=なし、3回目=10月、選択月=10月",
        "data": {
            '1回目提出日': '2025/09/25',
            '2回目提出日': None,
            '3回目提出日': '2025/10/03'
        },
        "selected_month": "202510",
        "expected": [3],
        "description": "3回目のみが選択月 → 3回目のみ請求"
    },
    {
        "name": "ケース5: 1回目=なし、2回目=10月、選択月=10月",
        "data": {
            '1回目提出日': None,
            '2回目提出日': '2025/10/03',
            '3回目提出日': None
        },
        "selected_month": "202510",
        "expected": [1, 2],
        "description": "2回目が選択月 → 1回目+2回目を請求（1回目が未提出でも）"
    }
]

print("=" * 80)
print("居住訪問調査報告書 - 請求ロジックテスト")
print("=" * 80)
print()

has_issues = False

for i, test in enumerate(test_cases, 1):
    print(f"【{test['name']}】")
    print(f"  説明: {test['description']}")
    print(f"  データ:")
    print(f"    1回目提出日: {test['data']['1回目提出日']}")
    print(f"    2回目提出日: {test['data']['2回目提出日']}")
    print(f"    3回目提出日: {test['data']['3回目提出日']}")
    print(f"  選択月: {test['selected_month'][:4]}年{test['selected_month'][4:]}月")

    # 現在のロジック
    current_result = determine_billing_rows_current(test['data'], test['selected_month'])

    # 修正後のロジック
    fixed_result = determine_billing_rows_fixed(test['data'], test['selected_month'])

    # 期待値
    expected = test['expected']

    print(f"  期待値: {expected}")
    print(f"  現在のロジック: {current_result}", end="")
    if current_result == expected:
        print(" ✅")
    else:
        print(" ❌ 問題あり！")
        has_issues = True

    print(f"  修正後のロジック: {fixed_result}", end="")
    if fixed_result == expected:
        print(" ✅")
    else:
        print(" ❌ 修正後も問題あり！")
        has_issues = True

    print()

print("=" * 80)
if has_issues:
    print("⚠️  問題が見つかりました。修正が必要です。")
else:
    print("✅ すべてのテストケースが正しく動作しています。")
print("=" * 80)
