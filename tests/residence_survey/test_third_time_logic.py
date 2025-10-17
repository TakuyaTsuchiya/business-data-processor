#!/usr/bin/env python3
"""
3回目提出時のロジックテスト
通常法人と高橋裕次郎の違いを検証
"""
import pandas as pd
import sys
from pathlib import Path

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from processors.residence_survey.billing_processor import determine_billing_rows

print("=" * 80)
print("3回目提出時のロジックテスト")
print("=" * 80)
print()

# テストケース
test_cases = [
    {
        "name": "ケース1: 3回目のみが選択月（通常法人）",
        "data": {
            '1回目提出日': '2025/09/15',
            '2回目提出日': '2025/10/10',
            '3回目提出日': '2025/11/05'
        },
        "selected_month": "202511",
        "is_takahashi": False,
        "expected": [3],
        "description": "3回目のみ選択月 → [3]のみ請求（1,2は既請求済み）"
    },
    {
        "name": "ケース2: 2回目と3回目が選択月（通常法人）",
        "data": {
            '1回目提出日': '2025/09/15',
            '2回目提出日': '2025/09/20',
            '3回目提出日': '2025/09/25'
        },
        "selected_month": "202509",
        "is_takahashi": False,
        "expected": [1, 2, 3],
        "description": "2回目と3回目が同月 → [1,2,3]全て請求"
    },
    # ケース3は削除: データエラー（3回目提出日 < 2回目提出日は論理的にあり得ない）
    {
        "name": "ケース3: 3回目のみが選択月（高橋裕次郎）",
        "data": {
            '1回目提出日': '2025/09/15',
            '2回目提出日': '2025/10/10',
            '3回目提出日': '2025/11/05'
        },
        "selected_month": "202511",
        "is_takahashi": True,
        "expected": [1, 2, 3],
        "description": "高橋裕次郎：3回目があれば必ず[1,2,3]全て請求"
    },
    {
        "name": "ケース4: 2回目と3回目が選択月（高橋裕次郎）",
        "data": {
            '1回目提出日': '2025/09/15',
            '2回目提出日': '2025/10/10',
            '3回目提出日': '2025/10/15'
        },
        "selected_month": "202510",
        "is_takahashi": True,
        "expected": [1, 2, 3],
        "description": "高橋裕次郎：3回目があれば必ず[1,2,3]全て請求"
    },
    {
        "name": "ケース5: 2回目のみが選択月（通常法人）",
        "data": {
            '1回目提出日': '2025/09/15',
            '2回目提出日': '2025/10/10',
            '3回目提出日': None
        },
        "selected_month": "202510",
        "is_takahashi": False,
        "expected": [1, 2],
        "description": "2回目のみ → [1,2]請求"
    },
    {
        "name": "ケース6: 1回目のみが選択月（通常法人）",
        "data": {
            '1回目提出日': '2025/09/15',
            '2回目提出日': None,
            '3回目提出日': None
        },
        "selected_month": "202509",
        "is_takahashi": False,
        "expected": [1],
        "description": "1回目のみ → [1]請求"
    }
]

has_issues = False

for i, test in enumerate(test_cases, 1):
    print(f"【{test['name']}】")
    print(f"  説明: {test['description']}")
    print(f"  データ:")
    print(f"    1回目提出日: {test['data']['1回目提出日']}")
    print(f"    2回目提出日: {test['data']['2回目提出日']}")
    print(f"    3回目提出日: {test['data']['3回目提出日']}")
    print(f"  選択月: {test['selected_month'][:4]}年{test['selected_month'][4:]}月")
    print(f"  高橋裕次郎: {test['is_takahashi']}")

    # 実際の関数を呼び出し
    result = determine_billing_rows(test['data'], test['is_takahashi'], test['selected_month'])

    # 期待値
    expected = test['expected']

    print(f"  期待値: {expected}")
    print(f"  実行結果: {result}", end="")
    if result == expected:
        print(" ✅")
    else:
        print(" ❌ 問題あり！")
        has_issues = True

    print()

print("=" * 80)
if has_issues:
    print("⚠️  問題が見つかりました。修正が必要です。")
else:
    print("✅ すべてのテストケースが正しく動作しています。")
print("=" * 80)
