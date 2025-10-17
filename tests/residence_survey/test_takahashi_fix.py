#!/usr/bin/env python3
"""
高橋裕次郎法律事務所の特例ロジック修正テスト
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

print("=" * 80)
print("高橋裕次郎法律事務所の特例ロジック修正テスト")
print("=" * 80)
print()

# テストケース
test_cases = [
    {
        "name": "ケース1: 1回目=9月、2回目=10月、選択月=10月（高橋裕次郎）",
        "data": {
            '1回目提出日': '2025/09/25',
            '2回目提出日': '2025/10/03',
            '3回目提出日': None
        },
        "selected_month": "202510",
        "is_takahashi": True,
        "expected": [1, 2],
        "description": "2回目が選択月 → 1回目+2回目を請求すべき（修正前は[2]のみ）"
    },
    {
        "name": "ケース2: 1回目=9月、2回目=9月、選択月=9月（高橋裕次郎）",
        "data": {
            '1回目提出日': '2025/09/15',
            '2回目提出日': '2025/09/25',
            '3回目提出日': None
        },
        "selected_month": "202509",
        "is_takahashi": True,
        "expected": [1, 2],
        "description": "両方が選択月 → 1回目+2回目を請求"
    },
    {
        "name": "ケース3: 1回目=9月、2回目=なし、3回目=10月、選択月=10月（高橋裕次郎）",
        "data": {
            '1回目提出日': '2025/09/25',
            '2回目提出日': None,
            '3回目提出日': '2025/10/03'
        },
        "selected_month": "202510",
        "is_takahashi": True,
        "expected": [1, 2, 3],
        "description": "3回目が選択月 → 1〜3回目全てを請求（修正前は[3]のみ）"
    },
    {
        "name": "ケース4: 1回目=9月、2回目=9月、3回目=10月、選択月=10月（高橋裕次郎）",
        "data": {
            '1回目提出日': '2025/09/15',
            '2回目提出日': '2025/09/25',
            '3回目提出日': '2025/10/03'
        },
        "selected_month": "202510",
        "is_takahashi": True,
        "expected": [1, 2, 3],
        "description": "3回目が選択月 → 1〜3回目全てを請求"
    },
    {
        "name": "ケース5: 1回目=9月のみ、選択月=9月（高橋裕次郎）",
        "data": {
            '1回目提出日': '2025/09/25',
            '2回目提出日': None,
            '3回目提出日': None
        },
        "selected_month": "202509",
        "is_takahashi": True,
        "expected": [1],
        "description": "1回目のみ → 1回目を請求"
    },
    {
        "name": "ケース6: 1回目=9月、2回目=10月、選択月=10月（通常）",
        "data": {
            '1回目提出日': '2025/09/25',
            '2回目提出日': '2025/10/03',
            '3回目提出日': None
        },
        "selected_month": "202510",
        "is_takahashi": False,
        "expected": [1, 2],
        "description": "通常法人：2回目が選択月 → 1回目+2回目を請求"
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
