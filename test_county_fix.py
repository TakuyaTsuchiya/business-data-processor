#!/usr/bin/env python3
"""
郡対応の動作確認テスト
"""

from processors.common.address_splitter import AddressSplitter

def test_county_addresses():
    """郡を含む住所のテスト"""
    splitter = AddressSplitter()

    test_cases = [
        {
            "input": "愛知県海部郡蟹江町本町9-114",
            "expected": {
                "prefecture": "愛知県",
                "city": "海部郡",
                "remaining": "蟹江町本町9-114"
            }
        },
        {
            "input": "神奈川県足柄上郡大井町金子2963-2",
            "expected": {
                "prefecture": "神奈川県",
                "city": "足柄上郡",
                "remaining": "大井町金子2963-2"
            }
        },
        {
            "input": "千葉県夷隅郡御宿町岩和田字東大谷1195-5",
            "expected": {
                "prefecture": "千葉県",
                "city": "夷隅郡",
                "remaining": "御宿町岩和田字東大谷1195-5"
            }
        },
        {
            "input": "奈良県大和郡山市",
            "expected": {
                "prefecture": "奈良県",
                "city": "大和郡山市",
                "remaining": ""
            }
        },
        {
            "input": "岐阜県郡上市",
            "expected": {
                "prefecture": "岐阜県",
                "city": "郡上市",
                "remaining": ""
            }
        }
    ]

    print("=== 郡対応テスト ===\n")

    all_passed = True
    for i, test in enumerate(test_cases, 1):
        result = splitter.split_address(test["input"])
        expected = test["expected"]

        print(f"テスト{i}: {test['input']}")
        print(f"  都道府県: {result['prefecture']} (期待: {expected['prefecture']})")
        print(f"  市区町村: {result['city']} (期待: {expected['city']})")
        print(f"  残り住所: {result['remaining']} (期待: {expected['remaining']})")

        if (result['prefecture'] == expected['prefecture'] and
            result['city'] == expected['city'] and
            result['remaining'] == expected['remaining']):
            print("  ✓ 成功\n")
        else:
            print("  ✗ 失敗\n")
            all_passed = False

    if all_passed:
        print("=== すべてのテストが成功しました！ ===")
    else:
        print("=== 一部のテストが失敗しました ===")

    return all_passed

if __name__ == "__main__":
    test_county_addresses()
