#!/usr/bin/env python3
"""
ARC退去手続き費用計算のテストケース
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from processors.ark_registration import DataConverter

def test_exit_fee_calculation():
    """退去手続き費用計算のテスト"""
    
    converter = DataConverter()
    
    # テストケース定義（月額賃料, 管理費, 駐車場代, その他料金, 期待値）
    test_cases = [
        # 基本ケース
        ("50000", "5000", "10000", "3000", 70000),  # 合計68,000円 → 最低70,000円
        ("60000", "5000", "10000", "5000", 80000),  # 合計80,000円
        ("100000", "10000", "15000", "5000", 130000),  # 合計130,000円
        
        # その他料金の影響確認
        ("50000", "5000", "10000", "0", 70000),     # その他料金なし: 65,000円 → 70,000円
        ("50000", "5000", "10000", "5000", 70000),  # その他料金あり: 70,000円
        ("50000", "5000", "10000", "10000", 75000), # その他料金で70,000円超過
        
        # エッジケース
        ("0", "0", "0", "0", 70000),                # 全て0円 → 最低70,000円
        ("", "", "", "", 70000),                    # 空文字 → 最低70,000円
        ("30,000", "5,000", "10,000", "5,000", 70000),  # カンマ付き: 50,000円 → 70,000円
        ("￥80,000", "￥5,000", "￥10,000", "￥5,000", 100000),  # 円記号付き
        
        # 大きな金額
        ("200000", "20000", "30000", "10000", 260000),  # 高額物件
        
        # 文字列混入
        ("abc", "def", "ghi", "jkl", 70000),        # 無効な文字列 → 最低70,000円
        ("50000円", "5000円", "10000円", "5000円", 70000),  # 「円」付き → 無効扱い
    ]
    
    print("=== ARC退去手続き費用計算テスト ===")
    print("計算式: MAX(月額賃料 + 管理費 + 駐車場代 + その他料金, 70,000円)")
    print()
    
    passed = 0
    failed = 0
    
    for rent, mgmt, parking, other, expected in test_cases:
        result = converter.calculate_exit_procedure_fee(rent, mgmt, parking, other)
        status = "PASS" if result == expected else "FAIL"
        
        # 入力値の合計を計算（表示用）
        try:
            total_input = 0
            for fee in [rent, mgmt, parking, other]:
                if fee:
                    clean = str(fee).replace(',', '').replace('￥', '').strip()
                    if clean.isdigit():
                        total_input += int(clean)
            input_str = f"(入力合計: {total_input:,}円)"
        except:
            input_str = "(入力合計: 計算不可)"
        
        print(f"賃料:{rent}, 管理費:{mgmt}, 駐車場:{parking}, その他:{other}")
        print(f"  → 結果: {result:,}円 (期待値: {expected:,}円) {input_str} {status}")
        print()
        
        if result == expected:
            passed += 1
        else:
            failed += 1
    
    print(f"=== テスト結果 ===")
    print(f"合計: {len(test_cases)} / 成功: {passed} / 失敗: {failed}")
    
    if failed == 0:
        print("全てのテストが成功しました！")
        return True
    else:
        print(f"{failed}個のテストが失敗しました。")
        return False

def test_integration():
    """統合テスト: その他料金が正しく読み込まれるか確認"""
    print("\n=== 統合テスト ===")
    
    # テスト用データ
    test_row = {
        "月額賃料": "50000",
        "管理費": "5000",
        "駐車場": "10000",
        "その他料金": "8000",  # 新規追加項目
    }
    
    converter = DataConverter()
    
    # 各料金を取得
    rent = converter.safe_str_convert(test_row.get("月額賃料", "0"))
    mgmt = converter.safe_str_convert(test_row.get("管理費", "0"))
    parking = converter.safe_str_convert(test_row.get("駐車場", "0"))
    other = converter.safe_str_convert(test_row.get("その他料金", "0"))
    
    # 退去費用計算
    exit_fee = converter.calculate_exit_procedure_fee(rent, mgmt, parking, other)
    
    print(f"入力データ: {test_row}")
    print(f"計算結果: {exit_fee:,}円")
    print(f"期待値: 73,000円 (50,000 + 5,000 + 10,000 + 8,000)")
    
    if exit_fee == 73000:
        print("統合テスト: PASS")
        return True
    else:
        print("統合テスト: FAIL")
        return False

if __name__ == "__main__":
    # 単体テスト実行
    unit_success = test_exit_fee_calculation()
    
    # 統合テスト実行
    integration_success = test_integration()
    
    if unit_success and integration_success:
        print("\nテスト完了：退去手続き費用計算（その他料金追加）は正常に動作します！")
        sys.exit(0)
    else:
        print("\nテスト失敗：修正が必要です。")
        sys.exit(1)