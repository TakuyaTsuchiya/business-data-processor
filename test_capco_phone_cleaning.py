#!/usr/bin/env python3
"""
カプコ電話番号クリーニング機能のテストケース
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from processors.capco_registration import DataConverter

def test_phone_cleaning():
    """電話番号クリーニング機能のテスト"""
    
    converter = DataConverter()
    
    # テストケース定義
    test_cases = [
        # 基本的な混入パターン
        ("未080-5787-5364", "080-5787-5364"),
        ("×042-361-5460", "042-361-5460"),
        ("娘080-6868-0817", "080-6868-0817"),
        
        # その他の混入パターン
        ("自宅03-1234-5678", "03-1234-5678"),
        ("本人090-1234-5678", "090-1234-5678"),  
        ("代理080-9999-8888", "080-9999-8888"),
        ("妻090-8888-7777", "090-8888-7777"),
        ("父042-555-1234", "042-555-1234"),
        
        # 複雑な混入パターン
        ("TEL080-1111-2222", "080-1111-2222"),
        ("携帯090-3333-4444", "090-3333-4444"),
        ("連絡先03-5555-6666", "03-5555-6666"),
        
        # ハイフンなしパターン
        ("未08012345678", "080-1234-5678"),
        ("×0312345678", "03-1234-5678"),
        
        # クリーンなデータ（変更なし）
        ("080-1234-5678", "080-1234-5678"),
        ("03-1234-5678", "03-1234-5678"),
        ("042-123-4567", "042-123-4567"),
        
        # エッジケース
        ("", ""),                          # 空文字
        ("無効", ""),                     # 電話番号なし
        ("連絡不可", ""),                 # 電話番号なし
        ("123", ""),                      # 短すぎる数字
        ("abcd-efgh-ijkl", ""),           # 数字以外
    ]
    
    print("=== カプコ電話番号クリーニング機能テスト ===")
    print()
    
    passed = 0
    failed = 0
    
    for input_phone, expected in test_cases:
        result = converter.extract_clean_phone_number(input_phone)
        status = "PASS" if result == expected else "FAIL"
        
        print(f"Input: '{input_phone}' -> Output: '{result}' (Expected: '{expected}') {status}")
        
        if result == expected:
            passed += 1
        else:
            failed += 1
    
    print()
    print(f"=== テスト結果 ===")
    print(f"合計: {len(test_cases)} / 成功: {passed} / 失敗: {failed}")
    
    if failed == 0:
        print("全てのテストが成功しました！")
        return True
    else:
        print(f"{failed}個のテストが失敗しました。")
        return False

def test_process_phone_numbers():
    """process_phone_numbers関数の統合テスト"""
    
    converter = DataConverter()
    
    print("\n=== process_phone_numbers統合テスト ===")
    
    test_cases = [
        # (自宅電話, 携帯電話) -> (期待される自宅, 期待される携帯)
        ("未03-1234-5678", "×080-9876-5432", "03-1234-5678", "080-9876-5432"),
        ("娘042-111-2222", "", "", "042-111-2222"),  # 携帯空の場合移動
        ("", "本人090-3333-4444", "", "090-3333-4444"),
        ("無効", "連絡不可", "", ""),
    ]
    
    for home_input, mobile_input, expected_home, expected_mobile in test_cases:
        result_home, result_mobile = converter.process_phone_numbers(home_input, mobile_input)
        
        home_ok = result_home == expected_home
        mobile_ok = result_mobile == expected_mobile
        status = "PASS" if (home_ok and mobile_ok) else "FAIL"
        
        print(f"Input: ('{home_input}', '{mobile_input}')")
        print(f"Output: ('{result_home}', '{result_mobile}')")  
        print(f"Expected: ('{expected_home}', '{expected_mobile}') {status}")
        print()

if __name__ == "__main__":
    # 単体テスト実行
    success = test_phone_cleaning()
    
    # 統合テスト実行  
    test_process_phone_numbers()
    
    if success:
        print("テスト完了：電話番号クリーニング機能は正常に動作します！")
        sys.exit(0)
    else:
        print("テスト失敗：修正が必要です。")
        sys.exit(1)