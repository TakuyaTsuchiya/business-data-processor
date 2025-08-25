#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
町田市住所分割修正テスト
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from processors.capco_registration import DataConverter

def test_machida_address_fix():
    """町田市住所分割修正テスト"""
    converter = DataConverter()
    
    print("=== 町田市住所分割修正テスト ===")
    
    # テストケース1: 問題となっていた住所
    test_address = "東京都町田市市谷大谷1255-1"
    result = converter.split_address(test_address, "", "")
    
    print(f"入力: {test_address}")
    print(f"都道府県: '{result['prefecture']}'")
    print(f"市区町村: '{result['city']}'")  
    print(f"住所3: '{result['address3']}'")
    print()
    
    # 期待値チェック
    assert result['prefecture'] == "東京都", f"都道府県エラー: {result['prefecture']}"
    assert result['city'] == "町田市", f"市区町村エラー: {result['city']}"
    assert result['address3'] == "市谷大谷1255-1", f"住所3エラー: {result['address3']}"
    
    print("[OK] 町田市住所分割テスト成功")
    
    # テストケース2: 他の町田市住所
    test_cases = [
        "東京都町田市原町田3-2-9",
        "東京都町田市森野1-39-16",
        "東京都町田市中町1-5-3"
    ]
    
    for address in test_cases:
        result = converter.split_address(address, "", "")
        assert result['city'] == "町田市", f"町田市認識エラー: {address}"
        print(f"[OK] {address} -> 町田市")
    
    # テストケース3: 他の市への影響確認
    other_addresses = [
        ("千葉県市川市八幡2-5-20", "市川市"),
        ("千葉県市原市五井中央東1-1-1", "市原市"),
        ("北海道上川郡美瑛町本町1-1-1", "上川郡美瑛町")
    ]
    
    for address, expected_city in other_addresses:
        result = converter.split_address(address, "", "")
        assert result['city'] == expected_city, f"他市影響エラー: {address}"
        print(f"[OK] {address} -> {expected_city}")
    
    print("\n=== 全テスト成功！ ===")

if __name__ == "__main__":
    test_machida_address_fix()