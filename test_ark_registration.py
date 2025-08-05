#!/usr/bin/env python3
"""
アーク新規登録プロセッサのテストスクリプト
住所分割ロジックと列名マッピングの動作確認
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from processors.ark_registration import split_address, safe_str_convert, remove_all_spaces, hankaku_to_zenkaku

def test_address_splitting():
    """住所分割テスト（政令指定都市対応）"""
    print("=== 住所分割テスト ===")
    
    test_cases = [
        "〒261-0024 千葉県千葉市美浜区豊砂1-1 イオンモール幕張新都心",
        "神奈川県横浜市港北区新横浜2-100-45 キュービックプラザ新横浜",
        "〒060-0001 北海道札幌市中央区北1条西5-2",
        "東京都渋谷区神宮前1-1-1",
        "大阪府大阪市淀川区西中島5-1-1"
    ]
    
    for address in test_cases:
        result = split_address(address)
        print(f"入力: {address}")
        print(f"  郵便番号: '{result['postal_code']}'")
        print(f"  都道府県: '{result['prefecture']}'")
        print(f"  市区町村: '{result['city']}'")
        print(f"  残り住所: '{result['remaining']}'")
        print()

def test_name_processing():
    """氏名処理テスト"""
    print("=== 氏名処理テスト ===")
    
    test_names = [
        "山田　太郎",
        "田中 花子",
        "佐藤　次郎",
        "ﾔﾏﾀﾞ ﾀﾛｳ",
        "タナカ　ハナコ"
    ]
    
    for name in test_names:
        clean_name = remove_all_spaces(safe_str_convert(name))
        clean_kana = remove_all_spaces(hankaku_to_zenkaku(safe_str_convert(name)))
        print(f"入力: '{name}' → 氏名: '{clean_name}' → カナ: '{clean_kana}'")

def test_column_mapping_simulation():
    """列名マッピングシミュレーションテスト"""
    print("=== 列名マッピングシミュレーション ===")
    
    # サンプルデータ（実際のCSVヘッダーに基づく）
    sample_row = {
        "契約番号": "12345",
        "契約元帳: 主契約者": "山田　太郎",
        "主契約者（カナ）": "ﾔﾏﾀﾞ ﾀﾛｳ",
        "生年月日": "1980/01/01",
        "物件住所": "〒261-0024 千葉県千葉市美浜区豊砂1-1",
        "物件名": "サンプルマンション101",
        "賃料": "80000",
        "管理共益費": "5000",
        "駐車場料金": "8000",
        "未収金額合計": "150000"
    }
    
    print("サンプルデータの処理結果:")
    for key, value in sample_row.items():
        if key in ["契約元帳: 主契約者", "主契約者（カナ）"]:
            processed = remove_all_spaces(safe_str_convert(value))
            if "カナ" in key:
                processed = hankaku_to_zenkaku(processed)
            print(f"  {key}: '{value}' → '{processed}'")
        elif key == "物件住所":
            addr_parts = split_address(value)
            print(f"  {key}: '{value}'")
            print(f"    → 郵便番号: '{addr_parts['postal_code']}'")
            print(f"    → 都道府県: '{addr_parts['prefecture']}'")
            print(f"    → 市区町村: '{addr_parts['city']}'")
            print(f"    → 残り住所: '{addr_parts['remaining']}'")
        else:
            print(f"  {key}: '{value}'")

if __name__ == "__main__":
    print("アーク新規登録プロセッサ テスト開始\n")
    
    try:
        test_address_splitting()
        test_name_processing()
        test_column_mapping_simulation()
        
        print("すべてのテストが正常に完了しました")
        
    except Exception as e:
        print(f"テストエラー: {str(e)}")
        import traceback
        traceback.print_exc()