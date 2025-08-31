#!/usr/bin/env python3
"""
SMS共通処理の動作確認スクリプト

共通化された8種類のSMS処理が正しく動作することを確認します。
"""

import sys
import pandas as pd
from datetime import datetime, date
from io import BytesIO

# プロジェクトのパスを追加
sys.path.append('.')

from processors.sms_common.factory import (
    process_faith_guarantor_sms,
    process_faith_emergency_sms,
    process_mirail_contract_sms,
    process_mirail_guarantor_sms,
    process_mirail_emergency_sms,
    process_plaza_guarantor_sms,
    process_plaza_emergency_sms
)


def create_test_data():
    """テスト用のCSVデータを作成"""
    data = {
        '契約者ID': ['001', '002', '003', '004', '005'],
        '契約者氏名': ['田中太郎', '佐藤花子', '鈴木一郎', '高橋二郎', '渡辺三郎'],
        '契約者カナ': ['タナカタロウ', 'サトウハナコ', 'スズキイチロウ', 'タカハシジロウ', 'ワタナベサブロウ'],
        '契約者携帯電話': ['090-1234-5678', '080-2345-6789', '070-3456-7890', 'invalid', '090-4567-8901'],
        'TEL携帯.1': ['090-1111-2222', '080-3333-4444', '070-5555-6666', '090-7777-8888', 'invalid'],
        'TEL携帯.2': ['090-9999-0000', 'invalid', '080-1111-2222', '070-3333-4444', '090-5555-6666'],
        '保証人氏名.1': ['保証人1', '保証人2', '保証人3', '保証人4', '保証人5'],
        '保証人カナ.1': ['ホショウニン1', 'ホショウニン2', 'ホショウニン3', 'ホショウニン4', 'ホショウニン5'],
        '緊急連絡先氏名.2': ['緊急1', '緊急2', '緊急3', '緊急4', '緊急5'],
        '緊急連絡先カナ.2': ['キンキュウ1', 'キンキュウ2', 'キンキュウ3', 'キンキュウ4', 'キンキュウ5'],
        '委託先法人ID': [1, 2, 3, 6, 5],
        '入金予定日': ['2024-01-01', '2024-01-02', pd.NaT, '2024-01-04', '2024-01-05'],
        '入金予定金額': [10000, 2, 3, 5000, 15000],
        '回収ランク': ['正常', '督促停止', '弁護士介入', '正常', '正常'],
        '物件名': ['アパートA', 'マンションB', 'ハイツC', 'レジデンスD', 'コーポE'],
        '部屋番号': ['101', '202', '303', '404', '505'],
        '顧客管理コード': ['C001', 'C002', 'C003', 'C004', 'C005'],
        '物件所在地１': ['東京都渋谷区', '東京都新宿区', '東京都港区', '東京都千代田区', '東京都中央区']
    }
    
    df = pd.DataFrame(data)
    
    # CSVバイトストリームに変換
    buffer = BytesIO()
    df.to_csv(buffer, index=False, encoding='cp932')
    buffer.seek(0)
    
    return buffer.getvalue()


def test_processor(processor_name, processor_func, system, target):
    """各プロセッサーをテスト"""
    print(f"\n{'='*60}")
    print(f"テスト: {processor_name}")
    print(f"システム: {system}, 対象: {target}")
    print(f"{'='*60}")
    
    try:
        # テストデータ作成
        test_data = create_test_data()
        payment_deadline = date.today()
        
        # 処理実行
        result_df, logs, filename, stats = processor_func(test_data, payment_deadline)
        
        # 結果表示
        print(f"✅ 処理成功")
        print(f"📊 統計情報:")
        print(f"  - 元データ: {stats['initial_rows']}件")
        print(f"  - 処理後: {stats['processed_rows']}件")
        print(f"📄 出力ファイル名: {filename}")
        print(f"📋 処理ログ:")
        for log in logs[:5]:  # 最初の5つのログを表示
            print(f"  - {log}")
        
        if not result_df.empty:
            print(f"📊 出力データサンプル:")
            print(result_df.head(2).to_string())
        else:
            print("⚠️  出力データなし（フィルタ条件に合致するデータがありませんでした）")
            
    except Exception as e:
        print(f"❌ エラー発生: {str(e)}")
        import traceback
        traceback.print_exc()


def main():
    """メイン処理"""
    print("SMS共通処理 動作確認テスト")
    print("=" * 80)
    
    # テストケース定義
    test_cases = [
        ("フェイス保証人SMS", process_faith_guarantor_sms, 'faith', 'guarantor'),
        ("フェイス緊急連絡人SMS", process_faith_emergency_sms, 'faith', 'emergency'),
        ("ミライル契約者SMS", process_mirail_contract_sms, 'mirail', 'contract'),
        ("ミライル保証人SMS", process_mirail_guarantor_sms, 'mirail', 'guarantor'),
        ("ミライル緊急連絡人SMS", process_mirail_emergency_sms, 'mirail', 'emergency'),
        ("プラザ保証人SMS", process_plaza_guarantor_sms, 'plaza', 'guarantor'),
        ("プラザ緊急連絡人SMS", process_plaza_emergency_sms, 'plaza', 'emergency'),
    ]
    
    # 各テストケースを実行
    for test_case in test_cases:
        test_processor(*test_case)
    
    print("\n" + "="*80)
    print("テスト完了")
    print("="*80)


if __name__ == "__main__":
    main()