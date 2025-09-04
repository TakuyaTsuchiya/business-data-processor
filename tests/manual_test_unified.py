"""
統合プロセッサーの手動動作確認スクリプト
実際のデータで動作を確認する際に使用
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from processors.mirail_autocall_unified import MirailAutocallUnifiedProcessor
import pandas as pd


def main():
    """動作確認のメイン関数"""
    print("=== ミライルオートコール統合プロセッサー動作確認 ===\n")
    
    # プロセッサーのインスタンス作成
    processor = MirailAutocallUnifiedProcessor()
    
    # 設定内容の確認
    print("対象者設定:")
    for target, config in processor.TARGET_CONFIG.items():
        print(f"  - {target}: 電話番号列={config['phone_column']}, 表示名={config['display_name']}")
    
    print(f"\n除外金額: {processor.COMMON_EXCLUDE_AMOUNTS}")
    print(f"特別残債除外（without10kのみ）: {processor.MIRAIL_DEBT_EXCLUDE}")
    
    # 全パターンの確認
    print("\n生成されるファイル名パターン:")
    targets = ["contract", "guarantor", "emergency_contact"]
    for target in targets:
        for with_10k in [True, False]:
            suffix = processor.TARGET_CONFIG[target]["name_suffix"]
            prefix = "with10k" if with_10k else "without10k"
            filename = f"MMDD ミライル_{prefix}_{suffix}.csv"
            print(f"  - {filename}")
    
    print("\n✅ 設定確認完了")
    
    # サンプルデータでのテスト
    print("\n=== サンプルデータでのテスト ===")
    
    # 簡単なサンプルデータ作成
    sample_data = pd.DataFrame({
        'col1': ['test1', 'test2'],
        'col2': ['data1', 'data2']
    })
    
    # CSVバイトデータに変換
    csv_bytes = sample_data.to_csv(index=False).encode('utf-8')
    
    try:
        # 契約者（with10k）でテスト実行
        result_df, logs, filename = processor.process_mirail_autocall(
            csv_bytes, "contract", with_10k=True
        )
        print(f"\nテスト実行成功！")
        print(f"出力ファイル名: {filename}")
        print(f"ログ数: {len(logs)}")
    except Exception as e:
        print(f"\nエラー（想定内）: {str(e)}")
        print("※実際のContractListフォーマットでないためエラーになります")
    
    print("\n=== 統合プロセッサーの準備完了 ===")


if __name__ == "__main__":
    main()