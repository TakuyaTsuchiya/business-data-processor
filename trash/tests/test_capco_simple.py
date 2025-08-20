"""
CAPCO新規登録処理の簡易テスト
基本的な動作確認を行う
"""

import pandas as pd
import io
from processors.capco_import_new_data_v2 import process_capco_import_new_data_v2

def create_sample_capco_data():
    """サンプルのカプコ元データを作成"""
    data = {
        "契約No": ["100001", "100002", "100003"],
        "契約者名": ["山田 太郎", "佐藤 花子", "田中 次郎"], 
        "契約者ふりがな": ["やまだ たろう", "さとう はなこ", "たなか じろう"],
        "契約者：電話番号": ["03-1234-5678", "", "06-9876-5432"],
        "契約者：携帯番号": ["090-1111-2222", "080-3333-4444", ""],
        "建物：郵便番号": ["100-0001", "200-0002", "300-0003"],
        "建物：住所": ["東京都千代田区千代田1-1-1", "神奈川県横浜市西区みなとみらい2-2-2", "大阪府大阪市北区梅田3-3-3"],
        "建物名": ["サンプルマンション", "テストアパート", "デモハイツ"],
        "部屋名": ["101", "202", "303"],
        "V口座銀行名": ["三井住友銀行", "三井住友銀行", "三井住友銀行"],
        "V口振支店名": ["中央支店", "東海支店", "中央支店"],
        "V口振番号": ["1234567", "2345678", "3456789"],
        "V口座振込先": ["ヤマダタロウ", "サトウハナコ", "タナカジロウ"],
        "約定日": ["1004/01/01", "1005/01/01", "1004/01/01"],
        "滞納額合計": ["50000", "75000", "30000"],
        "管理会社": ["管理会社A", "管理会社B", "管理会社C"],
        "契約開始": ["2024/01/15", "2024/02/20", "2024/03/10"]
    }
    return pd.DataFrame(data)

def create_sample_contract_data():
    """サンプルのContractListデータを作成"""
    data = {
        "引継番号": ["100004", "100005", "100006"],  # 重複しない番号
        "契約者氏名": ["既存 太郎", "既存 花子", "既存 次郎"],
        "管理番号": ["M001", "M002", "M003"]
    }
    return pd.DataFrame(data)

def test_capco_processing():
    """CAPCO処理のテスト実行"""
    print("=== CAPCO新規登録処理テスト開始 ===")
    
    # サンプルデータ作成
    capco_df = create_sample_capco_data()
    contract_df = create_sample_contract_data()
    
    print(f"カプコデータ: {len(capco_df)}件")
    print(f"ContractListデータ: {len(contract_df)}件")
    
    # DataFrameをCSVバイト形式に変換
    capco_csv = capco_df.to_csv(index=False, encoding='utf-8').encode('utf-8')
    contract_csv = contract_df.to_csv(index=False, encoding='utf-8').encode('utf-8')
    
    try:
        # CAPCO処理実行
        result_df, logs, filename = process_capco_import_new_data_v2(capco_csv, contract_csv)
        
        print("\n=== 処理結果 ===")
        print(f"出力ファイル名: {filename}")
        print(f"出力件数: {len(result_df)}件")
        print(f"出力列数: {len(result_df.columns)}列")
        
        print("\n=== 処理ログ ===")
        for log in logs:
            print(f"  {log}")
        
        if len(result_df) > 0:
            print("\n=== 出力データサンプル ===")
            # 主要列のみ表示
            display_columns = [
                "引継番号", "契約者氏名", "契約者カナ", "契約者TEL携帯",
                "契約者現住所1", "契約者現住所2", "物件名", "部屋番号",
                "入居ステータス", "滞納ステータス", "クライアントCD", "管理前滞納額"
            ]
            available_columns = [col for col in display_columns if col in result_df.columns]
            print(result_df[available_columns].head())
            
            print("\n=== データ変換検証 ===")
            # 具体的な変換結果をチェック
            first_row = result_df.iloc[0]
            print(f"引継番号: {first_row['引継番号']}")
            print(f"契約者氏名: {first_row['契約者氏名']}")
            print(f"契約者カナ: {first_row['契約者カナ']}")
            print(f"契約者TEL携帯: {first_row['契約者TEL携帯']}")
            print(f"契約者現住所1: {first_row['契約者現住所1']}")
            print(f"入居ステータス: {first_row['入居ステータス']}")
            print(f"滞納ステータス: {first_row['滞納ステータス']}")
            print(f"クライアントCD: {first_row['クライアントCD']}")
            print(f"回収口座支店CD: {first_row['回収口座支店CD']}")
            
        print("\n=== テスト完了 ===")
        return True
        
    except Exception as e:
        print(f"\n❌ エラー発生: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_capco_processing()