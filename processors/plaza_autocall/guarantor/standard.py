"""
プラザ保証人オートコール処理プロセッサー
統合アプリ用に移植
注意：プラザ保証人処理は現在未実装です（基本構造のみ提供）
"""

import pandas as pd
import io
from datetime import datetime
from typing import Tuple, List


def read_csv_auto_encoding(file_content: bytes) -> pd.DataFrame:
    """アップロードされたCSVファイルを自動エンコーディング判定で読み込み"""
    encodings = ['utf-8', 'utf-8-sig', 'shift_jis', 'cp932']
    
    for enc in encodings:
        try:
            return pd.read_csv(io.BytesIO(file_content), encoding=enc, dtype=str)
        except Exception:
            continue
    
    raise ValueError("CSVファイルの読み込みに失敗しました。エンコーディングを確認してください。")


def apply_plaza_guarantor_filters(df_contract: pd.DataFrame, df_report: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """プラザ保証人フィルタリング処理（基本構造のみ - 未実装）"""
    logs = []
    
    # TODO: プラザ保証人の具体的なフィルタリングロジックを実装
    logs.append("プラザ保証人処理は現在未実装です")
    logs.append("基本的なフィルタリング構造のみ提供")
    
    # 基本的な結合処理（他のプラザプロセッサーと同様）
    if "会員番号" not in df_contract.columns and "管理番号" in df_contract.columns:
        df_contract = df_contract.rename(columns={"管理番号": "会員番号"})
    
    merged = df_contract.merge(
        df_report[["会員番号", "延滞額合計", "緊急連絡人　電話番号"]],
        on="会員番号",
        how="left"
    )
    
    # 延滞額フィルター
    drop_values = ["0", "2", "3", "5"]
    merged = merged[~merged["延滞額合計"].isin(drop_values)]
    
    # TEL無効除外
    merged = merged[~merged["緊急連絡人　電話番号"].str.contains("TEL無効", na=False)]
    
    # 回収ランクフィルター
    if "回収ランク" in merged.columns:
        merged = merged[~merged["回収ランク"].isin(["督促停止", "弁護士介入"])]
    
    logs.append(f"基本フィルタリング後: {len(merged)}件")
    
    return merged, logs


def create_plaza_guarantor_output(df_filtered: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """プラザ保証人出力データ作成（基本構造のみ）"""
    logs = []
    
    # TODO: 保証人用の電話番号フィールドを特定・処理
    # 現在は基本構造のみ
    
    output_data = []
    for _, row in df_filtered.iterrows():
        output_row = {
            "電話番号": "",  # TODO: 保証人の電話番号フィールドを設定
            "架電番号": "",  # TODO: 保証人の電話番号フィールドを設定
            "管理番号": str(row.get("会員番号", "")),
            "契約者名（カナ）": str(row.get("契約者カナ", "")),
            "物件名": str(row.get("物件名", "")),
            "クライアント": "プラザ賃貸管理保証",
            "入居ステータス": "入居中",
            "滞納ステータス": "未精算"
        }
        output_data.append(output_row)
    
    df_output = pd.DataFrame(output_data)
    logs.append(f"保証人出力データ作成完了（未実装）: {len(df_output)}件")
    
    return df_output, logs


def process_plaza_guarantor_data(contract_content: bytes, report_content: bytes) -> Tuple[pd.DataFrame, pd.DataFrame, List[str], str]:
    """
    プラザ保証人データの処理メイン関数（基本構造のみ）
    
    Args:
        contract_content: ContractListのファイル内容
        report_content: Excel報告書のファイル内容
        
    Returns:
        tuple: (フィルタ済みDF, 出力DF, 処理ログ, 出力ファイル名)
    """
    try:
        logs = []
        
        # 未実装の警告
        logs.append("⚠️ プラザ保証人処理は現在未実装です")
        logs.append("基本構造のみ提供しています")
        
        # 1. CSVファイル読み込み
        logs.append("ContractList読み込み開始")
        df_contract = read_csv_auto_encoding(contract_content)
        logs.append(f"ContractList読み込み完了: {len(df_contract)}件")
        
        # 2. Excel報告書読み込み  
        logs.append("Excel報告書読み込み開始")
        df_report = pd.read_excel(io.BytesIO(report_content), dtype=str)
        logs.append(f"Excel報告書読み込み完了: {len(df_report)}件")
        
        # 3. フィルタリング処理（基本構造のみ）
        df_filtered, filter_logs = apply_plaza_guarantor_filters(df_contract, df_report)
        logs.extend(filter_logs)
        
        # 4. 出力データ作成（基本構造のみ）
        df_output, output_logs = create_plaza_guarantor_output(df_filtered)
        logs.extend(output_logs)
        
        # 5. 出力ファイル名生成
        today_str = datetime.now().strftime("%m%d")
        output_filename = f"{today_str}プラザ_保証人.csv"
        
        return df_filtered, df_output, logs, output_filename
        
    except Exception as e:
        raise Exception(f"プラザ保証人データ処理エラー: {str(e)}")


def get_sample_template() -> pd.DataFrame:
    """サンプルテンプレートを返す（デバッグ用）"""
    columns = [
        "電話番号", "架電番号", "管理番号", "契約者名（カナ）",
        "物件名", "クライアント", "入居ステータス", "滞納ステータス"
    ]
    return pd.DataFrame(columns=columns)