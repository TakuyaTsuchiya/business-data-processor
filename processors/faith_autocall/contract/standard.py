"""
フェイス契約者オートコール処理プロセッサー
統合アプリ用に移植
"""

import pandas as pd
import io
import sys
import os
from datetime import datetime
from typing import Tuple, List

# 共通定義のインポート
processors_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if processors_dir not in sys.path:
    sys.path.append(processors_dir)
from autocall_common import AUTOCALL_OUTPUT_COLUMNS


def read_csv_auto_encoding(file_content: bytes) -> pd.DataFrame:
    """アップロードされたCSVファイルを自動エンコーディング判定で読み込み"""
    encodings = ['utf-8', 'utf-8-sig', 'shift_jis', 'cp932']
    
    for enc in encodings:
        try:
            return pd.read_csv(io.BytesIO(file_content), encoding=enc, dtype=str)
        except Exception:
            continue
    
    raise ValueError("CSVファイルの読み込みに失敗しました。エンコーディングを確認してください。")


def apply_faith_contract_filters(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """フェイス契約者フィルタリング処理"""
    logs = []
    original_count = len(df)
    logs.append(f"元データ件数: {original_count}件")
    
    # フィルタリング条件
    # 1. 委託先法人IDのフィルタリング（1,2,3,4のみ）
    df["委託先法人ID"] = pd.to_numeric(df["委託先法人ID"], errors="coerce")
    df = df[df["委託先法人ID"].isin([1, 2, 3, 4])]
    logs.append(f"委託先法人IDフィルタ後: {len(df)}件")
    
    # 2. 入金予定日のフィルタリング（前日以前またはNaN）
    today = pd.Timestamp.now().normalize()
    df["入金予定日"] = pd.to_datetime(df["入金予定日"], errors='coerce')
    df = df[df["入金予定日"].isna() | (df["入金予定日"] < today)]
    logs.append(f"入金予定日フィルタ後: {len(df)}件")
    
    # 3. 入金予定金額のフィルタリング（2,3,5を除外）
    df["入金予定金額"] = pd.to_numeric(df["入金予定金額"], errors='coerce')
    df = df[df["入金予定金額"].isna() | ~df["入金予定金額"].isin([2, 3, 5])]
    logs.append(f"入金予定金額フィルタ後: {len(df)}件")
    
    # 4. 回収ランクのフィルタリング（死亡決定、破産決定、弁護士介入を除外）
    exclude_ranks = ["死亡決定", "破産決定", "弁護士介入"]
    df = df[~df["回収ランク"].isin(exclude_ranks)]
    logs.append(f"回収ランクフィルタ後: {len(df)}件")
    
    # 5. TEL携帯のフィルタリング（契約者TEL携帯が必須）
    df = df[
        df["TEL携帯"].notna() &
        (~df["TEL携帯"].astype(str).str.strip().isin(["", "nan", "NaN"]))
    ]
    logs.append(f"TEL携帯フィルタ後: {len(df)}件")
    
    return df, logs


def create_faith_contract_output(df_filtered: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """フェイス契約者出力データ作成（28列統一フォーマット）"""
    logs = []
    
    # 28列の統一フォーマットで初期化
    df_output = pd.DataFrame(index=range(len(df_filtered)), columns=AUTOCALL_OUTPUT_COLUMNS)
    df_output = df_output.fillna("")
    
    # 出力用のマッピング（残債マッピングを削除）
    mapping_rules = {
        "電話番号": "TEL携帯",
        "架電番号": "TEL携帯", 
        "入居ステータス": "入居ステータス",
        "滞納ステータス": "滞納ステータス",
        "管理番号": "管理番号",
        "契約者名（カナ）": "契約者カナ",
        "物件名": "物件名"
        # 「残債」列は空白で統一
    }
    
    # データをマッピング
    for i, (_, row) in enumerate(df_filtered.iterrows()):
        # 基本マッピング
        for output_col, input_col in mapping_rules.items():
            if output_col in df_output.columns and input_col in row:
                df_output.at[i, output_col] = str(row[input_col]) if pd.notna(row[input_col]) else ""
        
        # クライアント列の生成（フェイス1, フェイス2, フェイス3, フェイス4）
        if "委託先法人ID" in row and pd.notna(row["委託先法人ID"]):
            df_output.at[i, "クライアント"] = f"フェイス{int(row['委託先法人ID'])}"
        else:
            df_output.at[i, "クライアント"] = ""
    
    logs.append(f"契約者出力データ作成完了: {len(df_output)}件")
    
    return df_output, logs


def process_faith_contract_data(file_content: bytes) -> Tuple[pd.DataFrame, pd.DataFrame, List[str], str]:
    """
    フェイス契約者データの処理メイン関数
    
    Args:
        file_content: ContractListのファイル内容
        
    Returns:
        tuple: (フィルタ済みDF, 出力DF, 処理ログ, 出力ファイル名)
    """
    try:
        logs = []
        
        # 1. CSVファイル読み込み
        logs.append("ファイル読み込み開始")
        df_input = read_csv_auto_encoding(file_content)
        logs.append(f"読み込み完了: {len(df_input)}件")
        
        # 必須列チェック
        required_columns = ["委託先法人ID", "TEL携帯", "回収ランク"]
        missing_columns = [col for col in required_columns if col not in df_input.columns]
        if missing_columns:
            raise ValueError(f"必須列が不足しています: {missing_columns}")
        
        # 2. フィルタリング処理
        df_filtered, filter_logs = apply_faith_contract_filters(df_input)
        logs.extend(filter_logs)
        
        # 3. 出力データ作成
        df_output, output_logs = create_faith_contract_output(df_filtered)
        logs.extend(output_logs)
        
        # 4. 出力ファイル名生成
        today_str = datetime.now().strftime("%m%d")
        output_filename = f"{today_str}フェイス_契約者.csv"
        
        return df_filtered, df_output, logs, output_filename
        
    except Exception as e:
        raise Exception(f"フェイス契約者データ処理エラー: {str(e)}")


def get_sample_template() -> pd.DataFrame:
    """サンプルテンプレートを返す（デバッグ用）"""
    columns = [
        "電話番号", "架電番号", "入居ステータス", "滞納ステータス",
        "管理番号", "契約者名（カナ）", "物件名", "残債", "クライアント"
    ]
    return pd.DataFrame(columns=columns)