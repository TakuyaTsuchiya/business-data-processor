"""
ミライル緊急連絡人オートコール処理プロセッサー（残債10,000円・11,000円除外）
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


def apply_mirail_emergencycontact_without10k_filters(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """ミライル緊急連絡人（残債除外）フィルタリング処理"""
    logs = []
    original_count = len(df)
    logs.append(f"元データ件数: {original_count}件")
    
    # フィルタリング条件
    # 1. 委託先法人IDが空白と5
    df = df[df["委託先法人ID"].isna() | 
           (df["委託先法人ID"].astype(str).str.strip() == "") | 
           (df["委託先法人ID"].astype(str).str.strip() == "5")]
    logs.append(f"委託先法人ID（空白と5）フィルタ後: {len(df)}件")
    
    # 2. 入金予定日のフィルタリング（前日以前またはNaN）
    today = pd.Timestamp.now().normalize()
    df["入金予定日"] = pd.to_datetime(df["入金予定日"], errors='coerce')
    df = df[df["入金予定日"].isna() | (df["入金予定日"] < today)]
    logs.append(f"入金予定日フィルタ後: {len(df)}件")
    
    # 3. 回収ランクのフィルタリング（弁護士介入のみ除外）
    exclude_ranks = ["弁護士介入"]
    df = df[~df["回収ランク"].isin(exclude_ranks)]
    logs.append(f"回収ランクフィルタ後: {len(df)}件")
    
    # 4. 滞納残債とクライアントCDの複合フィルタリング
    # 「クライアントCD=1 かつ 滞納残債=10,000円・11,000円」のレコードのみ除外
    df["クライアントCD"] = pd.to_numeric(df["クライアントCD"], errors="coerce")
    df["滞納残債"] = pd.to_numeric(df["滞納残債"].astype(str).str.replace(',', ''), errors='coerce')
    
    exclude_debts = [10000, 11000]
    exclude_condition = (df["クライアントCD"] == 1) & (df["滞納残債"].isin(exclude_debts))
    df = df[~exclude_condition]
    logs.append(f"クライアントCD=1かつ残債10,000円・11,000円除外後: {len(df)}件")
    
    # 5. TEL携帯.2のフィルタリング（緊急連絡人電話番号が必須）
    df = df[
        df["TEL携帯.2"].notna() &
        (~df["TEL携帯.2"].astype(str).str.strip().isin(["", "nan", "NaN"]))
    ]
    logs.append(f"TEL携帯.2フィルタ後: {len(df)}件")
    
    return df, logs


def create_mirail_emergencycontact_output(df_filtered: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """ミライル緊急連絡人出力データ作成（28列統一フォーマット）"""
    logs = []
    
    # 28列の統一フォーマットで初期化
    df_output = pd.DataFrame(index=range(len(df_filtered)), columns=AUTOCALL_OUTPUT_COLUMNS)
    df_output = df_output.fillna("")
    
    # 出力用のマッピング
    mapping_rules = {
        "電話番号": "TEL携帯.2",
        "架電番号": "TEL携帯.2", 
        "入居ステータス": "入居ステータス",
        "滞納ステータス": "滞納ステータス",
        "管理番号": "管理番号",
        "契約者名（カナ）": "契約者カナ",  # 緊急連絡人でも契約者名を入れる
        "物件名": "物件名",
        "クライアント": "クライアント名"
    }
    
    # データが0件の場合
    if len(df_filtered) == 0:
        logs.append("緊急連絡人出力データ作成完了: 0件（フィルタリング後データなし）")
        return df_output, logs
    
    # データをマッピング
    for i, (_, row) in enumerate(df_filtered.iterrows()):
        for output_col, input_col in mapping_rules.items():
            if output_col in df_output.columns and input_col in row:
                df_output.at[i, output_col] = str(row[input_col]) if pd.notna(row[input_col]) else ""
    
    logs.append(f"緊急連絡人出力データ作成完了: {len(df_output)}件")
    
    return df_output, logs


def process_mirail_emergencycontact_without10k_data(file_content: bytes) -> Tuple[pd.DataFrame, pd.DataFrame, List[str], str]:
    """
    ミライル緊急連絡人（残債除外）データの処理メイン関数
    
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
        required_columns = ["委託先法人ID", "クライアントCD", "TEL携帯.2", "回収ランク"]
        missing_columns = [col for col in required_columns if col not in df_input.columns]
        if missing_columns:
            raise ValueError(f"必須列が不足しています: {missing_columns}")
        
        # 2. フィルタリング処理
        df_filtered, filter_logs = apply_mirail_emergencycontact_without10k_filters(df_input)
        logs.extend(filter_logs)
        
        # 3. 出力データ作成
        df_output, output_logs = create_mirail_emergencycontact_output(df_filtered)
        logs.extend(output_logs)
        
        # 4. 出力ファイル名生成
        today_str = datetime.now().strftime("%m%d")
        output_filename = f"{today_str}ミライル_without10k_緊急連絡人.csv"
        
        return df_filtered, df_output, logs, output_filename
        
    except Exception as e:
        raise Exception(f"ミライル緊急連絡人（残債除外）データ処理エラー: {str(e)}")


def get_sample_template() -> pd.DataFrame:
    """サンプルテンプレートを返す（デバッグ用）"""
    columns = [
        "電話番号", "架電番号", "入居ステータス", "滞納ステータス",
        "管理番号", "契約者名（カナ）", "物件名", "クライアント"
    ]
    return pd.DataFrame(columns=columns)