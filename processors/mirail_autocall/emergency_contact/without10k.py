"""
ミライル緊急連絡人オートコール処理プロセッサー（残債10,000円・11,000円除外）
統合アプリ用に移植
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
    
    # 4. クライアントCDのフィルタリング（1のみ）
    df["クライアントCD"] = pd.to_numeric(df["クライアントCD"], errors="coerce")
    df = df[df["クライアントCD"] == 1]
    logs.append(f"クライアントCD=1フィルタ後: {len(df)}件")
    
    # 5. 滞納残債のフィルタリング（10,000円・11,000円除外）
    exclude_debts = [10000, 11000]
    df["滞納残債"] = pd.to_numeric(df["滞納残債"].astype(str).str.replace(',', ''), errors='coerce')
    df = df[~df["滞納残債"].isin(exclude_debts)]
    logs.append(f"滞納残債除外フィルタ後: {len(df)}件")
    
    # 6. TEL携帯.2のフィルタリング（緊急連絡人電話番号が必須）
    df = df[
        df["TEL携帯.2"].notna() &
        (~df["TEL携帯.2"].astype(str).str.strip().isin(["", "nan", "NaN"]))
    ]
    logs.append(f"TEL携帯.2フィルタ後: {len(df)}件")
    
    return df, logs


def create_mirail_emergencycontact_output(df_filtered: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """ミライル緊急連絡人出力データ作成"""
    logs = []
    
    # 出力用のマッピング
    mapping_rules = {
        "電話番号": "TEL携帯.2",
        "架電番号": "TEL携帯.2", 
        "入居ステータス": "入居ステータス",
        "滞納ステータス": "滞納ステータス",
        "管理番号": "管理番号",
        "契約者名（カナ）": "契約者カナ",
        "物件名": "物件名",
        "クライアント": "クライアント名"
    }
    
    # データが0件の場合でも列構造を維持
    if len(df_filtered) == 0:
        df_output = pd.DataFrame(columns=list(mapping_rules.keys()))
        logs.append("緊急連絡人出力データ作成完了: 0件（フィルタリング後データなし）")
        return df_output, logs
    
    # 出力データ作成
    output_data = []
    for _, row in df_filtered.iterrows():
        output_row = {}
        for output_col, input_col in mapping_rules.items():
            if input_col in row:
                output_row[output_col] = str(row[input_col]) if pd.notna(row[input_col]) else ""
            else:
                output_row[output_col] = ""
        output_data.append(output_row)
    
    df_output = pd.DataFrame(output_data)
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