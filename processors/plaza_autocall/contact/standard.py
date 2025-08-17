"""
プラザ緊急連絡人オートコール処理プロセッサー
ミライル緊急連絡人（10,000円を除外しないパターン）ベースの処理
プラザ固有条件：委託先法人ID=6、入金予定日=前日以前（当日除外）
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


def apply_plaza_contact_filters(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """
    プラザ緊急連絡人フィルタリング処理（ミライルwith10kベース + プラザ固有条件）
    
    フィルタリング条件:
    - 委託先法人ID: 6のみ（プラザ固有）
    - 入金予定日: 前日以前またはNaN（当日は除外、プラザ固有）
    - 回収ランク: 弁護士介入を除外
    - 残債: フィルタなし（10,000円・11,000円も含む全件処理）
    - 緊急連絡人１のTEL（携帯）: 空でない値のみ
    - 入金予定金額: 2,3,5,12円を除外（手数料関連）
    """
    logs = []
    original_count = len(df)
    logs.append(f"元データ件数: {original_count}件")
    
    # 1. 委託先法人IDが6のみ（プラザ固有）
    df = df[df["委託先法人ID"].astype(str).str.strip() == "6"]
    logs.append(f"委託先法人ID（6のみ）フィルタ後: {len(df)}件")
    
    # 2. 入金予定日のフィルタリング（前日以前またはNaN：当日は除外）
    today = pd.Timestamp.now().normalize()
    df["入金予定日"] = pd.to_datetime(df["入金予定日"], errors='coerce')
    df = df[df["入金予定日"].isna() | (df["入金予定日"] < today)]
    logs.append(f"入金予定日フィルタ後: {len(df)}件")
    
    # 3. 回収ランクのフィルタリング（督促停止・弁護士介入案件は除外）
    exclude_ranks = ["督促停止", "弁護士介入"]
    df = df[~df["回収ランク"].isin(exclude_ranks)]
    logs.append(f"回収ランクフィルタ後: {len(df)}件")
    
    # 4. 残債・クライアントCDのフィルタリング（with10k版では除外なし - 全件処理）
    logs.append("残債フィルタ: 除外なし（with10k版：10,000円・11,000円も含む全件処理）")
    logs.append("クライアントCDフィルタ: 除外なし（with10k版：全クライアント対象）")
    
    # 5. 入金予定金額のフィルタリング（2,3,5,12を除外）
    exclude_amounts = [2, 3, 5, 12]
    df["入金予定金額"] = pd.to_numeric(df["入金予定金額"], errors='coerce')
    df = df[df["入金予定金額"].isna() | ~df["入金予定金額"].isin(exclude_amounts)]
    logs.append(f"入金予定金額フィルタ後: {len(df)}件")
    
    # 6. 緊急連絡人１のTEL（携帯）のフィルタリング（緊急連絡人電話番号が必須）
    df = df[
        df["緊急連絡人１のTEL（携帯）"].notna() &
        (~df["緊急連絡人１のTEL（携帯）"].astype(str).str.strip().isin(["", "nan", "NaN"]))
    ]
    logs.append(f"緊急連絡人１のTEL（携帯）フィルタ後: {len(df)}件")
    
    return df, logs


def create_plaza_contact_output(df_filtered: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """プラザ緊急連絡人出力データ作成（28列統一フォーマット）"""
    logs = []
    
    # 28列の統一フォーマットで初期化
    df_output = pd.DataFrame(index=range(len(df_filtered)), columns=AUTOCALL_OUTPUT_COLUMNS)
    df_output = df_output.fillna("")
    
    # 出力用のマッピング（ミライルwith10kと同じ）
    mapping_rules = {
        "電話番号": "緊急連絡人１のTEL（携帯）",
        "架電番号": "緊急連絡人１のTEL（携帯）", 
        "入居ステータス": "入居ステータス",
        "滞納ステータス": "滞納ステータス",
        "管理番号": "管理番号",
        "契約者名（カナ）": "契約者カナ",  # 緊急連絡人でも契約者名を入れる
        "物件名": "物件名",
        "クライアント": "クライアント名",  # 入力データからマッピング
        "残債": "滞納残債"  # J列「残債」にBT列「滞納残債」を格納
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


def process_plaza_contact_data(file_content: bytes) -> Tuple[pd.DataFrame, pd.DataFrame, List[str], str]:
    """
    プラザ緊急連絡人データの処理メイン関数
    
    フィルタリング条件:
    - 委託先法人ID: 6のみ（プラザ固有）
    - 入金予定日: 前日以前またはNaN（当日は除外、プラザ固有）
    - 回収ランク: 弁護士介入を除外
    - 残債: フィルタなし（10,000円・11,000円も含む全件処理）
    - 緊急連絡人１のTEL（携帯）: 空でない値のみ
    - 入金予定金額: 2,3,5,12円を除外（手数料関連）
    
    Args:
        file_content: ContractListのファイル内容
        
    Returns:
        tuple: (出力DF, フィルタ済みDF, 処理ログ, 出力ファイル名)
    """
    try:
        logs = []
        
        # 1. CSVファイル読み込み
        logs.append("ファイル読み込み開始")
        df_input = read_csv_auto_encoding(file_content)
        logs.append(f"読み込み完了: {len(df_input)}件")
        
        # 必須列チェック
        required_columns = ["委託先法人ID", "緊急連絡人１のTEL（携帯）", "回収ランク"]
        missing_columns = [col for col in required_columns if col not in df_input.columns]
        if missing_columns:
            raise ValueError(f"必須列が不足しています: {missing_columns}")
        
        # 2. フィルタリング処理
        df_filtered, filter_logs = apply_plaza_contact_filters(df_input)
        logs.extend(filter_logs)
        
        # 3. 出力データ作成
        df_output, output_logs = create_plaza_contact_output(df_filtered)
        logs.extend(output_logs)
        
        # 4. 出力ファイル名生成
        today_str = datetime.now().strftime("%m%d")
        output_filename = f"{today_str}プラザ_緊急連絡人.csv"
        
        return df_filtered, df_output, logs, output_filename
        
    except Exception as e:
        raise Exception(f"プラザ緊急連絡人データ処理エラー: {str(e)}")


def get_sample_template() -> pd.DataFrame:
    """サンプルテンプレートを返す（デバッグ用）"""
    columns = [
        "電話番号", "架電番号", "入居ステータス", "滞納ステータス",
        "管理番号", "契約者名（カナ）", "物件名", "クライアント"
    ]
    return pd.DataFrame(columns=columns)