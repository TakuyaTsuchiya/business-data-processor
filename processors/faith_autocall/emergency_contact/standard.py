"""
フェイス緊急連絡人オートコール処理プロセッサー
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
from domain.rules.business_rules import CLIENT_IDS, EXCLUDE_AMOUNTS
from processors.common.detailed_logger import DetailedLogger


def read_csv_auto_encoding(file_content: bytes) -> pd.DataFrame:
    """アップロードされたCSVファイルを自動エンコーディング判定で読み込み"""
    encodings = ['utf-8', 'utf-8-sig', 'shift_jis', 'cp932']
    
    for enc in encodings:
        try:
            return pd.read_csv(io.BytesIO(file_content), encoding=enc, dtype=str)
        except Exception:
            continue
    
    raise ValueError("CSVファイルの読み込みに失敗しました。エンコーディングを確認してください。")


def apply_faith_emergencycontact_filters(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """
    フェイス緊急連絡人フィルタリング処理
    
    📋 フィルタリング条件:
    - 委託先法人ID: 1,2,3,4のみ（フェイス管理案件）
    - 入金予定日: 前日以前またはNaN（当日は除外）
    - 入金予定金額: 2,3,5を除外（手数料関連）
    - 回収ランク: 死亡決定、破産決定、弁護士介入を除外
    - 緊急連絡人１のTEL（携帯）: 空でない値のみ（緊急連絡人電話番号必須）
    """
    logs = []
    original_count = len(df)
    logs.append(DetailedLogger.log_initial_load(original_count))
    
    # 📊 フィルタリング条件の適用
    # 1. 委託先法人IDのフィルタリング（1,2,3,4のみ）
    df["委託先法人ID"] = pd.to_numeric(df["委託先法人ID"], errors="coerce")
    before_filter = len(df)
    # 除外されるデータの詳細を記録
    excluded_data = df[~df["委託先法人ID"].isin(CLIENT_IDS['faith'])]
    detail_log = DetailedLogger.log_exclusion_details(
        excluded_data, 
        df.columns.get_loc("委託先法人ID"),
        "委託先法人ID", 
        'id'
    )
    if detail_log:
        logs.append(detail_log)
    
    df = df[df["委託先法人ID"].isin(CLIENT_IDS['faith'])]
    logs.append(DetailedLogger.log_filter_result(before_filter, len(df), "委託先法人ID"))
    
    # 2. 入金予定日のフィルタリング（前日以前またはNaN、当日は除外）
    today = pd.Timestamp.now().normalize()
    df["入金予定日"] = pd.to_datetime(df["入金予定日"], errors='coerce')
    before_filter = len(df)
    # 除外されるデータの詳細を記録
    excluded_data = df[~(df["入金予定日"].isna() | (df["入金予定日"] < today))]
    detail_log = DetailedLogger.log_exclusion_details(
        excluded_data,
        df.columns.get_loc("入金予定日"),
        "入金予定日",
        'date'
    )
    if detail_log:
        logs.append(detail_log)
    
    df = df[df["入金予定日"].isna() | (df["入金予定日"] < today)]
    logs.append(DetailedLogger.log_filter_result(before_filter, len(df), "入金予定日"))
    
    # 3. 入金予定金額のフィルタリング（2,3,5を除外）
    df["入金予定金額"] = pd.to_numeric(df["入金予定金額"], errors='coerce')
    before_filter = len(df)
    # 除外されるデータの詳細を記録
    excluded_data = df[df["入金予定金額"].isin(EXCLUDE_AMOUNTS['faith'])]
    detail_log = DetailedLogger.log_exclusion_details(
        excluded_data,
        df.columns.get_loc("入金予定金額"),
        "入金予定金額",
        'amount'
    )
    if detail_log:
        logs.append(detail_log)
    
    df = df[df["入金予定金額"].isna() | ~df["入金予定金額"].isin(EXCLUDE_AMOUNTS['faith'])]
    logs.append(DetailedLogger.log_filter_result(before_filter, len(df), "入金予定金額"))
    
    # 4. 回収ランクのフィルタリング（死亡決定、破産決定、弁護士介入を除外）
    exclude_ranks = ["死亡決定", "破産決定", "弁護士介入"]
    before_filter = len(df)
    # 除外されるデータの詳細を記録
    excluded_data = df[df["回収ランク"].isin(exclude_ranks)]
    detail_log = DetailedLogger.log_exclusion_details(
        excluded_data,
        df.columns.get_loc("回収ランク"),
        "回収ランク",
        'category'
    )
    if detail_log:
        logs.append(detail_log)

    df = df[~df["回収ランク"].isin(exclude_ranks)]
    logs.append(DetailedLogger.log_filter_result(before_filter, len(df), "回収ランク"))

    # 5. 滞納残債のフィルタリング（1円以上のみ対象）
    df["滞納残債"] = pd.to_numeric(
        df["滞納残債"].astype(str).str.replace(',', ''),
        errors='coerce'
    )
    before_filter = len(df)
    # 除外されるデータの詳細を記録
    excluded_data = df[~(df["滞納残債"] >= 1)]
    detail_log = DetailedLogger.log_exclusion_details(
        excluded_data,
        df.columns.get_loc("滞納残債"),
        "滞納残債",
        'amount'
    )
    if detail_log:
        logs.append(detail_log)

    df = df[df["滞納残債"] >= 1]
    logs.append(DetailedLogger.log_filter_result(before_filter, len(df), "滞納残債（1円以上）"))

    # 6. 緊急連絡人１のTEL（携帯）のフィルタリング（緊急連絡人電話番号が必須）
    before_filter = len(df)
    # 除外されるデータの詳細を記録
    excluded_data = df[~(df["緊急連絡人１のTEL（携帯）"].notna() &
                        (~df["緊急連絡人１のTEL（携帯）"].astype(str).str.strip().isin(["", "nan", "NaN"])))]
    detail_log = DetailedLogger.log_exclusion_details(
        excluded_data,
        df.columns.get_loc("緊急連絡人１のTEL（携帯）"),
        "緊急連絡人電話",
        'phone'
    )
    if detail_log:
        logs.append(detail_log)

    df = df[
        df["緊急連絡人１のTEL（携帯）"].notna() &
        (~df["緊急連絡人１のTEL（携帯）"].astype(str).str.strip().isin(["", "nan", "NaN"]))
    ]
    logs.append(DetailedLogger.log_filter_result(before_filter, len(df), "緊急連絡人１のTEL（携帯）"))

    return df, logs


def create_faith_emergencycontact_output(df_filtered: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """フェイス緊急連絡人出力データ作成（28列統一フォーマット）"""
    logs = []
    
    # 28列の統一フォーマットで初期化
    df_output = pd.DataFrame(index=range(len(df_filtered)), columns=AUTOCALL_OUTPUT_COLUMNS)
    df_output = df_output.fillna("")
    
    # 出力用のマッピング
    mapping_rules = {
        "電話番号": "緊急連絡人１のTEL（携帯）",
        "架電番号": "緊急連絡人１のTEL（携帯）",
        "入居ステータス": "入居ステータス",
        "滞納ステータス": "滞納ステータス",
        "管理番号": "管理番号",
        "契約者名（カナ）": "契約者カナ",  # 緊急連絡人でも契約者名を入れる
        "物件名": "物件名",
        "残債": "滞納残債"
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
    
    logs.append(f"緊急連絡人出力データ作成完了: {len(df_output)}件")
    
    return df_output, logs


def process_faith_emergencycontact_data(file_content: bytes) -> Tuple[pd.DataFrame, pd.DataFrame, List[str], str]:
    """
    フェイス緊急連絡人データの処理メイン関数
    
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
        required_columns = ["委託先法人ID", "緊急連絡人１のTEL（携帯）", "回収ランク"]
        missing_columns = [col for col in required_columns if col not in df_input.columns]
        if missing_columns:
            raise ValueError(f"必須列が不足しています: {missing_columns}")
        
        # 2. フィルタリング処理
        df_filtered, filter_logs = apply_faith_emergencycontact_filters(df_input)
        logs.extend(filter_logs)
        
        # 3. 出力データ作成
        df_output, output_logs = create_faith_emergencycontact_output(df_filtered)
        logs.extend(output_logs)
        
        # 4. 出力ファイル名生成
        today_str = datetime.now().strftime("%m%d")
        output_filename = f"{today_str}フェイス_連絡人.csv"
        
        return df_output, logs, output_filename
        
    except Exception as e:
        raise Exception(f"フェイス緊急連絡人データ処理エラー: {str(e)}")


def get_sample_template() -> pd.DataFrame:
    """サンプルテンプレートを返す（デバッグ用）"""
    columns = [
        "電話番号", "架電番号", "入居ステータス", "滞納ステータス",
        "管理番号", "契約者名（カナ）", "物件名", "クライアント"
    ]
    return pd.DataFrame(columns=columns)