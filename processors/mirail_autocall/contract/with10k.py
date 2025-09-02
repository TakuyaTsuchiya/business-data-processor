"""
ミライル（残債含む）契約者データ処理プロセッサー
統合アプリ用に移植
"""

import pandas as pd
import io
import sys
import os
from datetime import datetime
from typing import Tuple, List, Optional

# 共通定義のインポート
processors_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if processors_dir not in sys.path:
    sys.path.append(processors_dir)
from autocall_common import AUTOCALL_OUTPUT_COLUMNS
from common.contract_list_columns import ContractListColumns as COL
from common.detailed_logger import DetailedLogger


def read_csv_auto_encoding(file_content: bytes) -> pd.DataFrame:
    """アップロードされたCSVファイルを自動エンコーディング判定で読み込み"""
    encodings = ['utf-8', 'utf-8-sig', 'shift_jis', 'cp932']
    
    for enc in encodings:
        try:
            return pd.read_csv(io.BytesIO(file_content), encoding=enc, dtype=str)
        except Exception:
            continue
    
    raise ValueError("CSVファイルの読み込みに失敗しました。エンコーディングを確認してください。")


def apply_mirail_with10k_filters(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """
    ミライル契約者（残債含む）フィルタリング処理
    
    📋 フィルタリング条件 (with10k版):
    - 委託先法人ID: 空白と5（直接管理・特定委託案件）
    - 入金予定日: 前日以前またはNaN（当日は除外）
    - 回収ランク: 弁護士介入を除外
    - 残債含む: 全件処理（10,000円・11,000円も含む）
    - TEL携帯: 空でない値のみ（契約者電話番号）
    - 入金予定金額: 2,3,5,12円を除外（手数料関連）
    
    ⚠️ without10k版との違い:
    - without10k: 残債10,000円・11,000円を除外
    - with10k: 残債フィルタなし（全件処理）
    """
    logs = []
    original_count = len(df)
    logs.append(DetailedLogger.log_initial_load(original_count))
    
    # 📊 フィルタリング条件の適用
    # 1. 委託先法人IDが空白と5（直接管理・特定委託案件のみ対象）
    before_filter = len(df)
    
    # 新実装（列番号ベース）
    trustee_id_col = df.iloc[:, COL.TRUSTEE_ID].astype(str).str.strip()
    excluded_data = df[~(trustee_id_col.isin(["", "nan", "NaN", "5"]))]
    
    # 詳細ログ
    detail_log = DetailedLogger.log_exclusion_details(excluded_data, COL.TRUSTEE_ID, '委託先法人ID', 'id')
    if detail_log:
        logs.append(detail_log)
    
    # フィルタ適用
    df = df[trustee_id_col.isin(["", "nan", "NaN", "5"])]
    logs.append(DetailedLogger.log_filter_result(before_filter, len(df), '委託先法人ID（空白と5）'))
    
    # 2. 入金予定日のフィルタリング（前日以前またはNaN：当日は除外）
    today = pd.Timestamp.now().normalize()
    before_filter = len(df)
    
    # 新実装（列番号ベース）
    # 入金予定日列を一時的に変換
    temp_payment_date = pd.to_datetime(df.iloc[:, COL.PAYMENT_DATE], errors='coerce')
    excluded_data = df[~(temp_payment_date.isna() | (temp_payment_date < today))]
    
    # 詳細ログ（上位3件表示に変更）
    detail_log = DetailedLogger.log_exclusion_details(excluded_data, COL.PAYMENT_DATE, '入金予定日', 'date', top_n=3)
    if detail_log:
        logs.append(detail_log)
    
    # フィルタ適用
    df = df[temp_payment_date.isna() | (temp_payment_date < today)]
    logs.append(DetailedLogger.log_filter_result(before_filter, len(df), '入金予定日'))
    
    # 3. 回収ランクのフィルタリング（弁護士介入案件は除外）
    exclude_ranks = ["弁護士介入"]
    before_filter = len(df)
    
    # 新実装（列番号ベース）
    collection_rank_col = df.iloc[:, COL.COLLECTION_RANK]
    excluded_data = df[collection_rank_col.isin(exclude_ranks)]
    
    # 詳細ログ
    detail_log = DetailedLogger.log_exclusion_details(excluded_data, COL.COLLECTION_RANK, '回収ランク', 'category')
    if detail_log:
        logs.append(detail_log)
    
    # フィルタ適用
    df = df[~collection_rank_col.isin(exclude_ranks)]
    logs.append(DetailedLogger.log_filter_result(before_filter, len(df), '回収ランク'))
    
    # 4. 残債のフィルタリング（with10k版では除外なし - 全件処理）
    # ※ without10k版では残債10,000円・11,000円を除外するが、with10k版は全件処理
    logs.append("残債フィルタ: 除外なし（with10k版：10,000円・11,000円も含む全件処理）")
    logs.append("クライアントCDフィルタ: 除外なし（契約者版は全クライアント対象）")
    
    # 5. TEL携帯のフィルタリング（契約者電話番号が必須）
    before_filter = len(df)
    
    # 新実装（列番号ベース）
    tel_mobile_col = df.iloc[:, COL.TEL_MOBILE]
    # 有効な電話番号の判定
    tel_str = tel_mobile_col.astype(str).str.strip()
    valid_tel_mask = tel_mobile_col.notna() & (~tel_str.isin(["", "nan", "NaN"]))
    excluded_data = df[~valid_tel_mask]
    
    # 詳細ログ
    detail_log = DetailedLogger.log_exclusion_details(excluded_data, COL.TEL_MOBILE, '携帯電話', 'phone')
    if detail_log:
        logs.append(detail_log)
    
    # フィルタ適用
    df = df[valid_tel_mask]
    logs.append(DetailedLogger.log_filter_result(before_filter, len(df), 'TEL携帯'))
    
    # 6. 入金予定金額のフィルタリング（手数料関連の2,3,5,12円を除外）
    exclude_amounts = [2, 3, 5, 12]
    before_filter = len(df)
    
    # 新実装（列番号ベース）
    payment_amount_col = pd.to_numeric(df.iloc[:, COL.PAYMENT_AMOUNT], errors='coerce')
    excluded_data = df[payment_amount_col.isin(exclude_amounts)]
    
    # 詳細ログ
    detail_log = DetailedLogger.log_exclusion_details(excluded_data, COL.PAYMENT_AMOUNT, '除外金額', 'amount')
    if detail_log:
        logs.append(detail_log)
    
    # フィルタ適用
    df = df[~payment_amount_col.isin(exclude_amounts)]
    logs.append(DetailedLogger.log_filter_result(before_filter, len(df), '入金予定金額'))
    
    return df, logs


def create_mirail_with10k_output(df_filtered: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """ミライル（残債含む）出力データ作成（28列統一フォーマット）"""
    logs = []
    
    # 28列の統一フォーマットで初期化
    df_output = pd.DataFrame(index=range(len(df_filtered)), columns=AUTOCALL_OUTPUT_COLUMNS)
    df_output = df_output.fillna("")
    
    # 出力用のマッピング（列番号ベース）
    mapping_rules = {
        "電話番号": COL.TEL_MOBILE,
        "架電番号": COL.TEL_MOBILE, 
        "入居ステータス": COL.RESIDENCE_STATUS,
        "滞納ステータス": COL.DELINQUENT_STATUS,
        "管理番号": COL.MANAGEMENT_NO,
        "契約者名（カナ）": COL.CONTRACT_KANA,
        "物件名": COL.PROPERTY_NAME,
        "クライアント": COL.CLIENT_NAME,
        "残債": COL.DEBT_AMOUNT  # J列「残債」にBT列「滞納残債」を格納
    }
    
    # データをマッピング
    for i in range(len(df_filtered)):
        for output_col, col_index in mapping_rules.items():
            if output_col in df_output.columns:
                value = df_filtered.iloc[i, col_index]
                df_output.at[i, output_col] = str(value) if pd.notna(value) else ""
    
    logs.append(f"出力データ作成完了: {len(df_output)}件")
    
    return df_output, logs


def process_mirail_with10k_data(file_content: bytes) -> Tuple[pd.DataFrame, pd.DataFrame, List[str], str]:
    """
    ミライル契約者（残債含む）データの処理メイン関数
    
    📋 フィルタリング条件 (with10k版):
    - 委託先法人ID: 空白と5（直接管理・特定委託案件）
    - 入金予定日: 前日以前またはNaN（当日は除外）
    - 回収ランク: 弁護士介入を除外
    - 残債含む: 全件処理（10,000円・11,000円も含む）
    - TEL携帯: 空でない値のみ（契約者電話番号）
    - 入金予定金額: 2,3,5,12円を除外（手数料関連）
    
    ⚠️ without10k版との違い:
    - without10k: 残債10,000円・11,000円を除外
    - with10k: 残債フィルタなし（全件処理）
    
    Args:
        file_content: ContractListのファイル内容（bytes）
        
    Returns:
        tuple: (出力DF, フィルタ済みDF, 処理ログ, 出力ファイル名)
    """
    try:
        logs = []
        
        # 1. CSVファイル読み込み
        logs.append("ファイル読み込み開始")
        df_input = read_csv_auto_encoding(file_content)
        logs.append(f"読み込み完了: {len(df_input)}件")
        
        # 必須列チェック（列番号で確認）
        required_cols = {
            COL.TRUSTEE_ID: "委託先法人ID",
            COL.TEL_MOBILE: "TEL携帯",
            COL.COLLECTION_RANK: "回収ランク"
        }
        
        # 列数チェック
        if len(df_input.columns) <= max(required_cols.keys()):
            raise ValueError(f"CSVファイルの列数が不足しています。必要列数: {max(required_cols.keys()) + 1}列以上")
        
        # 2. フィルタリング処理
        df_filtered, filter_logs = apply_mirail_with10k_filters(df_input)
        logs.extend(filter_logs)
        
        # 3. 出力データ作成
        df_output, output_logs = create_mirail_with10k_output(df_filtered)
        logs.extend(output_logs)
        
        # 4. 出力ファイル名生成
        today_str = datetime.now().strftime("%m%d")
        output_filename = f"{today_str}ミライル_with10k_契約者.csv"
        
        return df_output, logs, output_filename
        
    except Exception as e:
        raise Exception(f"ミライル（残債含む）データ処理エラー: {str(e)}")


def get_sample_template() -> pd.DataFrame:
    """サンプルテンプレートを返す（デバッグ用）"""
    columns = [
        "電話番号", "架電番号", "入居ステータス", "滞納ステータス",
        "管理番号", "契約者名（カナ）", "物件名", "クライアント"
    ]
    return pd.DataFrame(columns=columns)


def process_mirail_contract_with10k_data(file_content: bytes) -> Tuple[pd.DataFrame, pd.DataFrame, list, str]:
    """
    ミライル契約者データ処理（残債含む版、app.py統合用）
    
    📋 フィルタリング条件 (with10k版):
    - 委託先法人ID: 空白と5（直接管理・特定委託案件）
    - 入金予定日: 前日以前またはNaN（当日は除外）
    - 回収ランク: 弁護士介入を除外
    - 残債含む: 全件処理（10,000円・11,000円も含む）
    - TEL携帯: 空でない値のみ（契約者電話番号）
    - 入金予定金額: 2,3,5,12円を除外（手数料関連）
    
    ⚠️ without10k版との違い:
    - without10k: 残債10,000円・11,000円を除外
    - with10k: 残債フィルタなし（全件処理）
    
    Args:
        file_content: CSVファイルの内容（bytes）
        
    Returns:
        Tuple[pd.DataFrame, pd.DataFrame, list, str]: (出力DF, フィルタリング済みDF, ログ, ファイル名)
    """
    try:
        # メイン処理を実行
        df_output, logs, output_filename = process_mirail_with10k_data(file_content)
        
        return df_output, logs, output_filename
        
    except Exception as e:
        # エラー時は空のデータを返す
        logs = [f"エラー: {str(e)}"]
        empty_df = pd.DataFrame()
        return empty_df, empty_df, logs, "error.csv"