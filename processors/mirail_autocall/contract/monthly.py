"""
ミライル契約者データ処理プロセッサー（月初版）
without10k.pyベース + 追加フィルタリング条件
"""

import pandas as pd
import io
import sys
import os
from datetime import datetime
from typing import Tuple, Optional

# 共通定義のインポート
processors_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if processors_dir not in sys.path:
    sys.path.append(processors_dir)
from autocall_common import AUTOCALL_OUTPUT_COLUMNS


class MirailMonthlyConfig:
    """ミライル月初処理の設定"""
    
    # フィルタリング条件（without10kベース + 追加条件）
    FILTER_CONDITIONS = {
        "委託先法人ID": "空白と5",
        "入金予定日": "前日以前またはNaN",
        "回収ランク_not_in": ["弁護士介入"],
        "滞納残債_not_in": [10000, 11000],
        "TEL携帯": "空でない値のみ",
        "入金予定金額_not_in": [2, 3, 5, 12],
        # 追加条件
        "クライアントCD_in": [1, 4, 5, 10, 40],
        "滞納比率_threshold": 1.3  # 1.3以上のみ対象（1.3未満を除外）
    }
    
    # マッピングルール
    MAPPING_RULES = {
        "電話番号": "TEL携帯",
        "架電番号": "TEL携帯",
        "入居ステータス": "入居ステータス",
        "滞納ステータス": "滞納ステータス",
        "管理番号": "管理番号",
        "契約者名（カナ）": "契約者カナ",
        "物件名": "物件名",
        "クライアント": "クライアント名"
    }
    
    OUTPUT_FILE_PREFIX = "ミライル_契約者_月初"


def read_csv_auto_encoding(file_content: bytes) -> pd.DataFrame:
    """アップロードされたCSVファイルを自動エンコーディング判定で読み込み"""
    encodings = ['utf-8', 'utf-8-sig', 'shift_jis', 'cp932']
    
    for enc in encodings:
        try:
            return pd.read_csv(io.BytesIO(file_content), encoding=enc, dtype=str)
        except Exception:
            continue
    
    raise ValueError("CSVファイルの読み込みに失敗しました。エンコーディングを確認してください。")


def apply_filters(df_input: pd.DataFrame) -> Tuple[pd.DataFrame, list]:
    """
    ミライル契約者（月初）フィルタリング処理
    
    📋 フィルタリング条件（without10kベース + 月初専用除外条件）:
    - 委託先法人ID: 空白と5
    - 入金予定日: 前日以前またはNaN（当日は除外）
    - 回収ランク: 弁護士介入を除外
    - 残債除外: クライアントCD=1かつ滞納残債10,000円・11,000円のレコードのみ除外
    - TEL携帯: 空でない値のみ（契約者電話番号）
    - 入金予定金額: 2,3,5,12除外
    
    月初専用除外条件:
    - 「クライアントCD ∈ {1,4,5,10,40} かつ 滞納比率 ≥ 1.3」のレコードを除外
    - 滞納比率 = 滞納残債 ÷ 月額賃料合計
    - 上記条件に該当しないレコードのみを最終出力
    
    Returns:
        tuple: (フィルタリング済みDataFrame, 処理ログ)
    """
    df = df_input.copy()
    logs = []
    filter_conditions = MirailMonthlyConfig.FILTER_CONDITIONS
    
    initial_count = len(df)
    logs.append(f"初期データ件数: {initial_count}件")
    
    # 委託先法人IDのフィルタリング（空白と5）
    if "委託先法人ID" in filter_conditions:
        df = df[df["委託先法人ID"].isna() | 
               (df["委託先法人ID"].astype(str).str.strip() == "") | 
               (df["委託先法人ID"].astype(str).str.strip() == "5")]
        logs.append(f"委託先法人IDフィルタリング後: {len(df)}件")
    
    # 入金予定日のフィルタリング（前日以前またはNaN、当日は除外）
    today = pd.Timestamp.now().normalize()
    df["入金予定日"] = pd.to_datetime(df["入金予定日"], errors='coerce')
    df = df[df["入金予定日"].isna() | (df["入金予定日"] < today)]
    logs.append(f"入金予定日フィルタリング後: {len(df)}件")
    
    # 回収ランクのフィルタリング（弁護士介入のみ除外）
    if "回収ランク_not_in" in filter_conditions:
        df = df[~df["回収ランク"].isin(filter_conditions["回収ランク_not_in"])]
        logs.append(f"回収ランクフィルタリング後: {len(df)}件")
    
    # 残債除外フィルタリング
    # 「クライアントCD=1 かつ 滞納残債=10,000円・11,000円」のレコードのみ除外
    # その他全てのレコードは対象（クライアントCD≠1や、CD=1でも残債が10k/11k以外）
    if "滞納残債_not_in" in filter_conditions:
        df["クライアントCD"] = pd.to_numeric(df["クライアントCD"], errors="coerce")
        df["滞納残債"] = pd.to_numeric(df["滞納残債"].astype(str).str.replace(',', ''), errors='coerce')
        
        exclude_condition = (df["クライアントCD"] == 1) & \
                           (df["滞納残債"].isin(filter_conditions["滞納残債_not_in"]))
        df = df[~exclude_condition]
        logs.append(f"クライアントCD=1かつ残債10,000円・11,000円除外後: {len(df)}件")
    
    # TEL携帯のフィルタリング（契約者電話番号が必須）
    if "TEL携帯" in filter_conditions:
        df = df[
            df["TEL携帯"].notna() &
            (~df["TEL携帯"].astype(str).str.strip().isin(["", "nan", "NaN"]))
        ]
        logs.append(f"TEL携帯フィルタリング後: {len(df)}件")
    
    # 入金予定金額のフィルタリング（2,3,5,12を除外）
    if "入金予定金額_not_in" in filter_conditions:
        df["入金予定金額"] = pd.to_numeric(df["入金予定金額"], errors='coerce')
        df = df[df["入金予定金額"].isna() | ~df["入金予定金額"].isin(filter_conditions["入金予定金額_not_in"])]
        logs.append(f"入金予定金額フィルタリング後: {len(df)}件")
    
    # 【追加条件】月初専用除外フィルタリング
    # 「クライアントCD ∈ {1,4,5,10,40} かつ 滞納比率 ≥ 1.3」のレコードを除外
    if "クライアントCD_in" in filter_conditions and "滞納比率_threshold" in filter_conditions:
        # 月額賃料合計を数値化
        df["月額賃料合計"] = pd.to_numeric(df["月額賃料合計"].astype(str).str.replace(',', ''), errors='coerce')
        
        # 滞納比率を計算：滞納残債 ÷ 月額賃料合計
        df["滞納比率"] = df["滞納残債"] / df["月額賃料合計"]
        
        # 除外条件：クライアントCD ∈ {1,4,5,10,40} かつ 滞納比率 ≥ 1.3
        threshold = filter_conditions["滞納比率_threshold"]
        mask_exclude = (
            df["クライアントCD"].isin(filter_conditions["クライアントCD_in"]) &
            (df["滞納比率"] >= threshold)
        )
        
        # 除外条件に該当しないレコードのみ残す
        df_excluded = df[mask_exclude]
        df = df[~mask_exclude]
        
        logs.append(f"月初除外条件(クライアントCD∈{{1,4,5,10,40}} かつ 滞納比率≥{threshold})で除外: {len(df_excluded)}件")
        logs.append(f"月初除外フィルタリング後: {len(df)}件")
    
    logs.append(f"最終フィルタリング結果: {len(df)}件")
    return df, logs


def create_template_dataframe(row_count: int) -> pd.DataFrame:
    """統一フォーマットのテンプレートDataFrameを作成"""
    # 28列の統一フォーマットで初期化（全て空文字）
    df_template = pd.DataFrame(index=range(row_count), columns=AUTOCALL_OUTPUT_COLUMNS)
    df_template = df_template.fillna("")
    return df_template


def map_data_to_template(df_filtered: pd.DataFrame) -> pd.DataFrame:
    """フィルタリング済みデータを28列の統一テンプレート形式にマッピング"""
    df_template = create_template_dataframe(len(df_filtered))
    mapping_rules = MirailMonthlyConfig.MAPPING_RULES
    
    # マッピングルールに従ってデータを転記
    for template_col, input_col in mapping_rules.items():
        if template_col in df_template.columns and input_col in df_filtered.columns:
            df_template[template_col] = df_filtered[input_col].values
        elif template_col in df_template.columns:
            df_template[template_col] = ""
    
    return df_template


def process_mirail_data(file_content: bytes) -> Tuple[pd.DataFrame, pd.DataFrame, list, str]:
    """
    ミライル契約者（月初）データの処理メイン関数
    
    Args:
        file_content: アップロードされたCSVファイルの内容
        
    Returns:
        tuple: (フィルタリング済みDF, 最終出力DF, 処理ログ, 出力ファイル名)
    """
    try:
        # 1. CSVファイル読み込み
        df_input = read_csv_auto_encoding(file_content)
        
        # 2. フィルタリング処理
        df_filtered, logs = apply_filters(df_input)
        
        # 3. テンプレートマッピング
        df_output = map_data_to_template(df_filtered)
        
        # 4. 出力ファイル名生成
        today_str = datetime.now().strftime("%m%d")
        output_filename = f"{today_str}{MirailMonthlyConfig.OUTPUT_FILE_PREFIX}.csv"
        
        return df_filtered, df_output, logs, output_filename
        
    except Exception as e:
        raise Exception(f"ミライル月初データ処理エラー: {str(e)}")


def get_sample_template() -> pd.DataFrame:
    """サンプルテンプレートを返す（デバッグ用）"""
    return create_template_dataframe(0)


def process_mirail_contract_monthly_data(file_content: bytes) -> Tuple[pd.DataFrame, pd.DataFrame, list, str]:
    """
    ミライル契約者（月初）データ処理（app.py統合用）
    
    Args:
        file_content: CSVファイルの内容（bytes）
        
    Returns:
        Tuple[pd.DataFrame, pd.DataFrame, list, str]: (フィルタリング済みDF, 出力DF, ログ, ファイル名)
    """
    try:
        # メイン処理を実行
        df_filtered, df_output, logs, output_filename = process_mirail_data(file_content)
        
        return df_filtered, df_output, logs, output_filename
        
    except Exception as e:
        # エラー時は空のデータを返す
        logs = [f"エラー: {str(e)}"]
        empty_df = pd.DataFrame()
        return empty_df, empty_df, logs, "error.csv"