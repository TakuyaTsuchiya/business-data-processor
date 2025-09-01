"""
ミライル契約者データ処理プロセッサー（残債除外版）
統合アプリ用に移植・最適化
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
from domain.rules.business_rules import MIRAIL_DEBT_EXCLUDE

# 新しいフィルタ関数のインポート
from .filters import (
    filter_client_id,
    filter_payment_date,
    filter_collection_rank,
    filter_mirail_special_debt,
    filter_mobile_phone,
    filter_exclude_amounts,
    MIRAIL_CONTRACT_WITHOUT10K_FILTERS
)


class MirailConfig:
    """ミライル処理の設定"""
    
    # フィルタリング条件
    FILTER_CONDITIONS = {
        "委託先法人ID": "空白と5",
        "入金予定日": "前日以前またはNaN",
        "回収ランク_not_in": ["弁護士介入"],
        "滞納残債_not_in": MIRAIL_DEBT_EXCLUDE,
        "TEL携帯": "空でない値のみ",
        "入金予定金額_not_in": [2, 3, 5, 12]
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
        "クライアント": "クライアント名",
        "残債": "滞納残債"  # J列「残債」にBT列「滞納残債」を格納
    }
    
    OUTPUT_FILE_PREFIX = "ミライル_without10k_契約者"


def read_csv_auto_encoding(file_content: bytes) -> pd.DataFrame:
    """アップロードされたCSVファイルを自動エンコーディング判定で読み込み"""
    encodings = ['utf-8', 'utf-8-sig', 'shift_jis', 'cp932']
    
    for enc in encodings:
        try:
            return pd.read_csv(io.BytesIO(file_content), encoding=enc, dtype=str)
        except Exception:
            continue
    
    raise ValueError("CSVファイルの読み込みに失敗しました。エンコーディングを確認してください。")


def apply_filters_legacy(df_input: pd.DataFrame) -> Tuple[pd.DataFrame, list]:
    """
    ミライル契約者（残債10,000円・11,000円除外）フィルタリング処理
    
    📋 フィルタリング条件:
    - 委託先法人ID: 空白と5
    - 入金予定日: 前日以前またはNaN（当日は除外）
    - 回収ランク: 弁護士介入を除外
    - 残債除外: クライアントCD=1かつ滞納残債10,000円・11,000円のレコードのみ除外
    - TEL携帯: 空でない値のみ（契約者電話番号）
    
    Returns:
        tuple: (フィルタリング済みDataFrame, 処理ログ)
    """
    df = df_input.copy()
    logs = []
    filter_conditions = MirailConfig.FILTER_CONDITIONS
    
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
        
        exclude_condition = ((df["クライアントCD"] == 1) | (df["クライアントCD"] == 4)) & \
                           (df["滞納残債"].isin(filter_conditions["滞納残債_not_in"]))
        df = df[~exclude_condition]
        logs.append(f"クライアントCD=1,4かつ残債10,000円・11,000円除外後: {len(df)}件")
    
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
    
    logs.append(f"最終フィルタリング結果: {len(df)}件")
    return df, logs


def apply_filters_new(df_input: pd.DataFrame) -> Tuple[pd.DataFrame, list]:
    """
    新しいフィルタリング実装（個別フィルタ関数を使用）
    
    Returns:
        tuple: (フィルタリング済みDataFrame, 処理ログ)
    """
    df = df_input.copy()
    logs = []
    
    initial_count = len(df)
    logs.append(f"初期データ件数: {initial_count}件")
    
    # 日付の前処理（filter_payment_dateで使用）
    df["入金予定日"] = pd.to_datetime(df["入金予定日"], errors='coerce')
    
    # 数値の前処理
    df["クライアントCD"] = pd.to_numeric(df["クライアントCD"], errors="coerce")
    df["滞納残債"] = pd.to_numeric(df["滞納残債"].astype(str).str.replace(',', ''), errors='coerce')
    df["入金予定金額"] = pd.to_numeric(df["入金予定金額"], errors='coerce')
    
    # 基準日（今日）
    today = pd.Timestamp.now().normalize()
    
    # 各フィルタを順番に適用
    for filter_name, filter_func in MIRAIL_CONTRACT_WITHOUT10K_FILTERS:
        before_count = len(df)
        
        # フィルタの適用
        if filter_name == '入金予定日':
            # 日付フィルタは基準日を渡す
            mask = df.apply(lambda row: filter_func(row.to_dict(), today), axis=1)
        else:
            mask = df.apply(lambda row: filter_func(row.to_dict()), axis=1)
        
        # 除外されるデータの詳細を記録
        excluded_data = df[~mask]
        if len(excluded_data) > 0:
            if filter_name == '委託先法人ID':
                # 除外される委託先法人IDの内訳
                excluded_counts = excluded_data['委託先法人ID'].value_counts().to_dict()
                # NaNを文字列に変換して表示
                excluded_counts_str = {str(k) if pd.notna(k) else '空白': v for k, v in excluded_counts.items()}
                logs.append(f"委託先法人ID除外詳細: {excluded_counts_str}")
                
            elif filter_name == '入金予定日':
                # 除外される日付の内訳（上位10件）
                excluded_dates = excluded_data['入金予定日'].dt.strftime('%Y/%m/%d').value_counts().head(10).to_dict()
                total_excluded = len(excluded_data)
                logs.append(f"入金予定日除外詳細（上位10件）: {excluded_dates}")
                if total_excluded > 10:
                    logs.append(f"  ※他{total_excluded - 10}件の日付も除外")
                    
            elif filter_name == '回収ランク':
                # 除外される回収ランクの内訳
                excluded_ranks = excluded_data['回収ランク'].value_counts().to_dict()
                logs.append(f"回収ランク除外詳細: {excluded_ranks}")
                
            elif filter_name == 'ミライル特殊残債':
                # CD×残債の組み合わせ
                special_debt_data = excluded_data[['クライアントCD', '滞納残債']].copy()
                special_debt_counts = special_debt_data.groupby(['クライアントCD', '滞納残債']).size().to_dict()
                special_debt_str = {f"CD={int(k[0])}, {int(k[1])}円": v for k, v in special_debt_counts.items()}
                logs.append(f"ミライル特殊残債除外詳細: {special_debt_str}")
                
            elif filter_name == '携帯電話':
                # 携帯電話の除外理由を分類
                tel_data = excluded_data['TEL携帯'].astype(str).str.strip()
                empty_count = tel_data[tel_data.isin(['', 'nan', 'NaN'])].count()
                fixed_phone_count = len(excluded_data) - empty_count
                logs.append(f"携帯電話除外詳細: {{空白/NaN: {empty_count}件, 固定電話等: {fixed_phone_count}件}}")
                
            elif filter_name == '除外金額':
                # 除外される金額の内訳
                excluded_amounts = excluded_data['入金予定金額'].value_counts().to_dict()
                excluded_amounts_str = {f"{int(k)}円": v for k, v in excluded_amounts.items() if pd.notna(k)}
                logs.append(f"除外金額詳細: {excluded_amounts_str}")
        
        df = df[mask]
        after_count = len(df)
        
        logs.append(f"{filter_name}フィルタリング後: {after_count}件 (除外: {before_count - after_count}件)")
    
    logs.append(f"最終フィルタリング結果: {len(df)}件")
    return df, logs


def apply_filters(df_input: pd.DataFrame) -> Tuple[pd.DataFrame, list]:
    """
    フィルタリング処理（新実装を使用）
    """
    return apply_filters_new(df_input)


def create_template_dataframe(row_count: int) -> pd.DataFrame:
    """統一フォーマットのテンプレートDataFrameを作成"""
    # 28列の統一フォーマットで初期化（全て空文字）
    df_template = pd.DataFrame(index=range(row_count), columns=AUTOCALL_OUTPUT_COLUMNS)
    df_template = df_template.fillna("")
    return df_template


def map_data_to_template(df_filtered: pd.DataFrame) -> pd.DataFrame:
    """フィルタリング済みデータを28列の統一テンプレート形式にマッピング"""
    df_template = create_template_dataframe(len(df_filtered))
    mapping_rules = MirailConfig.MAPPING_RULES
    
    # マッピングルールに従ってデータを転記
    for template_col, input_col in mapping_rules.items():
        if template_col in df_template.columns and input_col in df_filtered.columns:
            df_template[template_col] = df_filtered[input_col].values
        elif template_col in df_template.columns:
            df_template[template_col] = ""
    
    return df_template


def process_mirail_data(file_content: bytes) -> Tuple[pd.DataFrame, pd.DataFrame, list, str]:
    """
    ミライル契約者データの処理メイン関数
    
    Args:
        file_content: アップロードされたCSVファイルの内容
        
    Returns:
        tuple: (最終出力DF, 処理済みDF, 処理ログ, 出力ファイル名)
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
        output_filename = f"{today_str}{MirailConfig.OUTPUT_FILE_PREFIX}.csv"
        
        return df_output, logs, output_filename
        
    except Exception as e:
        raise Exception(f"ミライルデータ処理エラー: {str(e)}")


def get_sample_template() -> pd.DataFrame:
    """サンプルテンプレートを返す（デバッグ用）"""
    return create_template_dataframe(0)


def process_mirail_contract_without10k_data(file_content: bytes) -> Tuple[pd.DataFrame, pd.DataFrame, list, str]:
    """
    ミライル契約者データ処理（app.py統合用）
    
    Args:
        file_content: CSVファイルの内容（bytes）
        
    Returns:
        Tuple[pd.DataFrame, pd.DataFrame, list, str]: (出力DF, フィルタリング済みDF, ログ, ファイル名)
    """
    try:
        # メイン処理を実行
        df_output, logs, output_filename = process_mirail_data(file_content)
        
        return df_output, logs, output_filename
        
    except Exception as e:
        # エラー時は空のデータを返す
        logs = [f"エラー: {str(e)}"]
        empty_df = pd.DataFrame()
        return empty_df, empty_df, logs, "error.csv"