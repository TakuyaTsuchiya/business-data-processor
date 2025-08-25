"""
ミライル契約者データ処理プロセッサー（残債除外版）
統合アプリ用に移植・最適化
"""

import pandas as pd
import sys
import os
from datetime import datetime
from typing import Tuple, Optional

# Infrastructure Layer のインポート
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if project_root not in sys.path:
    sys.path.append(project_root)

from infra.csv.reader import read_csv_auto_encoding
from infra.logging.logger import create_logger

# Domain Layer のインポート
from domain.filters.filter_factory import FilterFactory

# 共通定義のインポート
processors_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if processors_dir not in sys.path:
    sys.path.append(processors_dir)
from autocall_common import AUTOCALL_OUTPUT_COLUMNS


class MirailConfig:
    """ミライル処理の設定"""
    
    # フィルタリング条件
    FILTER_CONDITIONS = {
        "委託先法人ID": "空白と5",
        "入金予定日": "前日以前またはNaN",
        "回収ランク_not_in": ["弁護士介入"],
        "滞納残債_not_in": [10000, 11000],
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




def apply_filters(df_input: pd.DataFrame, logger) -> pd.DataFrame:
    """
    ミライル契約者（残債10,000円・11,000円除外）フィルタリング処理
    
    Domain Layer のフィルターシステムを使用した新しい実装
    
    Args:
        df_input (pd.DataFrame): 入力データ
        logger: ログ出力用ロガー
    
    Returns:
        pd.DataFrame: フィルタリング済みDataFrame
    """
    # Domain Layerのフィルターファクトリーを使用
    filter_instance = FilterFactory.create_mirail_filter(
        process_type="contract",
        debt_filter_enabled=True  # without10k版なので残債除外有効
    )
    
    # 統合フィルターを適用
    df_filtered = filter_instance.apply_all_filters(df_input, logger)
    
    return df_filtered


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
        tuple: (最終出力DF, フィルタリング済みDF, 処理ログ, 出力ファイル名)
    """
    # Infrastructure Layer のロガーを初期化
    logger = create_logger("ミライル契約者without10k")
    
    try:
        logger.info("処理開始")
        
        # 1. CSVファイル読み込み
        logger.info(
            "CSVファイル読み込み開始",
            operation="csv_file_loading",
            context={
                "file_size_bytes": len(file_content),
                "expected_encoding": "cp932"
            },
            human_note="AI-TODO: エンコーディングエラーが発生した場合は文字化け対応を確認してください"
        )
        df_input = read_csv_auto_encoding(file_content)
        logger.log_data_processing(
            "CSV読み込み", 
            0, 
            len(df_input), 
            f"列数: {len(df_input.columns)}",
            conditions={
                "encoding_detection": "auto",
                "columns_detected": len(df_input.columns)
            },
            human_note="AI-INFO: CSV読み込み成功。列数が期待値と大きく異なる場合はデータ形式を確認"
        )
        
        # 2. フィルタリング処理
        logger.info(
            "フィルタリング処理開始",
            operation="filtering_process",
            context={
                "processor_type": "mirail_contract_without10k",
                "debt_filter_enabled": True,
                "expected_filter_conditions": MirailConfig.FILTER_CONDITIONS
            },
            human_note="AI-TODO: フィルタリング後の件数が大幅に減少した場合は、フィルタ条件の妥当性を確認"
        )
        df_filtered = apply_filters(df_input, logger)
        logger.log_data_processing(
            "フィルタリング", 
            len(df_input), 
            len(df_filtered),
            conditions={
                "filter_type": "mirail_without10k",
                "debt_exclusion": [10000, 11000],
                "trustee_company_filter": ["空白", "5"]
            },
            human_note="AI-ANALYZE: 除外率をチェック。通常20-40%程度が適切"
        )
        
        # 3. テンプレートマッピング
        logger.info(
            "テンプレートマッピング開始",
            operation="template_mapping",
            context={
                "template_columns": len(AUTOCALL_OUTPUT_COLUMNS),
                "mapping_rules": MirailConfig.MAPPING_RULES,
                "input_rows": len(df_filtered)
            },
            human_note="AI-INFO: 28列統一テンプレートへのマッピング実行"
        )
        df_output = map_data_to_template(df_filtered)
        logger.log_mapping_result("28列テンプレート", len(df_output))
        
        # 4. 出力ファイル名生成
        today_str = datetime.now().strftime("%m%d")
        output_filename = f"{today_str}{MirailConfig.OUTPUT_FILE_PREFIX}.csv"
        logger.info(
            f"出力ファイル名: {output_filename}",
            operation="output_file_generation",
            context={
                "filename": output_filename,
                "date_prefix": today_str,
                "file_prefix": MirailConfig.OUTPUT_FILE_PREFIX,
                "output_rows": len(df_output)
            },
            human_note="AI-INFO: 出力ファイル名生成完了。ファイル名形式: MMDDミライル_without10k_契約者.csv"
        )
        
        logger.info(
            "処理完了",
            operation="process_complete",
            context={
                "total_processing_time": (datetime.now() - logger.start_time).total_seconds(),
                "final_output_count": len(df_output),
                "success": True
            },
            human_note="AI-SUCCESS: ミライル契約者without10k処理が正常完了"
        )
        
        # ログを取得
        logs = logger.get_logs()
        
        return df_output, df_filtered, logs, output_filename
        
    except Exception as e:
        logger.error(f"処理エラー: {str(e)}")
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
        df_output, df_filtered, logs, output_filename = process_mirail_data(file_content)
        
        return df_output, df_filtered, logs, output_filename
        
    except Exception as e:
        # エラー時は空のデータを返す
        logs = [f"エラー: {str(e)}"]
        empty_df = pd.DataFrame()
        return empty_df, empty_df, logs, "error.csv"