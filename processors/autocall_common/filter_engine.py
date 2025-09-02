"""
共通フィルタリングエンジン

すべてのautocallプロセッサーで使用する統一的なフィルタリング処理を提供。
DetailedLoggerと統合し、フィルタリングロジックを一箇所に集約。

使用例:
    from processors.autocall_common.filter_engine import apply_filters
    
    filter_config = {
        "trustee_id": {
            "column": COL.TRUSTEE_ID,
            "values": ["", "5"],
            "log_type": "id",
            "label": "委託先法人ID"
        }
    }
    
    df_filtered, logs = apply_filters(df_input, filter_config)
"""

import pandas as pd
from typing import Dict, List, Tuple, Any, Optional
from datetime import datetime
import sys
import os

# プロジェクトルートをパスに追加
processors_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if processors_dir not in sys.path:
    sys.path.append(processors_dir)

from common.detailed_logger import DetailedLogger
from common.contract_list_columns import ContractListColumns as COL


class FilterEngine:
    """共通フィルタリングエンジン"""
    
    @staticmethod
    def apply_filters(df: pd.DataFrame, filter_config: Dict[str, Dict[str, Any]]) -> Tuple[pd.DataFrame, List[str]]:
        """
        設定に基づいてフィルタリングを実行
        
        Args:
            df: 入力DataFrame
            filter_config: フィルタ設定の辞書
            
        Returns:
            tuple: (フィルタリング済みDataFrame, ログリスト)
        """
        df = df.copy()
        logs = []
        
        # 初期件数を記録
        initial_count = len(df)
        logs.append(DetailedLogger.log_initial_load(initial_count))
        
        # 各フィルタを順番に適用
        for filter_name, config in filter_config.items():
            before_count = len(df)
            
            if filter_name == "trustee_id":
                df, filter_logs = FilterEngine._filter_trustee_id(df, config)
            elif filter_name == "payment_date":
                df, filter_logs = FilterEngine._filter_payment_date(df, config)
            elif filter_name == "collection_rank":
                df, filter_logs = FilterEngine._filter_collection_rank(df, config)
            elif filter_name == "special_debt":
                df, filter_logs = FilterEngine._filter_special_debt(df, config)
            elif filter_name == "mobile_phone":
                df, filter_logs = FilterEngine._filter_mobile_phone(df, config)
            elif filter_name == "payment_amount":
                df, filter_logs = FilterEngine._filter_payment_amount(df, config)
            else:
                continue
            
            logs.extend(filter_logs)
        
        # 最終結果を記録
        logs.append(DetailedLogger.log_final_result(len(df)))
        
        return df, logs
    
    @staticmethod
    def _filter_trustee_id(df: pd.DataFrame, config: Dict[str, Any]) -> Tuple[pd.DataFrame, List[str]]:
        """委託先法人IDフィルタ"""
        logs = []
        column_idx = config["column"]
        allowed_values = config.get("values", ["", "5"])
        
        # 除外されるデータを記録
        mask = df.iloc[:, column_idx].isna()
        for value in allowed_values:
            mask |= (df.iloc[:, column_idx].astype(str).str.strip() == value)
        
        excluded_data = df[~mask]
        if len(excluded_data) > 0:
            detail_log = DetailedLogger.log_exclusion_details(
                excluded_data, column_idx, config.get("label", "委託先法人ID"), 
                config.get("log_type", "id")
            )
            if detail_log:
                logs.append(detail_log)
        
        # フィルタ適用
        df_filtered = df[mask]
        logs.append(DetailedLogger.log_filter_result(
            len(df), len(df_filtered), 
            config.get("label", "委託先法人ID") + f"（{','.join(allowed_values)}）"
        ))
        
        return df_filtered, logs
    
    @staticmethod
    def _filter_payment_date(df: pd.DataFrame, config: Dict[str, Any]) -> Tuple[pd.DataFrame, List[str]]:
        """入金予定日フィルタ"""
        logs = []
        column_idx = config["column"]
        
        # 日付列を変換
        df.iloc[:, column_idx] = pd.to_datetime(df.iloc[:, column_idx], errors='coerce')
        
        # 基準日（デフォルトは今日）
        if config.get("type") == "before_today":
            reference_date = pd.Timestamp.now().normalize()
        else:
            reference_date = pd.Timestamp(config.get("reference_date", datetime.now())).normalize()
        
        # 除外されるデータを記録
        mask = df.iloc[:, column_idx].isna() | (df.iloc[:, column_idx] < reference_date)
        excluded_data = df[~mask]
        
        if len(excluded_data) > 0:
            detail_log = DetailedLogger.log_exclusion_details(
                excluded_data, column_idx, config.get("label", "入金予定日"), 
                "date", top_n=config.get("top_n", 3)
            )
            if detail_log:
                logs.append(detail_log)
        
        # フィルタ適用
        df_filtered = df[mask]
        logs.append(DetailedLogger.log_filter_result(
            len(df), len(df_filtered), config.get("label", "入金予定日")
        ))
        
        return df_filtered, logs
    
    @staticmethod
    def _filter_collection_rank(df: pd.DataFrame, config: Dict[str, Any]) -> Tuple[pd.DataFrame, List[str]]:
        """回収ランクフィルタ"""
        logs = []
        column_idx = config["column"]
        exclude_values = config.get("exclude", ["弁護士介入"])
        
        # 除外されるデータを記録
        mask = ~df.iloc[:, column_idx].isin(exclude_values)
        excluded_data = df[~mask]
        
        if len(excluded_data) > 0:
            detail_log = DetailedLogger.log_exclusion_details(
                excluded_data, column_idx, config.get("label", "回収ランク"), "category"
            )
            if detail_log:
                logs.append(detail_log)
        
        # フィルタ適用
        df_filtered = df[mask]
        logs.append(DetailedLogger.log_filter_result(
            len(df), len(df_filtered), config.get("label", "回収ランク")
        ))
        
        return df_filtered, logs
    
    @staticmethod
    def _filter_special_debt(df: pd.DataFrame, config: Dict[str, Any]) -> Tuple[pd.DataFrame, List[str]]:
        """特殊残債フィルタ（ミライル用）"""
        logs = []
        client_cd_idx = config["client_cd_column"]
        debt_idx = config["debt_column"]
        conditions = config.get("conditions", {})
        
        # 数値に変換
        df.iloc[:, client_cd_idx] = pd.to_numeric(df.iloc[:, client_cd_idx], errors="coerce")
        df.iloc[:, debt_idx] = pd.to_numeric(
            df.iloc[:, debt_idx].astype(str).str.replace(',', ''), errors='coerce'
        )
        
        # 除外条件
        client_cds = conditions.get("client_cds", [1, 4])
        debt_amounts = conditions.get("debt_amounts", [10000, 11000])
        
        exclude_condition = (
            df.iloc[:, client_cd_idx].isin(client_cds) & 
            df.iloc[:, debt_idx].isin(debt_amounts)
        )
        
        # 除外されるデータを記録
        excluded_data = df[exclude_condition]
        if len(excluded_data) > 0:
            special_debt_data = excluded_data.iloc[:, [client_cd_idx, debt_idx]].copy()
            special_debt_data.columns = ['クライアントCD', '滞納残債']
            special_debt_counts = special_debt_data.groupby(['クライアントCD', '滞納残債']).size().to_dict()
            special_debt_str = {f"CD={int(k[0])}, {int(k[1])}円": v for k, v in special_debt_counts.items()}
            logs.append(f"{config.get('label', 'ミライル特殊残債')}除外詳細: {special_debt_str}")
        
        # フィルタ適用
        df_filtered = df[~exclude_condition]
        logs.append(DetailedLogger.log_filter_result(
            len(df), len(df_filtered), config.get("label", "特殊残債")
        ))
        
        return df_filtered, logs
    
    @staticmethod
    def _filter_mobile_phone(df: pd.DataFrame, config: Dict[str, Any]) -> Tuple[pd.DataFrame, List[str]]:
        """携帯電話番号フィルタ"""
        logs = []
        column_idx = config["column"]
        
        # 空でない値のみを残す
        mask = df.iloc[:, column_idx].notna() & \
               (~df.iloc[:, column_idx].astype(str).str.strip().isin(["", "nan", "NaN"]))
        
        excluded_data = df[~mask]
        if len(excluded_data) > 0:
            detail_log = DetailedLogger.log_exclusion_details(
                excluded_data, column_idx, config.get("label", "携帯電話"), "phone"
            )
            if detail_log:
                logs.append(detail_log)
        
        # フィルタ適用
        df_filtered = df[mask]
        logs.append(DetailedLogger.log_filter_result(
            len(df), len(df_filtered), config.get("label", "携帯電話")
        ))
        
        return df_filtered, logs
    
    @staticmethod
    def _filter_payment_amount(df: pd.DataFrame, config: Dict[str, Any]) -> Tuple[pd.DataFrame, List[str]]:
        """入金予定金額フィルタ"""
        logs = []
        column_idx = config["column"]
        exclude_amounts = config.get("exclude", [2, 3, 5, 12])
        
        # 数値に変換
        df.iloc[:, column_idx] = pd.to_numeric(df.iloc[:, column_idx], errors='coerce')
        
        # 除外されるデータを記録
        mask = df.iloc[:, column_idx].isna() | ~df.iloc[:, column_idx].isin(exclude_amounts)
        excluded_data = df[~mask]
        
        if len(excluded_data) > 0:
            detail_log = DetailedLogger.log_exclusion_details(
                excluded_data, column_idx, config.get("label", "除外金額"), "amount"
            )
            if detail_log:
                logs.append(detail_log)
        
        # フィルタ適用
        df_filtered = df[mask]
        logs.append(DetailedLogger.log_filter_result(
            len(df), len(df_filtered), config.get("label", "入金予定金額")
        ))
        
        return df_filtered, logs


# エクスポート用の便利関数
def apply_filters(df: pd.DataFrame, filter_config: Dict[str, Dict[str, Any]]) -> Tuple[pd.DataFrame, List[str]]:
    """フィルタリングを実行する便利関数"""
    return FilterEngine.apply_filters(df, filter_config)