"""
詳細ログ生成共通クラス

AutocallとSMSの両方で使用する詳細ログ生成機能を提供。
統一されたログフォーマットにより、保守性と可読性を向上。

使用例:
    from processors.common.detailed_logger import DetailedLogger
    
    # フィルタ結果のログ
    log = DetailedLogger.log_filter_result(before_count, after_count, '委託先法人ID')
    
    # 除外詳細のログ
    detail = DetailedLogger.log_exclusion_details(
        excluded_data, ContractListColumns.TRUSTEE_ID, '委託先法人ID', 'id'
    )
"""

import pandas as pd
from typing import Optional, Dict, Any, Union


class DetailedLogger:
    """フィルター処理の詳細ログを生成する共通クラス"""
    
    @staticmethod
    def log_filter_result(before_count: int, after_count: int, label: str) -> str:
        """
        フィルタ適用結果の基本ログを生成
        
        Args:
            before_count: フィルタ適用前の件数
            after_count: フィルタ適用後の件数
            label: フィルタの名称
            
        Returns:
            str: フォーマットされたログメッセージ
            
        Example:
            "委託先法人IDフィルタ後: 1234件 (除外: 567件)"
        """
        excluded = before_count - after_count
        return f"{label}フィルタ後: {after_count}件 (除外: {excluded}件)"
    
    @staticmethod
    def log_exclusion_details(
        excluded_data: pd.DataFrame, 
        column_index: int, 
        label: str, 
        log_type: str = 'category',
        top_n: int = 3
    ) -> Optional[str]:
        """
        除外データの詳細ログを生成
        
        Args:
            excluded_data: 除外されたデータのDataFrame
            column_index: 対象列の番号（0ベース）
            label: ログに表示するラベル
            log_type: ログタイプ（'id', 'phone', 'date', 'amount', 'category'）
            top_n: 表示する上位件数（dateタイプで使用）
            
        Returns:
            str: フォーマットされた詳細ログ、またはNone（除外データがない場合）
        """
        if len(excluded_data) == 0:
            return None
        
        # 列データを取得
        column_data = excluded_data.iloc[:, column_index]
        
        if log_type == 'id':
            # ID系の集計（整数は整数として、それ以外は文字列として表示）
            counts = column_data.value_counts().to_dict()
            counts_str = {}
            for k, v in counts.items():
                if pd.notna(k) and isinstance(k, (int, float)) and k == int(k):
                    # 整数として扱える場合
                    counts_str[str(int(k))] = v
                else:
                    # その他の場合
                    counts_str[str(k)] = v
            return f"{label}除外詳細: {counts_str}"
            
        elif log_type == 'phone':
            # 電話番号の分類（空白/NaN vs その他）
            tel_data = column_data.astype(str).str.strip()
            empty_count = tel_data[tel_data.isin(['', 'nan', 'NaN'])].count()
            other_count = len(excluded_data) - empty_count
            return f"{label}除外詳細: {{空白/NaN: {empty_count}件, 固定電話等: {other_count}件}}"
            
        elif log_type == 'date':
            # 日付の上位N件表示
            try:
                dates = pd.to_datetime(column_data, errors='coerce')
                dates_str = dates.dt.strftime('%Y/%m/%d')
                top_dates = dates_str.value_counts().head(top_n).to_dict()
                log = f"{label}除外詳細（上位{top_n}件）: {top_dates}"
                if len(dates_str) > top_n:
                    log += f"\n  ※他{len(dates_str) - top_n}件の日付も除外"
                return log
            except:
                # 日付変換に失敗した場合は通常のカテゴリとして扱う
                return DetailedLogger.log_exclusion_details(
                    excluded_data, column_index, label, 'category', top_n
                )
                
        elif log_type == 'amount':
            # 金額の詳細（円付き）
            try:
                amounts = pd.to_numeric(column_data, errors='coerce')
                counts = amounts.value_counts().to_dict()
                amounts_str = {f"{int(k)}円": v for k, v in counts.items() if pd.notna(k)}
                return f"{label}除外詳細: {amounts_str}"
            except:
                # 数値変換に失敗した場合は通常のカテゴリとして扱う
                return DetailedLogger.log_exclusion_details(
                    excluded_data, column_index, label, 'category', top_n
                )
                
        else:  # category (default)
            # カテゴリ別の件数
            counts = column_data.value_counts().to_dict()
            return f"{label}除外詳細: {counts}"
    
    @staticmethod
    def log_initial_load(count: int) -> str:
        """
        初期データ読み込みのログ
        
        Args:
            count: 読み込んだ件数
            
        Returns:
            str: フォーマットされたログメッセージ
        """
        return f"元データ読み込み: {count}件"
    
    @staticmethod
    def log_final_result(count: int) -> str:
        """
        最終処理結果のログ
        
        Args:
            count: 最終的な件数
            
        Returns:
            str: フォーマットされたログメッセージ
        """
        return f"最終処理結果: {count}件"