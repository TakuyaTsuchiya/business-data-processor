"""
フィルタリングの基底クラス - Domain Layer

このモジュールは、全プロセッサーで重複していたフィルタリングロジックを抽象化し、
設定駆動型のフィルタリングシステムを提供します。

共通化対象：
- 委託先法人IDフィルタ（100%重複度）
- 入金予定日フィルタ（95%重複度）
- 回収ランクフィルタ（100%重複度）
- 電話番号フィルタ（90%重複度）
- 入金予定金額フィルタ（85%重複度）
"""

import pandas as pd
import re
from typing import List, Dict, Any, Optional, Union
from abc import ABC, abstractmethod
from datetime import datetime
from dataclasses import dataclass


@dataclass
class FilterConfig:
    """フィルタ設定のデータクラス"""
    
    # 委託先法人ID設定
    trustee_company_type: str  # "numeric", "string", "mixed"
    trustee_company_values: List[Union[int, str, None]]
    
    # 入金予定日設定
    payment_date_mode: str  # "before_today", "today_and_before"
    
    # 回収ランク除外設定
    collection_rank_excludes: List[str]
    
    # 電話番号列名
    phone_column: str
    
    # 入金予定金額除外設定
    payment_amount_excludes: List[Union[int, float]]
    
    # その他設定
    extra_filters: Dict[str, Any] = None


class BaseFilter(ABC):
    """
    全システム共通のフィルタ基盤クラス
    
    このクラスは、各プロセッサーで共通的に使用されているフィルタリングロジックを
    提供し、設定駆動での柔軟なフィルタリングを実現します。
    """
    
    def __init__(self, config: FilterConfig):
        """
        ベースフィルターを初期化
        
        Args:
            config (FilterConfig): フィルタリング設定
        """
        self.config = config
    
    @staticmethod
    def filter_trustee_company_id(
        df: pd.DataFrame, 
        filter_type: str, 
        values: List[Union[int, str, None]]
    ) -> pd.DataFrame:
        """
        委託先法人IDフィルタ（設定駆動）
        
        Args:
            df (pd.DataFrame): 対象DataFrame
            filter_type (str): フィルタタイプ ("numeric", "string", "mixed")
            values (List): 許可する値のリスト
            
        Returns:
            pd.DataFrame: フィルタリング後のDataFrame
            
        Examples:
            >>> # ミライル: 空白と5
            >>> df_filtered = BaseFilter.filter_trustee_company_id(df, "mixed", [None, "", "5"])
            >>> # フェイス: 1-4
            >>> df_filtered = BaseFilter.filter_trustee_company_id(df, "numeric", [1,2,3,4])
            >>> # プラザ: 6のみ
            >>> df_filtered = BaseFilter.filter_trustee_company_id(df, "string", ["6"])
        """
        if "委託先法人ID" not in df.columns:
            return df
        
        if filter_type == "numeric":
            # フェイスパターン: 数値として処理
            df_work = df.copy()
            df_work["委託先法人ID"] = pd.to_numeric(df_work["委託先法人ID"], errors="coerce")
            return df_work[df_work["委託先法人ID"].isin(values)]
            
        elif filter_type == "string":
            # プラザパターン: 文字列として処理
            df_work = df.copy()
            return df_work[df_work["委託先法人ID"].astype(str).str.strip().isin([str(v) for v in values])]
            
        elif filter_type == "mixed":
            # ミライルパターン: 空白とNaNと特定値
            condition = df["委託先法人ID"].isna()
            
            for value in values:
                if value is None or value == "":
                    # 空白やNaNは既にconditionで考慮済み
                    condition |= (df["委託先法人ID"].astype(str).str.strip() == "")
                else:
                    condition |= (df["委託先法人ID"].astype(str).str.strip() == str(value))
            
            return df[condition]
        
        else:
            raise ValueError(f"未対応のフィルタタイプ: {filter_type}")
    
    @staticmethod
    def filter_payment_date(df: pd.DataFrame, mode: str) -> pd.DataFrame:
        """
        入金予定日フィルタ（モード設定）
        
        Args:
            df (pd.DataFrame): 対象DataFrame
            mode (str): フィルタモード ("before_today", "today_and_before")
            
        Returns:
            pd.DataFrame: フィルタリング後のDataFrame
            
        Examples:
            >>> # ミライル・フェイス: 前日以前
            >>> df_filtered = BaseFilter.filter_payment_date(df, "before_today")
            >>> # プラザメイン: 当日以前（当日含む）
            >>> df_filtered = BaseFilter.filter_payment_date(df, "today_and_before")
        """
        if "入金予定日" not in df.columns:
            return df
        
        df_work = df.copy()
        today = pd.Timestamp.now().normalize()
        df_work["入金予定日"] = pd.to_datetime(df_work["入金予定日"], errors='coerce')
        
        if mode == "before_today":
            # 前日以前（当日除外）+ NaN許可
            return df_work[df_work["入金予定日"].isna() | (df_work["入金予定日"] < today)]
        elif mode == "today_and_before":
            # 当日以前（当日含む）+ NaN許可
            return df_work[df_work["入金予定日"].isna() | (df_work["入金予定日"] <= today)]
        else:
            raise ValueError(f"未対応の入金予定日モード: {mode}")
    
    @staticmethod
    def filter_collection_rank(df: pd.DataFrame, exclude_list: List[str]) -> pd.DataFrame:
        """
        回収ランクフィルタ（除外リスト設定）
        
        Args:
            df (pd.DataFrame): 対象DataFrame
            exclude_list (List[str]): 除外する回収ランクのリスト
            
        Returns:
            pd.DataFrame: フィルタリング後のDataFrame
            
        Examples:
            >>> # ミライル: 弁護士介入のみ除外
            >>> df_filtered = BaseFilter.filter_collection_rank(df, ["弁護士介入"])
            >>> # フェイス: 複数除外
            >>> df_filtered = BaseFilter.filter_collection_rank(df, ["死亡決定", "破産決定", "弁護士介入"])
            >>> # プラザ: 督促停止と弁護士介入除外
            >>> df_filtered = BaseFilter.filter_collection_rank(df, ["督促停止", "弁護士介入"])
        """
        if "回収ランク" not in df.columns or not exclude_list:
            return df
        
        return df[~df["回収ランク"].isin(exclude_list)]
    
    @staticmethod
    def filter_phone_required(df: pd.DataFrame, column: str) -> pd.DataFrame:
        """
        電話番号必須フィルタ
        
        Args:
            df (pd.DataFrame): 対象DataFrame
            column (str): 電話番号カラム名
            
        Returns:
            pd.DataFrame: フィルタリング後のDataFrame
            
        Examples:
            >>> # 契約者電話番号フィルタ
            >>> df_filtered = BaseFilter.filter_phone_required(df, "TEL携帯")
            >>> # 保証人電話番号フィルタ
            >>> df_filtered = BaseFilter.filter_phone_required(df, "TEL携帯.1")
        """
        if column not in df.columns:
            return df
        
        return df[
            df[column].notna() &
            (~df[column].astype(str).str.strip().isin(["", "nan", "NaN"]))
        ]
    
    @staticmethod
    def filter_payment_amount(df: pd.DataFrame, exclude_list: List[Union[int, float]]) -> pd.DataFrame:
        """
        入金予定金額フィルタ（除外リスト設定）
        
        Args:
            df (pd.DataFrame): 対象DataFrame
            exclude_list (List[Union[int, float]]): 除外する金額のリスト
            
        Returns:
            pd.DataFrame: フィルタリング後のDataFrame
            
        Examples:
            >>> # ミライル・プラザ: 手数料4種除外
            >>> df_filtered = BaseFilter.filter_payment_amount(df, [2, 3, 5, 12])
            >>> # フェイス: 手数料3種除外（12円除外しない）
            >>> df_filtered = BaseFilter.filter_payment_amount(df, [2, 3, 5])
        """
        if "入金予定金額" not in df.columns or not exclude_list:
            return df
        
        df_work = df.copy()
        df_work["入金予定金額"] = pd.to_numeric(df_work["入金予定金額"], errors='coerce')
        
        return df_work[
            df_work["入金予定金額"].isna() | 
            ~df_work["入金予定金額"].isin(exclude_list)
        ]
    
    def apply_common_filters(
        self, 
        df: pd.DataFrame,
        logger: Optional[Any] = None
    ) -> pd.DataFrame:
        """
        設定に基づいて共通フィルターを順次適用
        
        Args:
            df (pd.DataFrame): 対象DataFrame
            logger (Optional[Any]): ロガーインスタンス
            
        Returns:
            pd.DataFrame: フィルタリング後のDataFrame
        """
        df_work = df.copy()
        initial_count = len(df_work)
        
        # 1. 委託先法人IDフィルタ
        df_work = self.filter_trustee_company_id(
            df_work,
            self.config.trustee_company_type,
            self.config.trustee_company_values
        )
        if logger:
            logger.log_filter_result(
                "委託先法人ID", initial_count, len(df_work),
                f"タイプ: {self.config.trustee_company_type}, 値: {self.config.trustee_company_values}"
            )
        
        # 2. 入金予定日フィルタ
        df_work = self.filter_payment_date(df_work, self.config.payment_date_mode)
        if logger:
            logger.log_filter_result(
                "入金予定日", len(df_work), len(df_work),
                f"モード: {self.config.payment_date_mode}"
            )
        
        # 3. 回収ランクフィルタ
        df_work = self.filter_collection_rank(df_work, self.config.collection_rank_excludes)
        if logger:
            logger.log_filter_result(
                "回収ランク", len(df_work), len(df_work),
                f"除外: {self.config.collection_rank_excludes}"
            )
        
        # 4. 電話番号必須フィルタ
        df_work = self.filter_phone_required(df_work, self.config.phone_column)
        if logger:
            logger.log_filter_result(
                "電話番号必須", len(df_work), len(df_work),
                f"列: {self.config.phone_column}"
            )
        
        # 5. 入金予定金額フィルタ
        df_work = self.filter_payment_amount(df_work, self.config.payment_amount_excludes)
        if logger:
            logger.log_filter_result(
                "入金予定金額", len(df_work), len(df_work),
                f"除外: {self.config.payment_amount_excludes}"
            )
        
        return df_work
    
    @abstractmethod
    def apply_system_specific_filters(
        self, 
        df: pd.DataFrame,
        logger: Optional[Any] = None
    ) -> pd.DataFrame:
        """
        システム固有フィルターの適用（サブクラスで実装）
        
        Args:
            df (pd.DataFrame): 対象DataFrame
            logger (Optional[Any]): ロガーインスタンス
            
        Returns:
            pd.DataFrame: フィルタリング後のDataFrame
        """
        pass
    
    def apply_all_filters(
        self, 
        df: pd.DataFrame,
        logger: Optional[Any] = None
    ) -> pd.DataFrame:
        """
        全フィルター（共通 + システム固有）を適用
        
        Args:
            df (pd.DataFrame): 対象DataFrame
            logger (Optional[Any]): ロガーインスタンス
            
        Returns:
            pd.DataFrame: フィルタリング後のDataFrame
        """
        # 1. 共通フィルター適用
        df_work = self.apply_common_filters(df, logger)
        
        # 2. システム固有フィルター適用
        df_work = self.apply_system_specific_filters(df_work, logger)
        
        return df_work