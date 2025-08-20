"""
フィルター設定ファクトリー - Domain Layer

このモジュールは、各システムと処理タイプ用のフィルター設定を簡単に作成するための
ファクトリーメソッドを提供します。

設定の標準化により、新しいプロセッサー追加時の作業効率を向上させます。
"""

from typing import Dict, Any, Optional
from .base_filter import FilterConfig
from .system_filters import (
    MirailFilter, MirailFilterConfig,
    FaithFilter, FaithSmsFilter, FaithSmsFilterConfig,
    PlazaFilter
)


class FilterConfigFactory:
    """
    フィルター設定を自動生成するファクトリークラス
    
    各システムと処理タイプの組み合わせに応じて、適切なフィルター設定を生成します。
    """
    
    # システム別共通設定
    SYSTEM_CONFIGS = {
        "mirail": {
            "trustee_company_type": "mixed",
            "trustee_company_values": [None, "", "5"],
            "payment_date_mode": "before_today",
            "collection_rank_excludes": ["弁護士介入"],
            "payment_amount_excludes": [2, 3, 5, 12]
        },
        "faith": {
            "trustee_company_type": "numeric", 
            "trustee_company_values": [1, 2, 3, 4],
            "payment_date_mode": "before_today",
            "collection_rank_excludes": ["死亡決定", "破産決定", "弁護士介入"],
            "payment_amount_excludes": [2, 3, 5]  # 12円を除外しない
        },
        "plaza": {
            "trustee_company_type": "string",
            "trustee_company_values": ["6"],
            "payment_date_mode": "before_today",  # 契約者以外
            "collection_rank_excludes": ["督促停止", "弁護士介入"],
            "payment_amount_excludes": [2, 3, 5, 12]
        }
    }
    
    # 処理タイプ別電話番号列設定
    PHONE_COLUMN_MAPPING = {
        "contract": "TEL携帯",
        "guarantor": "TEL携帯.1", 
        "emergency_contact": "緊急連絡人１のTEL（携帯）"
    }
    
    @classmethod
    def create_mirail_config(
        cls,
        process_type: str,
        debt_filter_enabled: bool = False,
        **kwargs
    ) -> MirailFilterConfig:
        """
        ミライル用フィルター設定を作成
        
        Args:
            process_type (str): 処理タイプ ("contract", "guarantor", "emergency_contact")
            debt_filter_enabled (bool): 残債除外フィルター有効フラグ（without10k版=True）
            **kwargs: 追加設定の上書き
            
        Returns:
            MirailFilterConfig: ミライル固有設定
            
        Examples:
            >>> # ミライル契約者 without10k版
            >>> config = FilterConfigFactory.create_mirail_config(
            ...     "contract", debt_filter_enabled=True)
            >>> # ミライル保証人 with10k版  
            >>> config = FilterConfigFactory.create_mirail_config(
            ...     "guarantor", debt_filter_enabled=False)
        """
        base_config = cls.SYSTEM_CONFIGS["mirail"].copy()
        base_config.update(kwargs)
        
        return MirailFilterConfig(
            trustee_company_type=base_config["trustee_company_type"],
            trustee_company_values=base_config["trustee_company_values"],
            payment_date_mode=base_config["payment_date_mode"],
            collection_rank_excludes=base_config["collection_rank_excludes"],
            phone_column=cls.PHONE_COLUMN_MAPPING[process_type],
            payment_amount_excludes=base_config["payment_amount_excludes"],
            # ミライル固有設定
            debt_filter_enabled=debt_filter_enabled,
            client_cd_values=[1, 4] if debt_filter_enabled else None,
            exclude_debt_amounts=[10000, 11000] if debt_filter_enabled else None
        )
    
    @classmethod
    def create_faith_config(
        cls,
        process_type: str,
        **kwargs
    ) -> FilterConfig:
        """
        フェイス用フィルター設定を作成
        
        Args:
            process_type (str): 処理タイプ ("contract", "guarantor", "emergency_contact")
            **kwargs: 追加設定の上書き
            
        Returns:
            FilterConfig: フェイス設定
            
        Examples:
            >>> # フェイス契約者
            >>> config = FilterConfigFactory.create_faith_config("contract")
            >>> # フェイス保証人
            >>> config = FilterConfigFactory.create_faith_config("guarantor")
        """
        base_config = cls.SYSTEM_CONFIGS["faith"].copy()
        base_config.update(kwargs)
        
        return FilterConfig(
            trustee_company_type=base_config["trustee_company_type"],
            trustee_company_values=base_config["trustee_company_values"],
            payment_date_mode=base_config["payment_date_mode"],
            collection_rank_excludes=base_config["collection_rank_excludes"],
            phone_column=cls.PHONE_COLUMN_MAPPING[process_type],
            payment_amount_excludes=base_config["payment_amount_excludes"]
        )
    
    @classmethod
    def create_faith_sms_config(
        cls,
        process_type: str = "contract",
        residence_status: str = "退去済み",
        strict_validation: bool = True,
        **kwargs
    ) -> FaithSmsFilterConfig:
        """
        フェイスSMS用フィルター設定を作成
        
        Args:
            process_type (str): 処理タイプ（通常は"contract"）
            residence_status (str): 対象入居ステータス
            strict_validation (bool): 厳密な携帯番号チェック有効フラグ
            **kwargs: 追加設定の上書き
            
        Returns:
            FaithSmsFilterConfig: フェイスSMS固有設定
            
        Examples:
            >>> # フェイスSMS 退去済み契約者
            >>> config = FilterConfigFactory.create_faith_sms_config()
        """
        base_config = cls.SYSTEM_CONFIGS["faith"].copy()
        base_config.update(kwargs)
        
        return FaithSmsFilterConfig(
            trustee_company_type=base_config["trustee_company_type"],
            trustee_company_values=base_config["trustee_company_values"],
            payment_date_mode=base_config["payment_date_mode"],
            collection_rank_excludes=base_config["collection_rank_excludes"],
            phone_column=cls.PHONE_COLUMN_MAPPING[process_type],
            payment_amount_excludes=base_config["payment_amount_excludes"],
            # フェイスSMS固有設定
            residence_status_filter=residence_status,
            strict_mobile_validation=strict_validation
        )
    
    @classmethod
    def create_plaza_config(
        cls,
        process_type: str,
        **kwargs
    ) -> FilterConfig:
        """
        プラザ用フィルター設定を作成
        
        Args:
            process_type (str): 処理タイプ ("main", "guarantor", "contact")
            **kwargs: 追加設定の上書き
            
        Returns:
            FilterConfig: プラザ設定
            
        Examples:
            >>> # プラザ契約者（当日含む入金予定日フィルター）
            >>> config = FilterConfigFactory.create_plaza_config("main", payment_date_mode="today_and_before")
            >>> # プラザ緊急連絡人（前日以前）
            >>> config = FilterConfigFactory.create_plaza_config("contact")
        """
        base_config = cls.SYSTEM_CONFIGS["plaza"].copy()
        
        # プラザメインのみ当日含む入金予定日フィルター
        if process_type == "main":
            base_config["payment_date_mode"] = "today_and_before"
        
        # 電話番号列のマッピング（プラザ独自）
        phone_mapping = {
            "main": "TEL携帯",
            "guarantor": "TEL携帯.1", 
            "contact": "緊急連絡人１のTEL（携帯）"
        }
        
        base_config.update(kwargs)
        
        return FilterConfig(
            trustee_company_type=base_config["trustee_company_type"],
            trustee_company_values=base_config["trustee_company_values"],
            payment_date_mode=base_config["payment_date_mode"],
            collection_rank_excludes=base_config["collection_rank_excludes"],
            phone_column=phone_mapping.get(process_type, "TEL携帯"),
            payment_amount_excludes=base_config["payment_amount_excludes"]
        )


class FilterFactory:
    """
    フィルターインスタンス生成ファクトリークラス
    
    設定とフィルタークラスを組み合わせて、実際のフィルターインスタンスを生成します。
    """
    
    @classmethod
    def create_mirail_filter(
        cls,
        process_type: str,
        debt_filter_enabled: bool = False,
        **kwargs
    ):
        """
        ミライル用フィルターインスタンスを作成
        
        Args:
            process_type (str): 処理タイプ
            debt_filter_enabled (bool): 残債除外フィルター有効フラグ
            **kwargs: 追加設定
            
        Returns:
            MirailFilter: ミライル用フィルターインスタンス
            
        Examples:
            >>> # ミライル契約者 without10k版
            >>> filter_instance = FilterFactory.create_mirail_filter(
            ...     "contract", debt_filter_enabled=True)
            >>> filtered_df = filter_instance.apply_all_filters(df, logger)
        """
        config = FilterConfigFactory.create_mirail_config(
            process_type, debt_filter_enabled, **kwargs
        )
        return MirailFilter(config)
    
    @classmethod
    def create_faith_filter(
        cls,
        process_type: str,
        **kwargs
    ):
        """
        フェイス用フィルターインスタンスを作成
        
        Args:
            process_type (str): 処理タイプ
            **kwargs: 追加設定
            
        Returns:
            FaithFilter: フェイス用フィルターインスタンス
        """
        config = FilterConfigFactory.create_faith_config(process_type, **kwargs)
        return FaithFilter(config)
    
    @classmethod
    def create_faith_sms_filter(
        cls,
        process_type: str = "contract",
        **kwargs
    ):
        """
        フェイスSMS用フィルターインスタンスを作成
        
        Args:
            process_type (str): 処理タイプ
            **kwargs: 追加設定
            
        Returns:
            FaithSmsFilter: フェイスSMS用フィルターインスタンス
        """
        config = FilterConfigFactory.create_faith_sms_config(process_type, **kwargs)
        return FaithSmsFilter(config)
    
    @classmethod 
    def create_plaza_filter(
        cls,
        process_type: str,
        **kwargs
    ):
        """
        プラザ用フィルターインスタンスを作成
        
        Args:
            process_type (str): 処理タイプ
            **kwargs: 追加設定
            
        Returns:
            PlazaFilter: プラザ用フィルターインスタンス
        """
        config = FilterConfigFactory.create_plaza_config(process_type, **kwargs)
        return PlazaFilter(config)