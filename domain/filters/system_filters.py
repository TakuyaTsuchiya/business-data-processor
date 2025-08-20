"""
システム固有フィルタークラス - Domain Layer

このモジュールは、各システム（ミライル、フェイス、プラザ）固有のフィルタリングロジックを
BaseFilterを継承して実装します。

システム固有フィルター：
- ミライル: 残債除外フィルター（without10k版のみ）
- フェイスSMS: 携帯電話番号正規表現チェック + 入居ステータスフィルター
- プラザ: （現在は共通フィルターのみ）
"""

import pandas as pd
import re
from typing import List, Dict, Any, Optional, Union
from dataclasses import dataclass

from .base_filter import BaseFilter, FilterConfig


@dataclass
class MirailFilterConfig(FilterConfig):
    """ミライル固有フィルター設定"""
    
    # 残債除外設定
    debt_filter_enabled: bool = False
    client_cd_values: List[int] = None
    exclude_debt_amounts: List[Union[int, float]] = None


@dataclass
class FaithSmsFilterConfig(FilterConfig):
    """フェイスSMS固有フィルター設定"""
    
    # 入居ステータス設定
    residence_status_filter: str = "退去済み"
    
    # 厳密電話番号チェック
    strict_mobile_validation: bool = True


class MirailFilter(BaseFilter):
    """
    ミライル固有フィルタークラス
    
    ミライル特有の残債除外処理（without10k版）を実装します。
    """
    
    def __init__(self, config: MirailFilterConfig):
        """
        ミライルフィルターを初期化
        
        Args:
            config (MirailFilterConfig): ミライル固有設定を含むフィルター設定
        """
        super().__init__(config)
        self.mirail_config = config
    
    @staticmethod
    def filter_debt_exclusion(
        df: pd.DataFrame,
        enabled: bool,
        client_cd_values: List[int],
        exclude_amounts: List[Union[int, float]],
        logger: Optional[Any] = None
    ) -> pd.DataFrame:
        """
        残債除外フィルター（ミライル固有）
        
        クライアントCD=1,4 かつ 滞納残債=10,000円・11,000円のレコードのみ除外
        その他全てのレコードは処理対象（クライアントCD≠1,4 や、CD=1,4でも残債が10k/11k以外）
        
        Args:
            df (pd.DataFrame): 対象DataFrame
            enabled (bool): フィルター有効フラグ（without10k版=True, with10k版=False）
            client_cd_values (List[int]): 対象クライアントCDリスト（通常[1,4]）
            exclude_amounts (List): 除外する残債金額リスト（通常[10000,11000]）
            logger (Optional[Any]): ロガーインスタンス
            
        Returns:
            pd.DataFrame: フィルタリング後のDataFrame
            
        Examples:
            >>> # without10k版（残債除外あり）
            >>> df_filtered = MirailFilter.filter_debt_exclusion(
            ...     df, enabled=True, client_cd_values=[1,4], exclude_amounts=[10000,11000])
            >>> # with10k版（残債除外なし）
            >>> df_filtered = MirailFilter.filter_debt_exclusion(
            ...     df, enabled=False, client_cd_values=[], exclude_amounts=[])
        """
        if not enabled or not client_cd_values or not exclude_amounts:
            if logger:
                logger.info("残債除外フィルター: 無効またはパラメータ不足のためスキップ")
            return df
        
        if "クライアントCD" not in df.columns or "滞納残債" not in df.columns:
            if logger:
                logger.warning("残債除外フィルター: 必要な列が存在しないためスキップ")
            return df
        
        df_work = df.copy()
        initial_count = len(df_work)
        
        # データ型変換
        df_work["クライアントCD"] = pd.to_numeric(df_work["クライアントCD"], errors="coerce")
        df_work["滞納残債"] = pd.to_numeric(
            df_work["滞納残債"].astype(str).str.replace(',', ''), errors='coerce'
        )
        
        # 除外条件: クライアントCDが対象値 AND 滞納残債が除外対象金額
        exclude_condition = (
            df_work["クライアントCD"].isin(client_cd_values) &
            df_work["滞納残債"].isin(exclude_amounts)
        )
        
        df_result = df_work[~exclude_condition]
        excluded_count = initial_count - len(df_result)
        
        if logger:
            logger.log_filter_result(
                "残債除外", initial_count, len(df_result),
                f"CD={client_cd_values}, 除外残債={exclude_amounts}, 除外件数={excluded_count}"
            )
        
        return df_result
    
    def apply_system_specific_filters(
        self, 
        df: pd.DataFrame,
        logger: Optional[Any] = None
    ) -> pd.DataFrame:
        """
        ミライル固有フィルターを適用
        
        Args:
            df (pd.DataFrame): 共通フィルター適用済みDataFrame
            logger (Optional[Any]): ロガーインスタンス
            
        Returns:
            pd.DataFrame: ミライル固有フィルター適用後のDataFrame
        """
        if logger:
            logger.info("ミライル固有フィルター適用開始")
        
        # 残債除外フィルター適用
        df_result = self.filter_debt_exclusion(
            df,
            self.mirail_config.debt_filter_enabled,
            self.mirail_config.client_cd_values or [],
            self.mirail_config.exclude_debt_amounts or [],
            logger
        )
        
        if logger:
            logger.info("ミライル固有フィルター適用完了")
        
        return df_result


class FaithFilter(BaseFilter):
    """
    フェイス固有フィルタークラス
    
    現在は共通フィルターのみを使用しますが、将来的な拡張に備えています。
    """
    
    def apply_system_specific_filters(
        self, 
        df: pd.DataFrame,
        logger: Optional[Any] = None
    ) -> pd.DataFrame:
        """
        フェイス固有フィルターを適用（現在は追加フィルターなし）
        
        Args:
            df (pd.DataFrame): 共通フィルター適用済みDataFrame
            logger (Optional[Any]): ロガーインスタンス
            
        Returns:
            pd.DataFrame: そのまま返す
        """
        if logger:
            logger.info("フェイス固有フィルター: 追加フィルターなし")
        
        return df


class FaithSmsFilter(BaseFilter):
    """
    フェイスSMS固有フィルタークラス
    
    SMS送信特有の厳密な電話番号チェックと入居ステータスフィルターを実装します。
    """
    
    def __init__(self, config: FaithSmsFilterConfig):
        """
        フェイスSMSフィルターを初期化
        
        Args:
            config (FaithSmsFilterConfig): フェイスSMS固有設定を含むフィルター設定
        """
        super().__init__(config)
        self.sms_config = config
    
    @staticmethod
    def filter_residence_status(
        df: pd.DataFrame,
        target_status: str,
        logger: Optional[Any] = None
    ) -> pd.DataFrame:
        """
        入居ステータスフィルター（SMS固有）
        
        Args:
            df (pd.DataFrame): 対象DataFrame
            target_status (str): 対象ステータス（例: "退去済み"）
            logger (Optional[Any]): ロガーインスタンス
            
        Returns:
            pd.DataFrame: フィルタリング後のDataFrame
        """
        if "入居ステータス" not in df.columns:
            if logger:
                logger.warning("入居ステータスフィルター: 入居ステータス列が存在しません")
            return df
        
        initial_count = len(df)
        df_result = df[df["入居ステータス"] == target_status]
        
        if logger:
            logger.log_filter_result(
                "入居ステータス", initial_count, len(df_result),
                f"対象: {target_status}"
            )
        
        return df_result
    
    @staticmethod
    def filter_mobile_phone_regex(
        df: pd.DataFrame,
        column: str,
        logger: Optional[Any] = None
    ) -> pd.DataFrame:
        """
        携帯電話番号正規表現フィルター（SMS固有の厳密チェック）
        
        090/080/070-####-#### 形式の携帯電話番号のみを許可
        
        Args:
            df (pd.DataFrame): 対象DataFrame
            column (str): 電話番号カラム名
            logger (Optional[Any]): ロガーインスタンス
            
        Returns:
            pd.DataFrame: フィルタリング後のDataFrame
        """
        if column not in df.columns:
            if logger:
                logger.warning(f"携帯番号正規表現フィルター: {column}列が存在しません")
            return df
        
        mobile_regex = r'^(090|080|070)-\d{4}-\d{4}$'
        df_work = df.copy()
        initial_count = len(df_work)
        
        # 文字列変換とクリーニング
        df_work[column] = df_work[column].astype(str).str.strip().replace('nan', '')
        
        def is_mobile_phone(phone: str) -> bool:
            """携帯電話番号の正規表現チェック"""
            return bool(re.match(mobile_regex, phone)) if phone != '' else False
        
        df_result = df_work[df_work[column].apply(is_mobile_phone)]
        
        if logger:
            logger.log_filter_result(
                "携帯番号正規表現", initial_count, len(df_result),
                f"列: {column}, パターン: {mobile_regex}"
            )
        
        return df_result
    
    def apply_system_specific_filters(
        self, 
        df: pd.DataFrame,
        logger: Optional[Any] = None
    ) -> pd.DataFrame:
        """
        フェイスSMS固有フィルターを適用
        
        Args:
            df (pd.DataFrame): 共通フィルター適用済みDataFrame
            logger (Optional[Any]): ロガーインスタンス
            
        Returns:
            pd.DataFrame: フェイスSMS固有フィルター適用後のDataFrame
        """
        if logger:
            logger.info("フェイスSMS固有フィルター適用開始")
        
        df_work = df
        
        # 1. 入居ステータスフィルター
        if hasattr(self.sms_config, 'residence_status_filter') and self.sms_config.residence_status_filter:
            df_work = self.filter_residence_status(
                df_work,
                self.sms_config.residence_status_filter,
                logger
            )
        
        # 2. 厳密な携帯番号チェック
        if self.sms_config.strict_mobile_validation:
            df_work = self.filter_mobile_phone_regex(
                df_work,
                self.config.phone_column,
                logger
            )
        
        if logger:
            logger.info("フェイスSMS固有フィルター適用完了")
        
        return df_work


class PlazaFilter(BaseFilter):
    """
    プラザ固有フィルタークラス
    
    現在は共通フィルターのみを使用しますが、将来的な拡張に備えています。
    """
    
    def apply_system_specific_filters(
        self, 
        df: pd.DataFrame,
        logger: Optional[Any] = None
    ) -> pd.DataFrame:
        """
        プラザ固有フィルターを適用（現在は追加フィルターなし）
        
        Args:
            df (pd.DataFrame): 共通フィルター適用済みDataFrame
            logger (Optional[Any]): ロガーインスタンス
            
        Returns:
            pd.DataFrame: そのまま返す
        """
        if logger:
            logger.info("プラザ固有フィルター: 追加フィルターなし")
        
        return df