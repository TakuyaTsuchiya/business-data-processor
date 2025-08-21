"""
プラザシステム用Mapperクラス

プラザオートコール処理で使用される各種データマッピングを実装します。
契約者（メイン）、保証人、緊急連絡人用のMapperクラスを提供します。
"""

from typing import Dict, Any, Union
import pandas as pd
from .base import BaseMapper
from .mapping_rules.plaza_rules import (
    MAIN_MAPPING_RULES,
    MAIN_DEFAULT_VALUES,
    GUARANTOR_MAPPING_RULES,
    GUARANTOR_DEFAULT_VALUES,
    CONTACT_MAPPING_RULES,
    CONTACT_DEFAULT_VALUES,
    format_debt_with_comma,
    validate_plaza_phone
)


class PlazaMainMapper(BaseMapper):
    """
    プラザ契約者（メイン）用データマッパー
    
    ContractListのデータを28列のオートコールテンプレート形式に変換します。
    プラザは残債表示ありで、委託先法人ID=6固定です。
    """
    
    def get_mapping_rules(self) -> Dict[str, Union[str, callable]]:
        """契約者用マッピングルールを取得"""
        return MAIN_MAPPING_RULES.copy()
    
    def get_default_values(self) -> Dict[str, Any]:
        """契約者用デフォルト値を取得"""
        return MAIN_DEFAULT_VALUES.copy()
    
    def apply_custom_rules(self, output: Dict[str, Any], input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        プラザ契約者固有のカスタムルールを適用
        """
        # 督促状況の固定値設定
        output["督促状況"] = "TEL"
        
        # 電話番号が同じ値を架電番号にもセット
        if "電話番号" in output and output["電話番号"]:
            output["架電番号"] = output["電話番号"]
        
        # 残債のフォーマット（カンマ区切り）
        if "残債" in output and output["残債"]:
            try:
                amount = float(output["残債"])
                output["残債"] = f"{amount:,.0f}"
            except (ValueError, TypeError):
                pass
        
        return output


class PlazaGuarantorMapper(BaseMapper):
    """
    プラザ保証人用データマッパー
    
    保証人データを28列のオートコールテンプレート形式に変換します。
    TEL携帯.1を使用します。
    """
    
    def get_mapping_rules(self) -> Dict[str, Union[str, callable]]:
        """保証人用マッピングルールを取得"""
        return GUARANTOR_MAPPING_RULES.copy()
    
    def get_default_values(self) -> Dict[str, Any]:
        """保証人用デフォルト値を取得"""
        return GUARANTOR_DEFAULT_VALUES.copy()
    
    def apply_custom_rules(self, output: Dict[str, Any], input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        プラザ保証人固有のカスタムルールを適用
        """
        output["督促状況"] = "TEL"
        
        if "電話番号" in output and output["電話番号"]:
            output["架電番号"] = output["電話番号"]
        
        # 残債のフォーマット
        if "残債" in output and output["残債"]:
            try:
                amount = float(output["残債"])
                output["残債"] = f"{amount:,.0f}"
            except (ValueError, TypeError):
                pass
        
        return output


class PlazaContactMapper(BaseMapper):
    """
    プラザ緊急連絡人（コンタクト）用データマッパー
    
    緊急連絡人データを28列のオートコールテンプレート形式に変換します。
    プラザ独自の列名「緊急連絡人１のTEL（携帯）」を使用します。
    """
    
    def get_mapping_rules(self) -> Dict[str, Union[str, callable]]:
        """緊急連絡人用マッピングルールを取得"""
        return CONTACT_MAPPING_RULES.copy()
    
    def get_default_values(self) -> Dict[str, Any]:
        """緊急連絡人用デフォルト値を取得"""
        return CONTACT_DEFAULT_VALUES.copy()
    
    def apply_custom_rules(self, output: Dict[str, Any], input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        プラザ緊急連絡人固有のカスタムルールを適用
        """
        output["督促状況"] = "TEL"
        
        if "電話番号" in output and output["電話番号"]:
            output["架電番号"] = output["電話番号"]
        
        # 残債のフォーマット
        if "残債" in output and output["残債"]:
            try:
                amount = float(output["残債"])
                output["残債"] = f"{amount:,.0f}"
            except (ValueError, TypeError):
                pass
        
        return output
    
    def apply_custom_rules_batch(self, output_df: pd.DataFrame, input_df: pd.DataFrame) -> pd.DataFrame:
        """
        バッチ処理用のカスタムルール適用
        """
        # 督促状況の一括設定
        output_df["督促状況"] = "TEL"
        
        # 架電番号を電話番号と同じ値に設定
        if "電話番号" in output_df.columns:
            output_df["架電番号"] = output_df["電話番号"]
        
        # 残債のフォーマット（カンマ区切り）
        if "残債" in output_df.columns:
            def format_amount(val):
                if pd.isna(val) or val == "":
                    return ""
                try:
                    amount = float(val)
                    return f"{amount:,.0f}"
                except (ValueError, TypeError):
                    return str(val)
            
            output_df["残債"] = output_df["残債"].apply(format_amount)
        
        return output_df