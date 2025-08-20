"""
ミライルシステム用Mapperクラス

ミライルオートコール処理で使用される各種データマッピングを実装します。
契約者、保証人、緊急連絡人それぞれのMapperクラスを提供します。
"""

from typing import Dict, Any, Union
import pandas as pd
from .base import BaseMapper
from .mapping_rules.mirail_rules import (
    CONTRACT_MAPPING_RULES,
    CONTRACT_DEFAULT_VALUES,
    GUARANTOR_MAPPING_RULES,
    GUARANTOR_DEFAULT_VALUES,
    EMERGENCY_MAPPING_RULES,
    EMERGENCY_DEFAULT_VALUES,
    format_phone_number,
    format_debt_amount
)


class MirailContractMapper(BaseMapper):
    """
    ミライル契約者用データマッパー
    
    ContractListのデータを28列のオートコールテンプレート形式に変換します。
    """
    
    def get_mapping_rules(self) -> Dict[str, Union[str, callable]]:
        """契約者用マッピングルールを取得"""
        # 基本ルールをコピー
        rules = CONTRACT_MAPPING_RULES.copy()
        
        # カスタム関数を追加（必要に応じて）
        # rules["電話番号"] = format_phone_number
        
        return rules
    
    def get_default_values(self) -> Dict[str, Any]:
        """契約者用デフォルト値を取得"""
        return CONTRACT_DEFAULT_VALUES.copy()
    
    def apply_custom_rules(self, output: Dict[str, Any], input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        ミライル契約者固有のカスタムルールを適用
        
        Args:
            output: マッピング後のデータ
            input_data: 元の入力データ
            
        Returns:
            カスタムルール適用後のデータ
        """
        # 督促状況の固定値設定
        output["督促状況"] = "TEL"
        
        # 電話番号のフォーマット（必要に応じて）
        if "電話番号" in output and output["電話番号"]:
            phone = str(output["電話番号"]).strip()
            # ハイフンがない場合は追加
            if "-" not in phone and len(phone) == 11:
                output["電話番号"] = f"{phone[:3]}-{phone[3:7]}-{phone[7:]}"
                output["架電番号"] = output["電話番号"]  # 同じ値を設定
        
        return output


class MirailGuarantorMapper(BaseMapper):
    """
    ミライル保証人用データマッパー
    
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
        ミライル保証人固有のカスタムルールを適用
        """
        # 督促状況の固定値設定
        output["督促状況"] = "TEL"
        
        # 電話番号のフォーマット
        if "電話番号" in output and output["電話番号"]:
            phone = str(output["電話番号"]).strip()
            if "-" not in phone and len(phone) == 11:
                output["電話番号"] = f"{phone[:3]}-{phone[3:7]}-{phone[7:]}"
                output["架電番号"] = output["電話番号"]
        
        return output


class MirailEmergencyContactMapper(BaseMapper):
    """
    ミライル緊急連絡人用データマッパー
    
    緊急連絡人データを28列のオートコールテンプレート形式に変換します。
    TEL携帯.2を使用します。
    """
    
    def get_mapping_rules(self) -> Dict[str, Union[str, callable]]:
        """緊急連絡人用マッピングルールを取得"""
        return EMERGENCY_MAPPING_RULES.copy()
    
    def get_default_values(self) -> Dict[str, Any]:
        """緊急連絡人用デフォルト値を取得"""
        return EMERGENCY_DEFAULT_VALUES.copy()
    
    def apply_custom_rules(self, output: Dict[str, Any], input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        ミライル緊急連絡人固有のカスタムルールを適用
        """
        # 督促状況の固定値設定
        output["督促状況"] = "TEL"
        
        # 電話番号のフォーマット
        if "電話番号" in output and output["電話番号"]:
            phone = str(output["電話番号"]).strip()
            if "-" not in phone and len(phone) == 11:
                output["電話番号"] = f"{phone[:3]}-{phone[3:7]}-{phone[7:]}"
                output["架電番号"] = output["電話番号"]
        
        return output
    
    def apply_custom_rules_batch(self, output_df: pd.DataFrame, input_df: pd.DataFrame) -> pd.DataFrame:
        """
        バッチ処理用のカスタムルール適用
        
        Args:
            output_df: マッピング後のDataFrame
            input_df: 元の入力DataFrame
            
        Returns:
            カスタムルール適用後のDataFrame
        """
        # 督促状況の一括設定
        output_df["督促状況"] = "TEL"
        
        # 電話番号の一括フォーマット
        if "電話番号" in output_df.columns:
            def format_phone(phone):
                if pd.isna(phone) or phone == "":
                    return ""
                phone_str = str(phone).strip()
                if "-" not in phone_str and len(phone_str) == 11:
                    return f"{phone_str[:3]}-{phone_str[3:7]}-{phone_str[7:]}"
                return phone_str
            
            output_df["電話番号"] = output_df["電話番号"].apply(format_phone)
            output_df["架電番号"] = output_df["電話番号"]  # 同じ値をコピー
        
        return output_df