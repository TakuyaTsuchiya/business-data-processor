"""
フェイスシステム用Mapperクラス

フェイスオートコール・SMS処理で使用される各種データマッピングを実装します。
契約者、保証人、緊急連絡人、SMS用のMapperクラスを提供します。
"""

from typing import Dict, Any, Union
import pandas as pd
from .base import BaseMapper
from .mapping_rules.faith_rules import (
    CONTRACT_MAPPING_RULES,
    CONTRACT_DEFAULT_VALUES,
    GUARANTOR_MAPPING_RULES,
    GUARANTOR_DEFAULT_VALUES,
    EMERGENCY_MAPPING_RULES,
    EMERGENCY_DEFAULT_VALUES,
    SMS_MAPPING_RULES,
    SMS_DEFAULT_VALUES,
    generate_faith_client_name,
    format_phone_for_sms
)


class FaithContractMapper(BaseMapper):
    """
    フェイス契約者用データマッパー
    
    ContractListのデータを28列のオートコールテンプレート形式に変換します。
    フェイスは残債列を空白で統一します。
    """
    
    def get_mapping_rules(self) -> Dict[str, Union[str, callable]]:
        """契約者用マッピングルールを取得"""
        rules = CONTRACT_MAPPING_RULES.copy()
        # クライアント名生成関数を追加
        rules["クライアント"] = generate_faith_client_name
        return rules
    
    def get_default_values(self) -> Dict[str, Any]:
        """契約者用デフォルト値を取得"""
        return CONTRACT_DEFAULT_VALUES.copy()
    
    def apply_custom_rules(self, output: Dict[str, Any], input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        フェイス契約者固有のカスタムルールを適用
        """
        # 督促状況の固定値設定
        output["督促状況"] = "TEL"
        
        # 残債は必ず空白（フェイスの仕様）
        output["残債"] = ""
        
        # 電話番号が同じ値を架電番号にもセット
        if "電話番号" in output and output["電話番号"]:
            output["架電番号"] = output["電話番号"]
        
        return output


class FaithGuarantorMapper(BaseMapper):
    """
    フェイス保証人用データマッパー
    
    保証人データを28列のオートコールテンプレート形式に変換します。
    TEL携帯.1を使用します。
    """
    
    def get_mapping_rules(self) -> Dict[str, Union[str, callable]]:
        """保証人用マッピングルールを取得"""
        rules = GUARANTOR_MAPPING_RULES.copy()
        rules["クライアント"] = generate_faith_client_name
        return rules
    
    def get_default_values(self) -> Dict[str, Any]:
        """保証人用デフォルト値を取得"""
        return GUARANTOR_DEFAULT_VALUES.copy()
    
    def apply_custom_rules(self, output: Dict[str, Any], input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        フェイス保証人固有のカスタムルールを適用
        """
        output["督促状況"] = "TEL"
        output["残債"] = ""
        
        if "電話番号" in output and output["電話番号"]:
            output["架電番号"] = output["電話番号"]
        
        return output


class FaithEmergencyContactMapper(BaseMapper):
    """
    フェイス緊急連絡人用データマッパー
    
    緊急連絡人データを28列のオートコールテンプレート形式に変換します。
    TEL携帯.2を使用します。
    """
    
    def get_mapping_rules(self) -> Dict[str, Union[str, callable]]:
        """緊急連絡人用マッピングルールを取得"""
        rules = EMERGENCY_MAPPING_RULES.copy()
        rules["クライアント"] = generate_faith_client_name
        return rules
    
    def get_default_values(self) -> Dict[str, Any]:
        """緊急連絡人用デフォルト値を取得"""
        return EMERGENCY_DEFAULT_VALUES.copy()
    
    def apply_custom_rules(self, output: Dict[str, Any], input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        フェイス緊急連絡人固有のカスタムルールを適用
        """
        output["督促状況"] = "TEL"
        output["残債"] = ""
        
        if "電話番号" in output and output["電話番号"]:
            output["架電番号"] = output["電話番号"]
        
        return output


class FaithSMSMapper(BaseMapper):
    """
    フェイスSMS用データマッパー
    
    退去済み契約者のデータをSMSテンプレート形式に変換します。
    59列の出力形式に対応します。
    """
    
    def __init__(self):
        """SMS Mapperの初期化"""
        super().__init__()
        self.sms_template_columns = []  # 外部テンプレートから読み込む
    
    def get_mapping_rules(self) -> Dict[str, Union[str, callable]]:
        """SMS用マッピングルールを取得"""
        rules = SMS_MAPPING_RULES.copy()
        # 電話番号フォーマット関数を追加
        rules["賃借人1電話番号"] = format_phone_for_sms
        return rules
    
    def get_default_values(self) -> Dict[str, Any]:
        """SMS用デフォルト値を取得"""
        return SMS_DEFAULT_VALUES.copy()
    
    def apply_custom_rules(self, output: Dict[str, Any], input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        フェイスSMS固有のカスタムルールを適用
        """
        # 固定値の設定
        output["種別"] = "テキスト"
        output["トリガー"] = "即時"
        
        # メッセージ関連の固定値（支払期限は動的に設定される想定）
        if "支払期限" not in output:
            output["支払期限"] = ""  # プロセッサー層で設定
        
        # SMS送信フラグ
        output["SMS送信実行確定フラグ"] = "○"
        
        return output
    
    def apply_custom_rules_batch(self, output_df: pd.DataFrame, input_df: pd.DataFrame) -> pd.DataFrame:
        """
        バッチ処理用のカスタムルール適用（SMS用）
        """
        # 固定値の一括設定
        output_df["種別"] = "テキスト"
        output_df["トリガー"] = "即時"
        output_df["SMS送信実行確定フラグ"] = "○"
        
        # 空白列の設定（SMS特有の多数の空白列）
        empty_columns = [
            "予約日", "予約時刻", "有効期限", "有効時刻",
            "リトライ設定", "リトライ時間帯開始", "リトライ時間帯終了",
            "短縮URL", "個別クリック計測", "結果通知先URL"
        ]
        
        for col in empty_columns:
            if col in output_df.columns:
                output_df[col] = ""
        
        return output_df