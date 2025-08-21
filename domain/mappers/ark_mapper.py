"""
アークシステム用Mapperクラス

アーク新規登録・残債更新処理で使用される各種データマッピングを実装します。
111列テンプレート対応と地域別設定を管理します。
"""

from typing import Dict, Any, Union
import pandas as pd
from .base import BaseMapper
from .mapping_rules.ark_rules import (
    REGISTRATION_MAPPING_RULES,
    REGISTRATION_DEFAULT_VALUES,
    LATE_PAYMENT_MAPPING_RULES,
    LATE_PAYMENT_DEFAULT_VALUES,
    REGION_SETTINGS,
    generate_ark_management_company,
    set_ark_client_id,
    generate_region_fee,
    format_ark_phone_number,
    process_ark_amount
)


class ArkRegistrationMapper(BaseMapper):
    """
    アーク新規登録用データマッパー
    
    案件取込用レポートとContractListのデータを111列テンプレート形式に変換します。
    地域コード対応と空列問題の解決を含みます。
    """
    
    def __init__(self, region_code: int = 1):
        """
        アーク新規登録Mapperの初期化
        
        Args:
            region_code: 地域コード (1:東京, 2:大阪, 3:北海道, 4:北関東)
        """
        super().__init__()
        self.region_code = region_code
        self.region_name = REGION_SETTINGS.get(region_code, {}).get("name", "不明")
        
    def get_mapping_rules(self) -> Dict[str, Union[str, callable]]:
        """新規登録用マッピングルールを取得"""
        rules = REGISTRATION_MAPPING_RULES.copy()
        
        # カスタム関数を追加
        rules["管理会社"] = generate_ark_management_company
        rules["委託先法人ID"] = set_ark_client_id
        
        return rules
    
    def get_default_values(self) -> Dict[str, Any]:
        """新規登録用デフォルト値を取得"""
        defaults = REGISTRATION_DEFAULT_VALUES.copy()
        
        # 地域別設定を適用
        defaults["更新契約手数料"] = str(self.region_code)
        
        return defaults
    
    def apply_custom_rules(self, output: Dict[str, Any], input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        アーク新規登録固有のカスタムルールを適用
        """
        # 委託先法人IDの固定設定
        output["委託先法人ID"] = "5"
        
        # 地域別設定
        output["更新契約手数料"] = str(self.region_code)
        
        # 登録フラグ（111列目）
        output["登録フラグ"] = "1"
        
        # 金額フィールドの処理
        amount_fields = [
            "月額賃料", "管理費", "共益費", "水道代", "駐車場代", 
            "その他費用1", "その他費用2", "敷金", "礼金"
        ]
        
        for field in amount_fields:
            if field in output and output[field]:
                try:
                    amount = float(output[field])
                    output[field] = str(int(amount))  # 小数点以下切り捨て
                except (ValueError, TypeError):
                    pass
        
        # 電話番号フィールドの処理
        phone_fields = ["契約者TEL自宅", "契約者TEL携帯"]
        for field in phone_fields:
            if field in output and output[field]:
                phone = str(output[field]).strip()
                # 不要文字を除去
                phone = phone.replace("-", "").replace(" ", "").replace("(", "").replace(")", "")
                output[field] = phone
        
        return output
    
    def handle_empty_columns_for_111_template(self, output_df: pd.DataFrame) -> pd.DataFrame:
        """
        111列テンプレート用の空列処理
        
        Args:
            output_df: 出力DataFrame
            
        Returns:
            空列処理済みDataFrame
        """
        # 108-110番目の空列を追加（pandas空列問題の対策）
        if len(output_df.columns) < 111:
            # 空列を追加する必要がある場合
            current_columns = list(output_df.columns)
            
            # 登録フラグ（111番目）の前に空列を挿入
            if "登録フラグ" in current_columns:
                insert_index = current_columns.index("登録フラグ")
                
                # 3つの空列を挿入
                for i in range(3):
                    current_columns.insert(insert_index + i, f"")
                    output_df.insert(insert_index + i, "", "")
        
        return output_df


class ArkLatePaymentMapper(BaseMapper):
    """
    アーク残債更新用データマッパー
    
    アークデータとContractListから残債更新用データを作成します。
    """
    
    def get_mapping_rules(self) -> Dict[str, Union[str, callable]]:
        """残債更新用マッピングルールを取得"""
        return LATE_PAYMENT_MAPPING_RULES.copy()
    
    def get_default_values(self) -> Dict[str, Any]:
        """残債更新用デフォルト値を取得"""
        return LATE_PAYMENT_DEFAULT_VALUES.copy()
    
    def apply_custom_rules(self, output: Dict[str, Any], input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        アーク残債更新固有のカスタムルールを適用
        """
        # 現在日時を更新日として設定
        from datetime import datetime
        output["更新日"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # 管理前滞納額の処理（残債更新の計算）
        if "新残債" in input_data and "旧残債" in input_data:
            try:
                new_debt = float(input_data["新残債"])
                old_debt = float(input_data["旧残債"])
                # 差分を管理前滞納額に設定
                output["管理前滞納額"] = str(new_debt - old_debt)
            except (ValueError, TypeError):
                output["管理前滞納額"] = "0"
        
        return output
    
    def apply_custom_rules_batch(self, output_df: pd.DataFrame, input_df: pd.DataFrame) -> pd.DataFrame:
        """
        残債更新バッチ処理用のカスタムルール適用
        """
        from datetime import datetime
        
        # 更新日の一括設定
        output_df["更新日"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # 管理前滞納額の一括計算
        if "新残債" in input_df.columns and "旧残債" in input_df.columns:
            def calculate_debt_diff(row):
                try:
                    new_debt = float(row["新残債"])
                    old_debt = float(row["旧残債"])
                    return str(new_debt - old_debt)
                except (ValueError, TypeError):
                    return "0"
            
            output_df["管理前滞納額"] = input_df.apply(calculate_debt_diff, axis=1)
        
        return output_df