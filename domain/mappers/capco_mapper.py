"""
カプコシステム用Mapperクラス

カプコ新規登録・残債更新処理で使用される各種データマッピングを実装します。
111列テンプレート対応と電話番号クリーニング機能を管理します。
"""

from typing import Dict, Any, Union
import pandas as pd
from .base import BaseMapper
from .mapping_rules.capco_rules import (
    REGISTRATION_MAPPING_RULES,
    REGISTRATION_DEFAULT_VALUES,
    DEBT_UPDATE_MAPPING_RULES,
    DEBT_UPDATE_DEFAULT_VALUES,
    extract_clean_phone_number,
    clean_capco_phone_field,
    set_capco_client_id,
    process_capco_amount,
    calculate_debt_difference
)


class CapcoRegistrationMapper(BaseMapper):
    """
    カプコ新規登録用データマッパー
    
    カプコデータを111列テンプレート形式に変換します。
    L列「契約者：電話番号」の混入文字自動クリーニング機能を含みます。
    """
    
    def get_mapping_rules(self) -> Dict[str, Union[str, callable]]:
        """新規登録用マッピングルールを取得"""
        rules = REGISTRATION_MAPPING_RULES.copy()
        
        # カスタム関数を追加
        rules["委託先法人ID"] = set_capco_client_id
        
        return rules
    
    def get_default_values(self) -> Dict[str, Any]:
        """新規登録用デフォルト値を取得"""
        return REGISTRATION_DEFAULT_VALUES.copy()
    
    def apply_custom_rules(self, output: Dict[str, Any], input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        カプコ新規登録固有のカスタムルールを適用
        """
        # 委託先法人IDの固定設定
        output["委託先法人ID"] = "7"
        
        # 登録フラグ（111列目）
        output["登録フラグ"] = "1"
        
        # L列「契約者：電話番号」の電話番号クリーニング
        if "契約者：電話番号" in input_data:
            cleaned_phone = extract_clean_phone_number(input_data["契約者：電話番号"])
            output["契約者TEL携帯"] = cleaned_phone
        
        # その他の電話番号フィールドの処理
        phone_fields = ["契約者TEL自宅"]
        for field in phone_fields:
            if field in output and output[field]:
                phone = str(output[field]).strip()
                # 基本的なクリーニング（ハイフン・スペース除去）
                phone = phone.replace("-", "").replace(" ", "").replace("(", "").replace(")", "")
                output[field] = phone
        
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
    
    def apply_custom_rules_batch(self, output_df: pd.DataFrame, input_df: pd.DataFrame) -> pd.DataFrame:
        """
        バッチ処理用のカスタムルール適用（電話番号クリーニング含む）
        """
        # 委託先法人IDの一括設定
        output_df["委託先法人ID"] = "7"
        
        # 登録フラグの一括設定
        output_df["登録フラグ"] = "1"
        
        # L列「契約者：電話番号」の一括クリーニング
        if "契約者：電話番号" in input_df.columns:
            def clean_phone_batch(phone_input):
                if pd.isna(phone_input) or phone_input == "":
                    return ""
                return extract_clean_phone_number(str(phone_input))
            
            output_df["契約者TEL携帯"] = input_df["契約者：電話番号"].apply(clean_phone_batch)
        
        # 金額フィールドの一括処理
        amount_fields = [
            "月額賃料", "管理費", "共益費", "水道代", "駐車場代", 
            "その他費用1", "その他費用2", "敷金", "礼金"
        ]
        
        for field in amount_fields:
            if field in output_df.columns:
                def format_amount(val):
                    if pd.isna(val) or val == "":
                        return ""
                    try:
                        amount = float(val)
                        return str(int(amount))
                    except (ValueError, TypeError):
                        return str(val)
                
                output_df[field] = output_df[field].apply(format_amount)
        
        return output_df


class CapcoDebtUpdateMapper(BaseMapper):
    """
    カプコ残債更新用データマッパー
    
    カプコデータから残債更新用データを作成します。
    """
    
    def get_mapping_rules(self) -> Dict[str, Union[str, callable]]:
        """残債更新用マッピングルールを取得"""
        return DEBT_UPDATE_MAPPING_RULES.copy()
    
    def get_default_values(self) -> Dict[str, Any]:
        """残債更新用デフォルト値を取得"""
        return DEBT_UPDATE_DEFAULT_VALUES.copy()
    
    def apply_custom_rules(self, output: Dict[str, Any], input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        カプコ残債更新固有のカスタムルールを適用
        """
        # 現在日時を更新日として設定
        from datetime import datetime
        output["更新日"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # 差分金額の計算
        if "現在残債" in input_data and "更新後残債" in input_data:
            try:
                current = float(input_data["現在残債"])
                updated = float(input_data["更新後残債"])
                output["差分金額"] = str(updated - current)
            except (ValueError, TypeError):
                output["差分金額"] = "0"
        
        # 処理ステータスの設定
        output["処理ステータス"] = "完了"
        
        return output
    
    def apply_custom_rules_batch(self, output_df: pd.DataFrame, input_df: pd.DataFrame) -> pd.DataFrame:
        """
        残債更新バッチ処理用のカスタムルール適用
        """
        from datetime import datetime
        
        # 更新日の一括設定
        output_df["更新日"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # 処理ステータスの一括設定
        output_df["処理ステータス"] = "完了"
        
        # 差分金額の一括計算
        if "現在残債" in input_df.columns and "更新後残債" in input_df.columns:
            def calculate_diff(row):
                try:
                    current = float(row["現在残債"])
                    updated = float(row["更新後残債"])
                    return str(updated - current)
                except (ValueError, TypeError):
                    return "0"
            
            output_df["差分金額"] = input_df.apply(calculate_diff, axis=1)
        
        return output_df


class CapcoPhoneCleaningMapper(BaseMapper):
    """
    カプコ専用電話番号クリーニングMapper
    
    L列「契約者：電話番号」の混入文字除去に特化したMapperです。
    単独での電話番号クリーニング処理にも使用できます。
    """
    
    def get_mapping_rules(self) -> Dict[str, Union[str, callable]]:
        """電話番号クリーニング用マッピングルール"""
        return {
            "クリーニング済み電話番号": clean_capco_phone_field
        }
    
    def get_default_values(self) -> Dict[str, Any]:
        """デフォルト値（なし）"""
        return {}
    
    def apply_custom_rules(self, output: Dict[str, Any], input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        電話番号クリーニング処理
        """
        # L列「契約者：電話番号」をクリーニング
        if "契約者：電話番号" in input_data:
            cleaned = extract_clean_phone_number(input_data["契約者：電話番号"])
            output["クリーニング済み電話番号"] = cleaned
            
            # 元の値も保持
            output["元の電話番号"] = str(input_data["契約者：電話番号"])
        
        return output
    
    def clean_phone_batch(self, df: pd.DataFrame, phone_column: str = "契約者：電話番号") -> pd.DataFrame:
        """
        DataFrame内の電話番号を一括クリーニング
        
        Args:
            df: 入力DataFrame
            phone_column: 電話番号列名
            
        Returns:
            クリーニング済みDataFrame
        """
        if phone_column not in df.columns:
            return df
        
        def clean_single_phone(phone_input):
            if pd.isna(phone_input) or phone_input == "":
                return ""
            return extract_clean_phone_number(str(phone_input))
        
        result_df = df.copy()
        result_df["クリーニング済み電話番号"] = df[phone_column].apply(clean_single_phone)
        
        return result_df