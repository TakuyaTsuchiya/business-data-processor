"""
Domain層マッピング処理の基底クラス

このモジュールは、全てのMapperクラスの基底となる抽象クラスを定義します。
各システム固有のMapperはこのクラスを継承して実装します。
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Union, Callable
import pandas as pd
from infra.logging.logger import create_logger


class BaseMapper(ABC):
    """
    マッピング処理の基底クラス
    
    全てのシステム固有Mapperはこのクラスを継承し、
    必要な抽象メソッドを実装する必要があります。
    """
    
    def __init__(self):
        """Mapperを初期化"""
        self.logger = create_logger(self.__class__.__name__)
    
    @abstractmethod
    def get_mapping_rules(self) -> Dict[str, Union[str, Callable]]:
        """
        マッピングルールを取得
        
        Returns:
            Dict[str, Union[str, Callable]]: 出力列名 -> 入力列名またはカスタム関数
        """
        pass
    
    @abstractmethod
    def get_default_values(self) -> Dict[str, Any]:
        """
        デフォルト値を取得
        
        Returns:
            Dict[str, Any]: 列名 -> デフォルト値
        """
        pass
    
    def map_single(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        単一レコードのマッピング
        
        Args:
            input_data (Dict[str, Any]): 入力データ
            
        Returns:
            Dict[str, Any]: マッピング後のデータ
        """
        rules = self.get_mapping_rules()
        defaults = self.get_default_values()
        output = {}
        
        for output_col, input_source in rules.items():
            try:
                if callable(input_source):
                    # カスタム変換関数の場合
                    output[output_col] = input_source(input_data)
                elif isinstance(input_source, str) and input_source in input_data:
                    # 単純な列名マッピング
                    output[output_col] = input_data[input_source]
                elif output_col in defaults:
                    # デフォルト値を使用
                    output[output_col] = defaults[output_col]
                else:
                    # 値が見つからない場合は空文字列
                    output[output_col] = ""
                    
            except Exception as e:
                self.logger.error(f"マッピングエラー - 列: {output_col}, エラー: {str(e)}")
                output[output_col] = defaults.get(output_col, "")
                
        # カスタムルールの適用（サブクラスでオーバーライド可能）
        output = self.apply_custom_rules(output, input_data)
        
        return output
    
    def map_batch(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        バッチ処理（DataFrame）のマッピング
        
        Args:
            df (pd.DataFrame): 入力DataFrame
            
        Returns:
            pd.DataFrame: マッピング後のDataFrame
        """
        self.logger.info(f"バッチマッピング開始: {len(df)}件")
        
        # 効率的なバッチ処理
        rules = self.get_mapping_rules()
        defaults = self.get_default_values()
        output_data = {}
        
        # 各出力列に対して処理
        for output_col, input_source in rules.items():
            try:
                if callable(input_source):
                    # カスタム関数を各行に適用
                    output_data[output_col] = df.apply(
                        lambda row: input_source(row.to_dict()), axis=1
                    )
                elif isinstance(input_source, str) and input_source in df.columns:
                    # 単純な列コピー
                    output_data[output_col] = df[input_source]
                else:
                    # デフォルト値または空文字列
                    default_value = defaults.get(output_col, "")
                    output_data[output_col] = [default_value] * len(df)
                    
            except Exception as e:
                self.logger.error(f"バッチマッピングエラー - 列: {output_col}, エラー: {str(e)}")
                default_value = defaults.get(output_col, "")
                output_data[output_col] = [default_value] * len(df)
        
        # DataFrameを構築
        result_df = pd.DataFrame(output_data)
        
        # カスタムルールの適用
        result_df = self.apply_custom_rules_batch(result_df, df)
        
        self.logger.info(f"バッチマッピング完了: {len(result_df)}件")
        
        return result_df
    
    def apply_custom_rules(self, output: Dict[str, Any], input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        カスタムルールの適用（単一レコード）
        
        サブクラスでオーバーライドして、システム固有のビジネスルールを実装
        
        Args:
            output (Dict[str, Any]): マッピング後のデータ
            input_data (Dict[str, Any]): 元の入力データ
            
        Returns:
            Dict[str, Any]: カスタムルール適用後のデータ
        """
        return output
    
    def apply_custom_rules_batch(self, output_df: pd.DataFrame, input_df: pd.DataFrame) -> pd.DataFrame:
        """
        カスタムルールの適用（バッチ処理）
        
        サブクラスでオーバーライドして、システム固有のビジネスルールを実装
        
        Args:
            output_df (pd.DataFrame): マッピング後のDataFrame
            input_df (pd.DataFrame): 元の入力DataFrame
            
        Returns:
            pd.DataFrame: カスタムルール適用後のDataFrame
        """
        return output_df
    
    def validate_input(self, input_data: Union[Dict[str, Any], pd.DataFrame]) -> bool:
        """
        入力データのバリデーション
        
        Args:
            input_data: 検証対象のデータ
            
        Returns:
            bool: 検証成功の場合True
        """
        # サブクラスで実装
        return True
    
    def validate_output(self, output_data: Union[Dict[str, Any], pd.DataFrame]) -> bool:
        """
        出力データのバリデーション
        
        Args:
            output_data: 検証対象のデータ
            
        Returns:
            bool: 検証成功の場合True
        """
        # サブクラスで実装
        return True