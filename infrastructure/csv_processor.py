"""
CSV処理の技術的な詳細を扱うInfrastructure層コンポーネント

責務：
- CSVファイルの読み込み（エンコーディング自動判定）
- DataFrame形式への変換
- CSV出力用のファイル名生成
"""

import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional
from infrastructure.encoding_handler import EncodingHandler


class CsvProcessor:
    """CSV処理の技術的詳細を管理"""
    
    def __init__(self):
        self.encoding_handler = EncodingHandler()
    
    def read_csv(self, file_content: bytes, dtype: Optional[Dict] = None) -> pd.DataFrame:
        """
        CSVファイルを読み込み（エンコーディング自動判定）
        
        Args:
            file_content: CSVファイルのバイトデータ
            dtype: カラムのデータ型指定（デフォルト: str）
            
        Returns:
            読み込まれたDataFrame
        """
        # 既存のinfrastructure機能を活用
        from infrastructure import read_csv_auto_encoding
        return read_csv_auto_encoding(file_content, dtype=dtype or str)
    
    def generate_filename(
        self,
        prefix: str,
        suffix: Optional[str] = None,
        timestamp: bool = True,
        extension: str = "csv"
    ) -> str:
        """
        出力ファイル名を生成
        
        Args:
            prefix: ファイル名の接頭辞
            suffix: ファイル名の接尾辞（オプション）
            timestamp: タイムスタンプを含めるか
            extension: ファイル拡張子
            
        Returns:
            生成されたファイル名
            
        Examples:
            generate_filename("mirail", "契約者") -> "mirail_契約者_20240109.csv"
            generate_filename("report", timestamp=False) -> "report.csv"
        """
        parts = [prefix]
        
        if suffix:
            parts.append(suffix)
        
        if timestamp:
            parts.append(datetime.now().strftime("%Y%m%d"))
        
        filename = "_".join(parts)
        return f"{filename}.{extension}"
    
    def prepare_csv_output(self, df: pd.DataFrame, encoding: str = "cp932") -> bytes:
        """
        DataFrameをCSV出力用のバイトデータに変換
        
        Args:
            df: 出力するDataFrame
            encoding: 出力エンコーディング
            
        Returns:
            CSVのバイトデータ
        """
        # エンコーディング処理を委譲
        csv_string = df.to_csv(index=False)
        return self.encoding_handler.encode_output(csv_string, encoding)
    
    @staticmethod
    def validate_columns(df: pd.DataFrame, required_columns: List[str]) -> List[str]:
        """
        必須カラムの存在を検証
        
        Args:
            df: 検証対象のDataFrame
            required_columns: 必須カラムのリスト
            
        Returns:
            不足しているカラムのリスト（空の場合はすべて存在）
        """
        missing_columns = [col for col in required_columns if col not in df.columns]
        return missing_columns
    
    @staticmethod
    def get_column_by_index(df: pd.DataFrame, index: int) -> pd.Series:
        """
        列番号でカラムを取得（安全なアクセス）
        
        Args:
            df: DataFrame
            index: 列番号（0ベース）
            
        Returns:
            指定された列のSeries
            
        Raises:
            IndexError: 列番号が範囲外の場合
        """
        if index < 0 or index >= len(df.columns):
            raise IndexError(f"列番号 {index} は範囲外です（0-{len(df.columns)-1}）")
        return df.iloc[:, index]